# Copyright (c) 2026 Beijing Volcano Engine Technology Co., Ltd.
# SPDX-License-Identifier: AGPL-3.0
"""Runtime helpers for server-side automatic session commits."""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from openviking.pyagfs import AsyncAGFSClient
from openviking.server.identity import RequestContext, Role
from openviking.session import Session
from openviking.utils.time_utils import get_current_timestamp
from openviking_cli.session.user_id import UserIdentifier
from openviking_cli.utils import get_logger

logger = get_logger(__name__)

SESSION_AUTO_COMMIT_INDEX_URI = "/local/_system/session_auto_commit/index.json"


@dataclass(frozen=True)
class IndexedSession:
    account_id: str
    user_id: str
    session_id: str
    next_check_at: str = ""


class SessionAutoCommitIndex:
    """Persistent membership index of active idle auto-commit candidates."""

    def __init__(self, viking_fs: Any):
        self._viking_fs = viking_fs
        self._agfs = AsyncAGFSClient(viking_fs.agfs)
        self._lock = asyncio.Lock()
        self._ctx = RequestContext(user=UserIdentifier.the_default_user(), role=Role.ROOT)
        self._initialized = False
        self._runtime_next_check_at: Dict[Tuple[str, str, str], str] = {}

    async def initialize(self) -> None:
        if self._initialized:
            return
        async with self._lock:
            if self._initialized:
                return
            logger.info("SessionAutoCommitIndex ready at %s", SESSION_AUTO_COMMIT_INDEX_URI)
            self._initialized = True

    async def list_sessions(self) -> List[IndexedSession]:
        await self.initialize()
        async with self._lock:
            data = await self._read_index_file(SESSION_AUTO_COMMIT_INDEX_URI)
            normalized = _normalize_index_data(data)
            results: List[IndexedSession] = []
            for item in _iter_indexed_sessions(normalized):
                results.append(
                    IndexedSession(
                        account_id=item.account_id,
                        user_id=item.user_id,
                        session_id=item.session_id,
                        next_check_at=self._runtime_next_check_at.get(_session_key(item), ""),
                    )
                )
            return results

    async def get_next_due_sessions(self, now: datetime) -> List[IndexedSession]:
        await self.initialize()
        due: List[IndexedSession] = []
        async with self._lock:
            for item in _iter_runtime_sessions(self._runtime_next_check_at):
                is_due = is_next_check_due(self._runtime_next_check_at[_session_key(item)], now)
                if is_due is None:
                    continue
                if is_due:
                    due.append(item)
        return due

    async def upsert_session(
        self,
        account_id: str,
        user_id: str,
        session_id: str,
        *,
        next_check_at: str,
    ) -> None:
        await self.initialize()
        async with self._lock:
            data = await self._read_current_index_locked()
            session_node = (
                data.setdefault("data", {}).setdefault(account_id, {}).setdefault(user_id, {})
            )
            key = _session_key_from_parts(account_id, user_id, session_id)
            self._runtime_next_check_at[key] = next_check_at
            if session_id in session_node:
                return
            session_node[session_id] = {}
            data.setdefault("meta", {})["updated_at"] = get_current_timestamp()
            await self._persist_locked(data)

    async def remove_session(self, account_id: str, user_id: str, session_id: str) -> None:
        await self.initialize()
        async with self._lock:
            self._runtime_next_check_at.pop(
                _session_key_from_parts(account_id, user_id, session_id), None
            )
            data = await self._read_current_index_locked()
            users = data.get("data", {}).get(account_id)
            if not isinstance(users, dict):
                return
            sessions = users.get(user_id)
            if not isinstance(sessions, dict) or session_id not in sessions:
                return
            sessions.pop(session_id, None)
            if not sessions:
                users.pop(user_id, None)
            if not users:
                data.get("data", {}).pop(account_id, None)
            data.setdefault("meta", {})["updated_at"] = get_current_timestamp()
            await self._persist_locked(data)

    async def refresh_runtime_session(self, session: Session) -> None:
        await self.initialize()
        async with self._lock:
            next_check_at = _compute_runtime_next_check_at(session)
            key = _session_key_from_parts(
                session.ctx.account_id, session.ctx.user.user_id, session.session_id
            )
            if next_check_at:
                self._runtime_next_check_at[key] = next_check_at
            else:
                self._runtime_next_check_at.pop(key, None)

    async def sync_runtime_state(self, load_session: Any) -> List[IndexedSession]:
        await self.initialize()
        async with self._lock:
            data = await self._read_current_index_locked()
            indexed_items = list(_iter_indexed_sessions(data))
            current_keys = {_session_key(item) for item in indexed_items}
            stale_keys = [key for key in self._runtime_next_check_at if key not in current_keys]
            for key in stale_keys:
                self._runtime_next_check_at.pop(key, None)
            missing_items = [
                item
                for item in indexed_items
                if _session_key(item) not in self._runtime_next_check_at
            ]

        resolved_next_check_at: Dict[Tuple[str, str, str], str] = {}
        removable_keys: set[Tuple[str, str, str]] = set()
        for item in missing_items:
            try:
                session = await load_session(item.account_id, item.user_id, item.session_id)
            except Exception:
                logger.debug(
                    "SessionAutoCommitIndex failed to sync runtime session %s/%s/%s",
                    item.account_id,
                    item.user_id,
                    item.session_id,
                    exc_info=True,
                )
                continue
            if session is None:
                removable_keys.add(_session_key(item))
                continue
            next_check_at = _compute_runtime_next_check_at(session)
            if not next_check_at:
                removable_keys.add(_session_key(item))
                continue
            resolved_next_check_at[_session_key(item)] = next_check_at

        async with self._lock:
            data = await self._read_current_index_locked()
            indexed_items = list(_iter_indexed_sessions(data))
            changed = False

            current_keys = {_session_key(item) for item in indexed_items}
            stale_keys = [key for key in self._runtime_next_check_at if key not in current_keys]
            for key in stale_keys:
                self._runtime_next_check_at.pop(key, None)

            for key, next_check_at in resolved_next_check_at.items():
                if key in current_keys:
                    self._runtime_next_check_at[key] = next_check_at

            for item in indexed_items:
                key = _session_key(item)
                if key in removable_keys:
                    changed = _remove_membership_from_data(data, item) or changed
                    self._runtime_next_check_at.pop(key, None)

            if changed:
                data.setdefault("meta", {})["updated_at"] = get_current_timestamp()
                await self._persist_locked(data)
                indexed_items = list(_iter_indexed_sessions(data))

            results: List[IndexedSession] = []
            for item in indexed_items:
                results.append(
                    IndexedSession(
                        account_id=item.account_id,
                        user_id=item.user_id,
                        session_id=item.session_id,
                        next_check_at=self._runtime_next_check_at.get(_session_key(item), ""),
                    )
                )
            return results

    async def _read_current_index_locked(self) -> Dict[str, Any]:
        data = await self._read_index_file(SESSION_AUTO_COMMIT_INDEX_URI)
        return _normalize_index_data(data)

    async def _read_index_file(self, path: str) -> Optional[Dict[str, Any]]:
        try:
            raw = await self._agfs.read(path)
        except Exception as exc:
            if _is_index_file_missing(exc, path):
                logger.debug("SessionAutoCommitIndex index file missing: %s", path)
                return None
            logger.warning("SessionAutoCommitIndex failed to read %s: %s", path, exc)
            return None
        content = raw.decode("utf-8") if isinstance(raw, bytes) else str(raw)
        if not content or not content.strip():
            logger.debug("SessionAutoCommitIndex read empty content from %s", path)
            return None
        try:
            return json.loads(content)
        except json.JSONDecodeError as exc:
            logger.warning("Invalid session auto-commit index JSON in %s: %s", path, exc)
            return None

    async def _persist_locked(self, data: Dict[str, Any]) -> None:
        content = json.dumps(data, ensure_ascii=False, indent=2)
        json.loads(content)

        await self._ensure_parent_dirs(SESSION_AUTO_COMMIT_INDEX_URI)
        await self._agfs.write(SESSION_AUTO_COMMIT_INDEX_URI, content.encode("utf-8"))
        logger.debug(
            "SessionAutoCommitIndex persisted sessions=%d updated_at=%s",
            len(_iter_indexed_sessions(data)),
            data.get("meta", {}).get("updated_at", ""),
        )

    async def _ensure_parent_dirs(self, path: str) -> None:
        parent = path.rsplit("/", 1)[0]
        try:
            await self._agfs.ensure_parent_dirs(path)
            return
        except AttributeError:
            if parent:
                await self._agfs.mkdir(parent)
            return
        except Exception as exc:
            logger.warning("Failed to ensure session auto-commit index parent dirs: %s", exc)
            raise


class SessionAutoCommitScheduler:
    """Scheduler for idle-based automatic session commits."""

    DEFAULT_CHECK_INTERVAL = 60.0

    def __init__(
        self,
        session_service: Any,
        config: Any,
        *,
        check_interval: Optional[float] = None,
    ):
        self._session_service = session_service
        self._config = config
        self._check_interval = (
            self.DEFAULT_CHECK_INTERVAL if check_interval is None else float(check_interval)
        )
        self._index: Optional[SessionAutoCommitIndex] = None
        self._running = False
        self._task: Optional[asyncio.Task] = None

    @property
    def index(self) -> Optional[SessionAutoCommitIndex]:
        return self._index

    async def start(self) -> None:
        if self._running:
            return
        self._index = SessionAutoCommitIndex(self._session_service.viking_fs)
        await self._index.initialize()
        self._running = True
        logger.info(
            "SessionAutoCommitScheduler started with check interval %.3fs", self._check_interval
        )
        self._task = asyncio.create_task(self._run_loop())

    async def stop(self) -> None:
        self._running = False
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

    async def _run_loop(self) -> None:
        while self._running:
            try:
                if self._config.idle_enabled and self._index is not None:
                    indexed = await self._index.sync_runtime_state(self._load_session_for_runtime)
                    due = await self._index.get_next_due_sessions(datetime.now())
                    if due:
                        due_details = [
                            f"{item.account_id}/{item.user_id}/{item.session_id}@{item.next_check_at}"
                            for item in due
                        ]
                        logger.info(
                            "SessionAutoCommitScheduler indexed=%d due=%d sessions=%s",
                            len(indexed),
                            len(due),
                            due_details,
                        )
                    for item in due:
                        ctx = RequestContext(
                            user=UserIdentifier(account_id=item.account_id, user_id=item.user_id),
                            role=Role.USER,
                        )
                        await self._session_service.maybe_schedule_auto_commit(
                            item.session_id,
                            ctx,
                            reason_hint="idle_timeout",
                        )
            except Exception as exc:
                logger.error("Session auto-commit scheduler loop failed: %s", exc, exc_info=True)
            try:
                await asyncio.sleep(self._check_interval)
            except asyncio.CancelledError:
                break

    async def _load_session_for_runtime(
        self, account_id: str, user_id: str, session_id: str
    ) -> Optional[Session]:
        ctx = RequestContext(
            user=UserIdentifier(account_id=account_id, user_id=user_id),
            role=Role.USER,
        )
        try:
            return await self._session_service.get(session_id, ctx, auto_create=False)
        except Exception:
            logger.debug(
                "SessionAutoCommitScheduler failed to load session for runtime sync: %s/%s/%s",
                account_id,
                user_id,
                session_id,
                exc_info=True,
            )
            return None


def should_enable_auto_commit(policy: Optional[Dict[str, Any]]) -> bool:
    return bool(isinstance(policy, dict) and policy.get("enabled") is True)


def get_idle_timeout_seconds(policy: Optional[Dict[str, Any]]) -> Optional[int]:
    if not isinstance(policy, dict):
        return None
    value = policy.get("idle_timeout_seconds")
    if value is None:
        return None
    try:
        seconds = int(value)
    except (TypeError, ValueError):
        return None
    return seconds if seconds > 0 else None


def get_token_threshold(policy: Optional[Dict[str, Any]]) -> Optional[int]:
    if not isinstance(policy, dict):
        return None
    value = policy.get("token_threshold")
    if value is None:
        return None
    try:
        threshold = int(value)
    except (TypeError, ValueError):
        return None
    return threshold if threshold >= 0 else None


def compute_next_check_at(last_message_at: str, idle_timeout_seconds: int) -> Optional[str]:
    if not last_message_at:
        return None
    try:
        base = datetime.fromisoformat(last_message_at)
    except Exception:
        return None
    return (base + timedelta(seconds=idle_timeout_seconds)).isoformat()


def is_next_check_due(next_check_at: str, now: datetime) -> Optional[bool]:
    try:
        next_dt = datetime.fromisoformat(next_check_at)
    except Exception:
        return None

    compare_now = now
    if next_dt.tzinfo is not None:
        if compare_now.tzinfo is None:
            compare_now = datetime.fromtimestamp(compare_now.timestamp(), tz=next_dt.tzinfo)
        else:
            compare_now = compare_now.astimezone(next_dt.tzinfo)
    elif compare_now.tzinfo is not None:
        compare_now = compare_now.replace(tzinfo=None)

    return next_dt <= compare_now


def _normalize_index_data(data: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not isinstance(data, dict):
        return {"meta": {"updated_at": ""}, "data": {}}
    normalized = json.loads(json.dumps(data, ensure_ascii=False))
    if not isinstance(normalized.get("meta"), dict):
        normalized["meta"] = {"updated_at": ""}
    if not isinstance(normalized.get("data"), dict):
        normalized["data"] = {}
    return normalized


def _session_key(item: IndexedSession) -> Tuple[str, str, str]:
    return _session_key_from_parts(item.account_id, item.user_id, item.session_id)


def _session_key_from_parts(account_id: str, user_id: str, session_id: str) -> Tuple[str, str, str]:
    return (account_id, user_id, session_id)


def _iter_runtime_sessions(
    runtime_next_check_at: Dict[Tuple[str, str, str], str],
) -> List[IndexedSession]:
    results: List[IndexedSession] = []
    for key in runtime_next_check_at:
        account_id, user_id, session_id = key
        results.append(
            IndexedSession(
                account_id=account_id,
                user_id=user_id,
                session_id=session_id,
                next_check_at=runtime_next_check_at[key],
            )
        )
    return results


def _compute_runtime_next_check_at(session: Session) -> Optional[str]:
    policy = session.meta.auto_commit_policy
    if not should_enable_auto_commit(policy):
        return None
    idle_timeout = get_idle_timeout_seconds(policy)
    if idle_timeout is None:
        return None
    keep_recent_count = int(session.meta.keep_recent_count or 0)
    has_uncommitted = bool(
        int(session.meta.pending_tokens or 0) > 0
        or int(session.meta.message_count or 0) > keep_recent_count
    )
    if not has_uncommitted:
        return None
    return compute_next_check_at(session.meta.last_message_at, idle_timeout)


def _remove_membership_from_data(data: Dict[str, Any], item: IndexedSession) -> bool:
    users = data.get("data", {}).get(item.account_id)
    if not isinstance(users, dict):
        return False
    sessions = users.get(item.user_id)
    if not isinstance(sessions, dict) or item.session_id not in sessions:
        return False
    sessions.pop(item.session_id, None)
    if not sessions:
        users.pop(item.user_id, None)
    if not users:
        data.get("data", {}).pop(item.account_id, None)
    return True


def _is_index_file_missing(exc: Exception, path: str) -> bool:
    text = str(exc).strip()
    if text == path:
        return True
    lower = text.lower()
    return path in text and (
        "not found" in lower or "no such file" in lower or "does not exist" in lower
    )


def _iter_indexed_sessions(index_data: Dict[str, Any]) -> List[IndexedSession]:
    results: List[IndexedSession] = []
    data = index_data.get("data", {})
    if not isinstance(data, dict):
        return results
    for account_id, users in data.items():
        if not isinstance(users, dict):
            continue
        for user_id, sessions in users.items():
            if not isinstance(sessions, dict):
                continue
            for session_id, payload in sessions.items():
                if not isinstance(payload, dict):
                    continue
                results.append(
                    IndexedSession(
                        account_id=account_id,
                        user_id=user_id,
                        session_id=session_id,
                    )
                )
    return results
