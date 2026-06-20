# Copyright (c) 2026 Beijing Volcano Engine Technology Co., Ltd.
# SPDX-License-Identifier: AGPL-3.0

from __future__ import annotations

from types import SimpleNamespace

import pytest

from openviking.server.identity import RequestContext, Role
from openviking.service.session_auto_commit import SessionAutoCommitIndex
from openviking_cli.session.user_id import UserIdentifier


class _FakeAGFS:
    def __init__(self) -> None:
        self._files: dict[str, bytes] = {}

    def read(self, path: str, **_kwargs) -> bytes:
        if path not in self._files:
            raise FileNotFoundError(path)
        return self._files[path]

    def write(self, path: str, content: bytes, **_kwargs) -> None:
        self._files[path] = content

    def ensure_parent_dirs(self, _path: str, **_kwargs) -> None:
        return None


class _FakeVikingFS:
    def __init__(self) -> None:
        self.agfs = _FakeAGFS()


def _fake_session(account_id: str, user_id: str, session_id: str, *, next_check_at: str):
    return SimpleNamespace(
        ctx=RequestContext(
            user=UserIdentifier(account_id=account_id, user_id=user_id),
            role=Role.USER,
        ),
        session_id=session_id,
        meta=SimpleNamespace(
            auto_commit_policy={
                "enabled": True,
                "idle_timeout_seconds": 60,
                "keep_recent_count": 0,
            },
            keep_recent_count=0,
            pending_tokens=1,
            message_count=1,
            last_message_at="2026-06-22T12:00:00+08:00",
        ),
    )


@pytest.mark.asyncio
async def test_runtime_index_handles_ids_containing_colons():
    index = SessionAutoCommitIndex(_FakeVikingFS())
    await index.initialize()

    await index.upsert_session(
        "acct:west",
        "user:red",
        "session:42",
        next_check_at="2026-06-22T12:01:00+08:00",
    )

    listed = await index.list_sessions()

    assert len(listed) == 1
    assert listed[0].account_id == "acct:west"
    assert listed[0].user_id == "user:red"
    assert listed[0].session_id == "session:42"
    assert listed[0].next_check_at == "2026-06-22T12:01:00+08:00"


@pytest.mark.asyncio
async def test_sync_runtime_state_loads_sessions_outside_index_lock():
    index = SessionAutoCommitIndex(_FakeVikingFS())
    await index.initialize()
    await index.upsert_session(
        "acct_a",
        "user_b",
        "session_c",
        next_check_at="2026-06-22T12:01:00+08:00",
    )

    async def slow_load_session(_account_id: str, _user_id: str, _session_id: str):
        assert not index._lock.locked()
        return _fake_session(
            "acct_a",
            "user_b",
            "session_c",
            next_check_at="2026-06-22T12:01:00+08:00",
        )

    synced = await index.sync_runtime_state(slow_load_session)

    assert [item.session_id for item in synced] == ["session_c"]
