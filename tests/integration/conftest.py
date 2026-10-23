"""Integration fixtures: every test gets its own throwaway database.

Maintenance tests DELETE data, so they must never run against a database that
holds real recordings. A fresh database per test also gives each test a clean
schema, created by the same code path production uses (Database.ensure()).
"""

from __future__ import annotations

import uuid
from collections.abc import AsyncIterator

import asyncpg
import pytest

from ticksentinel.common.config import get_settings
from ticksentinel.storage.db import Database


@pytest.fixture
async def test_dsn() -> AsyncIterator[str]:
    base_dsn = get_settings().postgres_dsn
    try:
        admin = await asyncpg.connect(base_dsn, timeout=3)
    except (OSError, asyncpg.PostgresError, TimeoutError) as exc:
        pytest.skip(f"local Postgres not reachable: {exc}")
    name = f"ts_test_{uuid.uuid4().hex[:10]}"
    await admin.execute(f'CREATE DATABASE "{name}"')
    try:
        yield base_dsn.rsplit("/", 1)[0] + "/" + name
    finally:
        await admin.execute(f'DROP DATABASE "{name}" WITH (FORCE)')
        await admin.close()


@pytest.fixture
async def db(test_dsn: str) -> AsyncIterator[Database]:
    database = Database(test_dsn)
    assert await database.ensure() is not None, database.last_error
    yield database
    await database.close()
