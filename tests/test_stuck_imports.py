import pytest

from src.db.database import init_db, get_db
from src.db import models
from src.scanner import engine

from tests.fakes import FakeSonarr, stuck_item


@pytest.fixture
async def db_and_env(tmp_path, monkeypatch):
    db_path = str(tmp_path / "stuck_test.db")
    await init_db(db_path)

    monkeypatch.setenv("DB_PATH", db_path)
    monkeypatch.setenv("SONARR_URL", "http://fake-sonarr:8989")
    monkeypatch.setenv("SONARR_API_KEY", "fake-key")
    from src.config import get_settings, invalidate_effective_settings_cache
    get_settings.cache_clear()
    invalidate_effective_settings_cache()

    db = await get_db(db_path)
    yield db
    await db.close()
    get_settings.cache_clear()
    invalidate_effective_settings_cache()


async def seed_tracked_episode(db, ep_id, series_id=1):
    """Make an episode 'Babel-tracked' — resolve_stuck_imports only acts on
    episodes with a pending upgrade_tracking row."""
    await models.upsert_series(db, series_id, "Show A")
    await models.upsert_episode(db, ep_id, series_id, 1, 1, "Ep1", "/media/e1.mkv", 1000)
    await models.create_upgrade_record(db, ep_id, series_id, "Show A", 1, 1, 1000)


def patch_sonarr(monkeypatch, fake):
    monkeypatch.setattr(engine, "SonarrClient", lambda url, key: fake)


@pytest.mark.asyncio
async def test_untracked_episode_is_skipped(db_and_env, monkeypatch):
    # No upgrade_tracking row for episode 999 — must be skipped, not acted on.
    fake = FakeSonarr(stuck_imports=[
        stuck_item(1, 999, 1, "Some.Release", ["already imported"]),
    ])
    patch_sonarr(monkeypatch, fake)

    summary = await engine.resolve_stuck_imports()

    assert summary["checked"] == 1
    assert summary["skipped"] == 1
    assert fake.removed_from_queue == []


@pytest.mark.asyncio
async def test_already_imported_is_removed_without_blocklist(db_and_env, monkeypatch):
    db = db_and_env
    await seed_tracked_episode(db, 101)
    fake = FakeSonarr(stuck_imports=[
        stuck_item(1, 101, 1, "Show.A.S01E01", ["Episode file already imported"]),
    ])
    patch_sonarr(monkeypatch, fake)

    summary = await engine.resolve_stuck_imports()

    assert summary["resolved"] == 1
    assert fake.removed_from_queue == [(1, False)]


@pytest.mark.asyncio
async def test_sample_release_is_blocklisted_and_research(db_and_env, monkeypatch):
    db = db_and_env
    await seed_tracked_episode(db, 102)
    fake = FakeSonarr(stuck_imports=[
        stuck_item(2, 102, 1, "Show.A.S01E01", ["sample"]),
    ])
    patch_sonarr(monkeypatch, fake)

    summary = await engine.resolve_stuck_imports()

    assert summary["resolved"] == 1
    assert fake.removed_from_queue == [(2, True)]
    assert fake.searched_ids == [[102]]


@pytest.mark.asyncio
async def test_id_mismatch_triggers_force_manual_import(db_and_env, monkeypatch):
    db = db_and_env
    await seed_tracked_episode(db, 103)
    fake = FakeSonarr(stuck_imports=[
        stuck_item(3, 103, 1, "Show.A.S01E01", ["Matched to series by ID"]),
    ])
    patch_sonarr(monkeypatch, fake)

    summary = await engine.resolve_stuck_imports()

    assert summary["resolved"] == 1
    assert fake.force_manual_imports == [(3, 1, [103])]


@pytest.mark.asyncio
async def test_unmatched_message_is_logged_not_acted_on(db_and_env, monkeypatch):
    db = db_and_env
    await seed_tracked_episode(db, 104)
    fake = FakeSonarr(stuck_imports=[
        stuck_item(4, 104, 1, "Show.A.S01E01", ["some completely novel Sonarr error we've never seen"]),
    ])
    patch_sonarr(monkeypatch, fake)

    summary = await engine.resolve_stuck_imports()

    assert summary["unmatched"] == 1
    assert summary["resolved"] == 0
    assert summary["retried"] == 0
    assert fake.removed_from_queue == []
    assert fake.force_manual_imports == []


@pytest.mark.asyncio
async def test_dry_run_does_not_call_sonarr_mutating_endpoints(db_and_env, monkeypatch):
    db = db_and_env
    await seed_tracked_episode(db, 105)
    await models.set_setting(db, "STUCK_IMPORT_DRY_RUN", "true")
    from src.config import invalidate_effective_settings_cache
    invalidate_effective_settings_cache()

    fake = FakeSonarr(stuck_imports=[
        stuck_item(5, 105, 1, "Show.A.S01E01", ["already imported"]),
    ])
    patch_sonarr(monkeypatch, fake)

    summary = await engine.resolve_stuck_imports()

    # Still reports what *would* have happened...
    assert summary["resolved"] == 1
    # ...but never actually mutates the Sonarr queue.
    assert fake.removed_from_queue == []
    assert fake.force_manual_imports == []
    assert fake.retried_imports == []


@pytest.mark.asyncio
async def test_disabled_feature_short_circuits(db_and_env, monkeypatch):
    db = db_and_env
    await models.set_setting(db, "AUTO_RESOLVE_IMPORTS", "false")
    from src.config import invalidate_effective_settings_cache
    invalidate_effective_settings_cache()

    fake = FakeSonarr(stuck_imports=[
        stuck_item(6, 106, 1, "Show.A.S01E01", ["already imported"]),
    ])
    patch_sonarr(monkeypatch, fake)

    summary = await engine.resolve_stuck_imports()

    assert summary == {"checked": 0, "resolved": 0, "skipped": 0}
