import pytest

from src.db.database import init_db, get_db
from src.db import models
from src.scanner import engine
from src.scanner.engine import _scan_plex_only, _scan_cancel

from tests.fakes import FakePlex, base_cfg, plex_show, plex_episode


@pytest.fixture
async def db(tmp_path):
    db_path = str(tmp_path / "babel_test.db")
    await init_db(db_path)
    conn = await get_db(db_path)
    yield conn
    await conn.close()


@pytest.mark.asyncio
async def test_plex_only_classifies_dubbed_and_subonly(db):
    shows = [
        plex_show(1001, "Show A", "/media/show-a", [
            plex_episode(2001, 1, 1, "Ep1", "/media/show-a/e1.mkv", 1000,
                         [{"language": "eng", "codec": "aac", "source": "plex"}], "DUBBED"),
            plex_episode(2002, 1, 2, "Ep2", "/media/show-a/e2.mkv", 2000,
                         [{"language": "jpn", "codec": "aac", "source": "plex"}], "SUB_ONLY"),
        ]),
    ]
    plex = FakePlex(library_data=shows)
    cfg = base_cfg()
    scan_id = await models.start_scan_log(db)

    result = await _scan_plex_only(db, cfg, plex, "eng", scan_id)

    assert result["dubbed"] == 1
    assert result["sub_only"] == 1

    series_id = -(1001 + 1000000)
    series = await models.get_series(db, series_id)
    assert series is not None
    assert series["dubbed_count"] == 1
    assert series["sub_only_count"] == 1

    ep1_id = -(2001 + 1000000)
    ep2_id = -(2002 + 1000000)
    ep1 = await models.get_episode(db, ep1_id)
    ep2 = await models.get_episode(db, ep2_id)
    assert ep1["dub_status"] == "DUBBED"
    assert ep2["dub_status"] == "SUB_ONLY"


@pytest.mark.asyncio
async def test_plex_only_cancel_mid_scan_keeps_unprocessed_series(db):
    """Regression test for the P0 bug where cancelling a Plex-only scan
    deleted every series the scan hadn't reached yet."""
    show_a = plex_show(1001, "Show A", "/media/show-a", [
        plex_episode(2001, 1, 1, "Ep1", "/media/show-a/e1.mkv", 1000,
                     [{"language": "eng", "codec": "aac", "source": "plex"}], "DUBBED"),
    ])
    show_b = plex_show(1002, "Show B", "/media/show-b", [
        plex_episode(2002, 1, 1, "Ep1", "/media/show-b/e1.mkv", 1000,
                     [{"language": "jpn", "codec": "aac", "source": "plex"}], "SUB_ONLY"),
    ])

    # Pre-seed Show B as if a previous completed scan had found it, to prove
    # the cancelled scan doesn't wipe it out just because this run never
    # got to re-process it.
    series_b_id = -(1002 + 1000000)
    await models.upsert_series(db, series_b_id, "Show B", "/media/show-b")

    plex = FakePlex(library_data=[show_a, show_b])
    cfg = base_cfg()
    scan_id = await models.start_scan_log(db)

    _scan_cancel.set()
    try:
        result = await _scan_plex_only(db, cfg, plex, "eng", scan_id)
    finally:
        _scan_cancel.clear()

    assert result["status"] == "completed"  # return dict has no "cancelled" status field, but scan_log should say cancelled
    scan_log = await models.get_scan_logs(db, limit=1)
    assert scan_log[0]["status"] == "cancelled"

    # Show B must still exist — it must NOT have been deleted just because
    # this cancelled run never got around to re-visiting it.
    series_b = await models.get_series(db, series_b_id)
    assert series_b is not None, "cancelled scan must not delete series it didn't process"
