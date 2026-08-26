"""
Tests for la_county_parcel_sync.py.

These tests never touch the real network: `ArcGISClient` is tested against a
duck-typed fake `requests.Session`, and `SyncEngine` is tested against a
fake `ArcGISClient` that emulates ArcGIS's keyset-pagination query semantics
in-memory (sort by OBJECTID, filter `OBJECTID > N`, slice to
`resultRecordCount`, report `exceededTransferLimit`).
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import la_county_parcel_sync as sync


# --------------------------------------------------------------------------
# Shared fixtures / fakes
# --------------------------------------------------------------------------


def make_field(name, alias, ftype="esriFieldTypeString", length=50):
    return sync.FieldMeta(name=name, alias=alias, type=ftype, length=length)


SAMPLE_FIELDS = (
    make_field("OBJECTID", "OBJECTID", "esriFieldTypeOID"),
    make_field("AIN", "AIN"),
    make_field("RollYear", "Roll Year"),
    make_field("rowID", "Row ID", length=14),
    make_field("PropertyLocation", "Property Location", length=120),
    make_field("SitusCity", "City", length=24),
    make_field("SitusZIP5", "Zip Code", length=5),
    make_field("UseType", "Property Use Type"),
    make_field("UseCode", "Property Use Code"),
    make_field("YearBuilt", "Year Built"),
    make_field("EffectiveYearBuilt", "Effective Year"),
    make_field("SQFTmain", "Square Footage", "esriFieldTypeInteger"),
    make_field("totBuildingDataLines", "Number of Buildings", "esriFieldTypeInteger"),
    make_field("Bedrooms", "Number of Bedrooms", "esriFieldTypeInteger"),
    make_field("Bathrooms", "Number of Bathrooms", "esriFieldTypeInteger"),
    make_field("Units", "Number of Units", "esriFieldTypeInteger"),
    make_field("RecordingDate", "Recording Date", "esriFieldTypeDate"),
    make_field("Roll_LandValue", "Land Value", "esriFieldTypeInteger"),
    make_field("Roll_ImpValue", "Improvement Value", "esriFieldTypeInteger"),
    make_field("Roll_TotalValue", "Total Value", "esriFieldTypeInteger"),
    make_field("netTaxableValue", "Taxable Value", "esriFieldTypeInteger"),
    make_field("ParcelClassification", "Classification"),
    make_field("ParcelBoundaryDescription", "Parcel Legal Description", length=1000),
    make_field("CENTER_LAT", "Location Latitude", "esriFieldTypeDouble"),
    make_field("CENTER_LON", "Location Longitude", "esriFieldTypeDouble"),
    make_field("AssessorID", "Assessor ID"),
)


def make_layer_info(url: str = "https://example.test/FeatureServer/0") -> sync.LayerInfo:
    return sync.LayerInfo(
        layer_id=0,
        name="Parcel_Data",
        kind="table",
        query_url=url,
        object_id_field="OBJECTID",
        fields=SAMPLE_FIELDS,
        max_record_count=2000,
        supports_pagination=True,
        supports_order_by=True,
    )


def make_attributes(object_id: int, ain: str = "1234567890", roll_year: str = "2025", **overrides: Any) -> Dict[str, Any]:
    row_id = f"{roll_year}{ain}"
    attrs: Dict[str, Any] = {
        "OBJECTID": object_id,
        "AIN": ain,
        "RollYear": roll_year,
        "rowID": row_id,
        "PropertyLocation": "123 MAIN ST",
        "SitusCity": "LOS ANGELES",
        "SitusZIP5": "90001",
        "UseType": "01",
        "UseCode": "0100",
        "YearBuilt": "1950",
        "EffectiveYearBuilt": "1950",
        "SQFTmain": 1200,
        "totBuildingDataLines": 1,
        "Bedrooms": 3,
        "Bathrooms": 2,
        "Units": 1,
        "RecordingDate": 1700000000000,
        "Roll_LandValue": 100000,
        "Roll_ImpValue": 200000,
        "Roll_TotalValue": 300000,
        "netTaxableValue": 300000,
        "ParcelClassification": "R1",
        "ParcelBoundaryDescription": "LOT 1 TRACT 1",
        "CENTER_LAT": 34.05,
        "CENTER_LON": -118.25,
        "AssessorID": "AS-1",
    }
    attrs.update(overrides)
    return attrs


class FakeArcGISClient:
    """Emulates the subset of ArcGISClient.query() behavior SyncEngine relies on."""

    def __init__(self, records: List[Dict[str, Any]]):
        self.records = records
        self.calls = 0

    def query(self, url: str, params: Dict[str, Any]) -> Dict[str, Any]:
        self.calls += 1
        if params.get("returnCountOnly") == "true":
            return {"count": len(self.records)}

        where = params.get("where", "1=1")
        m = re.search(r"OBJECTID\s*>\s*(-?\d+)", where)
        min_oid = int(m.group(1)) if m else None
        offset = params.get("resultOffset")

        candidates = sorted(self.records, key=lambda r: r["OBJECTID"])
        if min_oid is not None:
            candidates = [r for r in candidates if r["OBJECTID"] > min_oid]
        elif offset is not None:
            candidates = candidates[offset:]

        n = params.get("resultRecordCount", len(candidates))
        page = candidates[:n]
        features = [{"attributes": dict(r)} for r in page]
        return {"features": features, "exceededTransferLimit": len(candidates) > len(page)}


class StuckArcGISClient:
    """Always returns the same first page, regardless of the where-clause -- simulates a broken server."""

    def __init__(self, records: List[Dict[str, Any]]):
        self.records = sorted(records, key=lambda r: r["OBJECTID"])
        self.calls = 0

    def query(self, url: str, params: Dict[str, Any]) -> Dict[str, Any]:
        self.calls += 1
        if params.get("returnCountOnly") == "true":
            return {"count": len(self.records)}
        n = params.get("resultRecordCount", len(self.records))
        page = self.records[:n]
        return {"features": [{"attributes": dict(r)} for r in page], "exceededTransferLimit": False}


def make_config(tmp_path: Path, **overrides: Any) -> sync.Config:
    cfg = sync.Config(
        output_dir=tmp_path / "output",
        state_db=tmp_path / "state" / "parcels.sqlite3",
        request_delay=0.0,
        page_size=overrides.pop("page_size", 1000),
    )
    for key, value in overrides.items():
        setattr(cfg, key, value)
    return cfg


def run_sync(tmp_path: Path, client, records=None, **config_overrides: Any):
    config = make_config(tmp_path, **config_overrides)
    layer_info = make_layer_info()
    normalizer = sync.ParcelNormalizer(layer_info)
    with sync.StateRepository(config.state_db) as state:
        engine = sync.SyncEngine(config, client, layer_info, normalizer, state)
        result = engine.run()
    return result, config


# --------------------------------------------------------------------------
# 1. Identical records twice -> no duplicates
# --------------------------------------------------------------------------


def test_identical_record_synced_twice_produces_no_duplicate(tmp_path):
    records = [make_attributes(1)]

    result1, config = run_sync(tmp_path, FakeArcGISClient(records), records=records)
    assert result1.stats.inserted == 1
    assert result1.stats.updated == 0

    result2, _ = run_sync(tmp_path, FakeArcGISClient(records), records=records)
    assert result2.stats.inserted == 0
    assert result2.stats.updated == 0
    assert result2.stats.unchanged == 1

    with sync.StateRepository(config.state_db) as state:
        cur = state._conn.execute("SELECT COUNT(*) AS n FROM parcel_snapshots")
        assert cur.fetchone()["n"] == 1
        cur = state._conn.execute("SELECT COUNT(*) AS n FROM parcel_history WHERE change_type = 'inserted'")
        assert cur.fetchone()["n"] == 1


# --------------------------------------------------------------------------
# 2. Same snapshot_id, changed data -> 'updated' event + history row
# --------------------------------------------------------------------------


def test_changed_record_produces_updated_event_and_history(tmp_path):
    records = [make_attributes(1, Roll_TotalValue=300000)]
    result1, config = run_sync(tmp_path, FakeArcGISClient(records), records=records)
    assert result1.stats.inserted == 1

    changed = [make_attributes(1, Roll_TotalValue=350000)]
    result2, _ = run_sync(tmp_path, FakeArcGISClient(changed), records=changed)
    assert result2.stats.updated == 1
    assert result2.stats.inserted == 0

    changes_text = result2.changes_path.read_text(encoding="utf-8").strip().splitlines()
    assert len(changes_text) == 1
    event = json.loads(changes_text[0])
    assert event["event_type"] == "updated"
    assert event["normalized"]["total_value"] == 350000

    with sync.StateRepository(config.state_db) as state:
        cur = state._conn.execute(
            "SELECT change_type, json_data FROM parcel_history WHERE change_type = 'updated'"
        )
        row = cur.fetchone()
        archived = json.loads(row["json_data"])
        assert archived["normalized"]["total_value"] == 300000  # the OLD version was archived


# --------------------------------------------------------------------------
# 3. Same AIN across two roll years -> two snapshots, one property_id
# --------------------------------------------------------------------------


def test_same_ain_two_roll_years_two_snapshots_one_property(tmp_path):
    records = [
        make_attributes(1, ain="7001002003", roll_year="2024"),
        make_attributes(2, ain="7001002003", roll_year="2025"),
    ]
    result, config = run_sync(tmp_path, FakeArcGISClient(records), records=records)
    assert result.stats.inserted == 2

    with sync.StateRepository(config.state_db) as state:
        cur = state._conn.execute(
            "SELECT snapshot_id, property_id, roll_year FROM parcel_snapshots ORDER BY roll_year"
        )
        rows = cur.fetchall()
    assert len(rows) == 2
    assert {r["property_id"] for r in rows} == {"7001002003"}
    assert {r["roll_year"] for r in rows} == {2024, 2025}
    assert rows[0]["snapshot_id"] != rows[1]["snapshot_id"]


# --------------------------------------------------------------------------
# 4. max_records=10000 processes exactly 10000
# --------------------------------------------------------------------------


def test_max_records_processes_exact_count(tmp_path):
    records = [make_attributes(i, ain=f"{i:010d}", roll_year="2025") for i in range(1, 10501)]
    client = FakeArcGISClient(records)
    result, _ = run_sync(tmp_path, client, records=records, max_records=10000, page_size=1000)
    assert result.stats.fetched == 10000
    assert result.stats.inserted == 10000
    assert client.calls == 10 + 1  # 10 pages + 1 count query


# --------------------------------------------------------------------------
# 5. Last page smaller than page_size
# --------------------------------------------------------------------------


def test_last_page_smaller_than_page_size(tmp_path):
    records = [make_attributes(i, ain=f"{i:010d}", roll_year="2025") for i in range(1, 2501)]
    client = FakeArcGISClient(records)
    result, _ = run_sync(tmp_path, client, records=records, max_records=None, page_size=1000)
    assert result.stats.fetched == 2500
    assert result.stats.inserted == 2500


# --------------------------------------------------------------------------
# 6. Resume continues after the last committed page
# --------------------------------------------------------------------------


def test_resume_continues_after_last_completed_page(tmp_path):
    records = [make_attributes(i, ain=f"{i:010d}", roll_year="2025") for i in range(1, 2501)]

    config = make_config(tmp_path, max_records=None, page_size=1000)
    layer_info = make_layer_info()
    normalizer = sync.ParcelNormalizer(layer_info)

    # Simulate an interruption: run a normal engine, but only "deliver" the
    # first page's worth of records to the client and stop early by
    # capping max_records at one page's size, leaving the run 'running'
    # if it were partial -- instead, we directly emulate a crash by
    # starting a run and applying exactly one page, then abandoning it.
    with sync.StateRepository(config.state_db) as state:
        client = FakeArcGISClient(records)
        run_id = "run_test_resume"
        state.start_run(run_id, config.where, None, 1000)
        first_page = sorted(records, key=lambda r: r["OBJECTID"])[:1000]
        engine = sync.SyncEngine(config, client, layer_info, normalizer, state)
        prepared = [engine._prepare_record(run_id, r, r["OBJECTID"]) for r in first_page]
        state.apply_page(run_id, prepared, last_object_id=1000, result_offset=None,
                          fetched_increment=len(prepared), failed_increment=0)
        # run_id stays 'running' here, exactly like an interrupted process.

    resumed_config = make_config(tmp_path, max_records=None, page_size=1000, resume=True)
    with sync.StateRepository(resumed_config.state_db) as state:
        client2 = FakeArcGISClient(records)
        engine2 = sync.SyncEngine(resumed_config, client2, layer_info, normalizer, state)
        result = engine2.run()

    # stats are cumulative for the whole run: 1000 inserted before the
    # simulated interruption + 1500 inserted after resuming = 2500 total.
    assert result.stats.fetched == 2500
    assert result.stats.inserted == 2500
    assert client2.calls == 1 + 2  # 1 count query + 2 pages covering OBJECTID 1001..2500
    with sync.StateRepository(resumed_config.state_db) as state:
        cur = state._conn.execute("SELECT COUNT(*) AS n FROM parcel_snapshots")
        assert cur.fetchone()["n"] == 2500  # no duplicates from the pre-applied first page


# --------------------------------------------------------------------------
# 7 & 8. Partial / failed runs never inactivate
# --------------------------------------------------------------------------


def test_partial_run_does_not_inactivate(tmp_path):
    records = [make_attributes(i, ain=f"{i:010d}", roll_year="2025") for i in range(1, 51)]
    run_sync(tmp_path, FakeArcGISClient(records), records=records, max_records=None, page_size=100)

    fewer_records = records[:10]
    result, config = run_sync(
        tmp_path, FakeArcGISClient(fewer_records), records=fewer_records, max_records=5, page_size=100
    )
    assert result.is_full_run is False
    assert result.inactivated == 0

    with sync.StateRepository(config.state_db) as state:
        cur = state._conn.execute("SELECT COUNT(*) AS n FROM parcel_snapshots WHERE status = 'inactive'")
        assert cur.fetchone()["n"] == 0


def test_failed_run_does_not_inactivate(tmp_path):
    records = [make_attributes(i, ain=f"{i:010d}", roll_year="2025") for i in range(1, 51)]
    run_sync(tmp_path, FakeArcGISClient(records), records=records, max_records=None, page_size=100)

    stuck_client = StuckArcGISClient(records[:10])
    config = make_config(tmp_path, max_records=None, page_size=5)
    layer_info = make_layer_info()
    normalizer = sync.ParcelNormalizer(layer_info)
    with sync.StateRepository(config.state_db) as state:
        engine = sync.SyncEngine(config, stuck_client, layer_info, normalizer, state)
        with pytest.raises(sync.ArcGISPaginationError):
            engine.run()

        cur = state._conn.execute("SELECT COUNT(*) AS n FROM parcel_snapshots WHERE status = 'inactive'")
        assert cur.fetchone()["n"] == 0
        cur = state._conn.execute("SELECT status FROM sync_runs ORDER BY started_at DESC LIMIT 1")
        assert cur.fetchone()["status"] == "failed"


# --------------------------------------------------------------------------
# 9. Retry after 429 and 503
# --------------------------------------------------------------------------


class FakeResponse:
    def __init__(self, status_code, json_data=None, headers=None, text=""):
        self.status_code = status_code
        self._json_data = json_data
        self.headers = headers or {}
        self.text = text

    def json(self):
        if self._json_data is None:
            raise ValueError("no JSON body")
        return self._json_data


class FakeSession:
    def __init__(self, responses):
        self._responses = list(responses)
        self.headers = {}
        self.calls = 0

    def get(self, url, params=None, timeout=None):
        self.calls += 1
        return self._responses.pop(0)


def test_retries_after_429_and_503(tmp_path, monkeypatch):
    monkeypatch.setattr(sync.time, "sleep", lambda s: None)
    responses = [
        FakeResponse(429, headers={"Retry-After": "0"}),
        FakeResponse(503),
        FakeResponse(200, json_data={"features": [], "exceededTransferLimit": False}),
    ]
    session = FakeSession(responses)
    config = make_config(tmp_path)
    client = sync.ArcGISClient(config, session=session)

    result = client.query("https://example.test/FeatureServer/0", {"where": "1=1"})
    assert result == {"features": [], "exceededTransferLimit": False}
    assert session.calls == 3


def test_malformed_json_raises_api_error(tmp_path, monkeypatch):
    monkeypatch.setattr(sync.time, "sleep", lambda s: None)
    session = FakeSession([FakeResponse(200, json_data=None, text="<html>not json</html>")])
    config = make_config(tmp_path)
    client = sync.ArcGISClient(config, session=session)
    with pytest.raises(sync.ArcGISAPIError):
        client.query("https://example.test/FeatureServer/0", {"where": "1=1"})


def test_esri_error_object_raises_api_error(tmp_path, monkeypatch):
    monkeypatch.setattr(sync.time, "sleep", lambda s: None)
    session = FakeSession(
        [FakeResponse(200, json_data={"error": {"code": 400, "message": "Invalid where clause"}})]
    )
    config = make_config(tmp_path)
    client = sync.ArcGISClient(config, session=session)
    with pytest.raises(sync.ArcGISAPIError):
        client.query("https://example.test/FeatureServer/0", {"where": "bogus"})


# --------------------------------------------------------------------------
# 10. Invalid record goes to errors file, doesn't stop the run
# --------------------------------------------------------------------------


def test_invalid_record_goes_to_errors_file_without_stopping(tmp_path):
    good = make_attributes(1, ain="1111111111", roll_year="2025")
    bad = make_attributes(2, ain="", roll_year="")  # no AIN, no roll year, no row id
    bad["rowID"] = ""
    records = [good, bad, make_attributes(3, ain="3333333333", roll_year="2025")]

    result, _ = run_sync(tmp_path, FakeArcGISClient(records), records=records, page_size=100)

    assert result.stats.inserted == 2
    assert result.stats.failed == 1

    errors_lines = result.errors_path.read_text(encoding="utf-8").strip().splitlines()
    assert len(errors_lines) == 1
    error_record = json.loads(errors_lines[0])
    assert error_record["source_object_id"] == "2"
    assert "AIN" in error_record["error"] or "ain" in error_record["error"].lower()


# --------------------------------------------------------------------------
# 11. Stuck ObjectID / repeated page does not loop forever
# --------------------------------------------------------------------------


def test_stuck_pagination_raises_instead_of_looping_forever(tmp_path):
    records = [make_attributes(i, ain=f"{i:010d}", roll_year="2025") for i in range(1, 21)]
    client = StuckArcGISClient(records)
    config = make_config(tmp_path, max_records=None, page_size=5)
    layer_info = make_layer_info()
    normalizer = sync.ParcelNormalizer(layer_info)
    with sync.StateRepository(config.state_db) as state:
        engine = sync.SyncEngine(config, client, layer_info, normalizer, state)
        with pytest.raises(sync.ArcGISPaginationError):
            engine.run()
    # It must have bailed out quickly, not spun until MAX_PAGES_SAFETY.
    assert client.calls < 10


# --------------------------------------------------------------------------
# 12. export-current streams without loading everything into RAM at once
# --------------------------------------------------------------------------


def test_export_current_streams_all_rows(tmp_path):
    records = [make_attributes(i, ain=f"{i:010d}", roll_year="2025") for i in range(1, 251)]
    result, config = run_sync(tmp_path, FakeArcGISClient(records), records=records, page_size=50)
    assert result.stats.inserted == 250

    export_path = tmp_path / "output" / "current_parcels.ndjson"
    with sync.StateRepository(config.state_db) as state:
        count = 0
        with sync.NDJSONWriter(export_path, mode="w") as writer:
            # Use a small batch size to prove it's genuinely streaming in chunks.
            for line in state.iter_current(batch_size=17):
                writer.write_raw(line)
                count += 1
    assert count == 250

    lines = export_path.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 250
    parsed = [json.loads(line) for line in lines]
    assert {p["property_id"] for p in parsed} == {f"{i:010d}" for i in range(1, 251)}


# --------------------------------------------------------------------------
# Extra: content hash stability / normalization sanity
# --------------------------------------------------------------------------


def test_content_hash_is_stable_and_ignores_technical_fields():
    layer_info = make_layer_info()
    normalizer = sync.ParcelNormalizer(layer_info)
    attrs1 = make_attributes(1)
    attrs2 = dict(attrs1)
    attrs2["OBJECTID"] = 999  # technical field changes...

    h1 = sync.build_content_hash(normalizer.canonical_source_attributes(attrs1))
    h2 = sync.build_content_hash(normalizer.canonical_source_attributes(attrs2))
    assert h1 == h2  # ...but the hash must not change

    attrs3 = dict(attrs1)
    attrs3["Roll_TotalValue"] = 999999
    h3 = sync.build_content_hash(normalizer.canonical_source_attributes(attrs3))
    assert h3 != h1


def test_normalizer_handles_missing_field_gracefully():
    fields = tuple(f for f in SAMPLE_FIELDS if f.name != "SitusCity")
    layer_info = sync.LayerInfo(
        layer_id=0, name="Parcel_Data", kind="table", query_url="https://example.test/FeatureServer/0",
        object_id_field="OBJECTID", fields=fields, max_record_count=2000,
        supports_pagination=True, supports_order_by=True,
    )
    normalizer = sync.ParcelNormalizer(layer_info)
    normalized = normalizer.normalize(make_attributes(1))
    assert normalized["city"] is None  # missing source field -> null, never invented


def test_layer_discovery_error_lists_available_layers():
    class NoAinClient:
        def get_service_metadata(self, url):
            return {"layers": [], "tables": [{"id": 5, "name": "Other_Table"}]}

        def get_layer_metadata(self, url):
            return {"objectIdField": "OBJECTID", "fields": [{"name": "OBJECTID", "type": "esriFieldTypeOID"}]}

    discovery = sync.LayerDiscovery(NoAinClient())
    with pytest.raises(sync.LayerDiscoveryError) as excinfo:
        discovery.discover("https://example.test/FeatureServer")
    assert "Other_Table" in str(excinfo.value)


def test_layer_discovery_finds_table_not_just_layer_zero():
    class MultiTableClient:
        def get_service_metadata(self, url):
            return {"layers": [], "tables": [{"id": 7, "name": "Parcel_Data"}]}

        def get_layer_metadata(self, url):
            assert url.endswith("/7")
            return {
                "objectIdField": "OBJECTID",
                "maxRecordCount": 2000,
                "advancedQueryCapabilities": {"supportsPagination": True, "supportsOrderBy": True},
                "fields": [
                    {"name": "OBJECTID", "alias": "OBJECTID", "type": "esriFieldTypeOID"},
                    {"name": "AIN", "alias": "AIN", "type": "esriFieldTypeString"},
                ],
            }

    discovery = sync.LayerDiscovery(MultiTableClient())
    layer_info = discovery.discover("https://example.test/FeatureServer")
    assert layer_info.layer_id == 7
    assert layer_info.kind == "table"
