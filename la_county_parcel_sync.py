"""
la_county_parcel_sync.py
=========================

Incremental, resumable synchronization of LA County Assessor Parcel Data
(Annual Assessment Roll Data, 2021+) from the public ArcGIS FeatureServer
into local NDJSON files, backed by a SQLite state store.

Dataset landing page:
    https://data.lacounty.gov/datasets/785f54236d1644dc975a55af19b3dd70/about

ArcGIS FeatureServer:
    https://services.arcgis.com/RmCCgQtiZLDCtblq/arcgis/rest/services/Parcel_Data_2021_Table/FeatureServer

Design goals
------------
* Never assume the target layer/table id (auto-discovered from `?f=json`).
* Never hold the whole dataset in memory (page-by-page streaming, NDJSON out).
* Identify parcels with three layers of identity:
    - property_id  -> normalized AIN (the land parcel)
    - snapshot_id  -> one AIN's state in one roll year ("Row ID", or a
                      "{roll_year}:{ain}" fallback)
    - content_hash -> SHA-256 of the canonicalized source attributes, used
                      purely to detect whether a given snapshot changed.
* Resumable, idempotent syncs: unchanged snapshots never re-enter history or
  the changes file; changed snapshots get their previous version archived to
  history; missing snapshots are only ever marked 'inactive' (never deleted),
  and only after a verified, unrestricted, error-free full run.

This module is intentionally a single file for now, but is organized into
independent classes/functions (Config, ArcGISClient, LayerDiscovery,
ParcelNormalizer, StateRepository, NDJSONWriter, SyncEngine,
build_content_hash, main) so each can be lifted into its own module in a
future ETL project with minimal churn.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import re
import sqlite3
import sys
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Sequence, Tuple, Union

import requests

logger = logging.getLogger("la_county_parcel_sync")

# --------------------------------------------------------------------------
# Constants
# --------------------------------------------------------------------------

SCHEMA_VERSION = "1.0"
SOURCE_NAME = "LA County Assessor Parcel Data"
DEFAULT_DATASET_URL = "https://data.lacounty.gov/datasets/785f54236d1644dc975a55af19b3dd70/about"
DEFAULT_FEATURE_SERVER_URL = (
    "https://services.arcgis.com/RmCCgQtiZLDCtblq/arcgis/rest/services/"
    "Parcel_Data_2021_Table/FeatureServer"
)
DEFAULT_WHERE = "1=1"
DEFAULT_USER_AGENT = "la-county-parcel-sync/1.0 (+https://data.lacounty.gov)"

# Logical field -> ordered list of candidate real ArcGIS field names / aliases.
# Matching is case-insensitive against both `name` and `alias`. The first
# candidate that matches a field actually present in the discovered service
# wins, so a future rename of e.g. "SitusCity" -> "SITUS_CITY" only requires
# adding a candidate here, not rewriting normalization logic.
FIELD_CANDIDATES: Dict[str, Tuple[str, ...]] = {
    "ain": ("AIN",),
    "assessor_id": ("AssessorID", "Assessor ID"),
    "property_location": ("PropertyLocation", "Property Location", "SitusFullAddress"),
    "city": ("SitusCity", "City", "TaxRateArea_CITY", "City Tax Rate Area"),
    "zip_code": ("SitusZIP5", "Zip Code", "SitusZIP"),
    "latitude": ("CENTER_LAT", "Location Latitude", "Latitude", "LAT"),
    "longitude": ("CENTER_LON", "Location Longitude", "Longitude", "LON"),
    "property_use_type": ("UseType", "Property Use Type"),
    "property_use_code": ("UseCode", "Property Use Code"),
    "year_built": ("YearBuilt", "Year Built"),
    "effective_year": ("EffectiveYearBuilt", "Effective Year"),
    "square_footage": ("SQFTmain", "Square Footage"),
    "number_of_buildings": ("totBuildingDataLines", "Number of Buildings"),
    "bedrooms": ("Bedrooms", "Number of Bedrooms"),
    "bathrooms": ("Bathrooms", "Number of Bathrooms"),
    "units": ("Units", "Number of Units"),
    "recording_date": ("RecordingDate", "Recording Date"),
    "land_value": ("Roll_LandValue", "Land Value"),
    "improvement_value": ("Roll_ImpValue", "Improvement Value"),
    "total_value": ("Roll_TotalValue", "Total Value"),
    "taxable_value": ("netTaxableValue", "Taxable Value", "NetTaxableValue"),
    "classification": ("ParcelClassification", "Classification"),
    "legal_description": ("ParcelBoundaryDescription", "Parcel Legal Description", "Legal Description"),
    "roll_year": ("RollYear", "Roll Year"),
    "row_id": ("rowID", "Row ID", "RowID"),
}

# ArcGIS/technical fields that describe the *query result*, not the parcel,
# and must never influence content_hash.
TECHNICAL_FIELD_NAMES = {"shape", "shape_length", "shape_area", "globalid", "geometry"}

# Hints used to pick the right layer/table out of a FeatureServer when more
# than one candidate exposes an AIN-like field.
AIN_ALIAS_HINTS = ("ain", "assessor identification", "assessor's identification", "assessor id")
NAME_HINTS = ("parcel", "roll", "assessor")

MAX_PAGES_SAFETY = 5_000_000  # circuit breaker against pathological pagination bugs


# --------------------------------------------------------------------------
# Exceptions
# --------------------------------------------------------------------------


class ArcGISError(Exception):
    """Base class for all ArcGIS client errors."""


class ArcGISHTTPError(ArcGISError):
    """Raised for HTTP-level failures (bad status codes, exhausted retries, transport errors)."""


class ArcGISAPIError(ArcGISError):
    """Raised when the ArcGIS server responds with an `{"error": {...}}` JSON payload, or malformed JSON."""


class ArcGISPaginationError(ArcGISError):
    """Raised when pagination does not make forward progress (stuck ObjectID/offset)."""


class LayerDiscoveryError(Exception):
    """Raised when no layer/table with parcel roll data can be found on the FeatureServer."""


class NormalizationError(Exception):
    """Raised when a single source record cannot be turned into a valid identity (AIN/snapshot)."""


# --------------------------------------------------------------------------
# Small utilities
# --------------------------------------------------------------------------


def _utc_now_iso() -> str:
    """Current UTC timestamp as an ISO-8601 string with a trailing 'Z'."""
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _parse_retry_after(value: Optional[str]) -> Optional[float]:
    """Parse a `Retry-After` header value (seconds or HTTP-date) into seconds."""
    if not value:
        return None
    value = value.strip()
    try:
        return max(0.0, float(value))
    except ValueError:
        pass
    try:
        dt = parsedate_to_datetime(value)
        if dt is None:
            return None
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        delta = (dt - datetime.now(timezone.utc)).total_seconds()
        return max(0.0, delta)
    except (TypeError, ValueError):
        return None


def _clean_string(value: Any) -> Optional[str]:
    """Trim strings, turn empty strings into None. Never invents a value."""
    if value is None:
        return None
    if isinstance(value, str):
        stripped = value.strip()
        return stripped if stripped != "" else None
    return str(value)


def _safe_number(value: Any) -> Optional[Union[int, float]]:
    """Best-effort, non-raising conversion to int (preferred) or float."""
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if value != value:  # NaN
            return None
        return int(value) if value.is_integer() else value
    if isinstance(value, str):
        stripped = value.strip()
        if stripped == "":
            return None
        try:
            parsed = float(stripped)
        except ValueError:
            return None
        if parsed != parsed:
            return None
        return int(parsed) if parsed.is_integer() else parsed
    return None


def _safe_float(value: Any) -> Optional[float]:
    number = _safe_number(value)
    return float(number) if number is not None else None


def _esri_epoch_to_iso(value: Any) -> Optional[str]:
    """Convert an Esri date value (epoch milliseconds, or an ISO-ish string) to ISO-8601 UTC."""
    if value is None or value == "":
        return None
    try:
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            dt = datetime.fromtimestamp(float(value) / 1000.0, tz=timezone.utc)
            return dt.isoformat().replace("+00:00", "Z")
        if isinstance(value, str):
            stripped = value.strip()
            if stripped == "":
                return None
            try:
                millis = float(stripped)
                dt = datetime.fromtimestamp(millis / 1000.0, tz=timezone.utc)
                return dt.isoformat().replace("+00:00", "Z")
            except ValueError:
                pass
            iso_candidate = stripped.replace("Z", "+00:00")
            dt = datetime.fromisoformat(iso_candidate)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.isoformat().replace("+00:00", "Z")
    except (ValueError, OverflowError, OSError):
        logger.debug("Could not parse date-like value: %r", value)
        return None
    return None


def build_content_hash(payload: Dict[str, Any]) -> str:
    """
    SHA-256 hex digest of the canonical JSON representation of `payload`.

    `payload` is expected to already be a flat, type-normalized dict (see
    ParcelNormalizer.canonical_source_attributes): stable null/date/number/
    string representations, with technical/unstable ArcGIS fields removed.
    Keys are sorted and separators are compact so the same logical content
    always yields the same hash regardless of source field ordering.
    """
    canonical = json.dumps(payload, sort_keys=True, ensure_ascii=False, separators=(",", ":"), default=str)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _parse_max_records(value: str) -> Optional[int]:
    if value is None:
        return None
    if value.strip().lower() in ("none", "all", "unlimited", ""):
        return None
    ivalue = int(value)
    if ivalue < 0:
        raise argparse.ArgumentTypeError("--max-records must be a non-negative integer or 'none'")
    return ivalue


# --------------------------------------------------------------------------
# Config
# --------------------------------------------------------------------------


@dataclass
class Config:
    """All runtime configuration, gathered in one place for easy testing/reuse."""

    feature_server_url: str = DEFAULT_FEATURE_SERVER_URL
    dataset_url: str = DEFAULT_DATASET_URL
    max_records: Optional[int] = None
    page_size: int = 1000
    where: str = DEFAULT_WHERE
    output_dir: Path = Path("./output")
    state_db: Path = Path("./state/parcels.sqlite3")
    resume: bool = False
    fresh_run: bool = False
    request_delay: float = 0.1
    log_level: str = "INFO"
    export_current: Optional[Path] = None
    connect_timeout: float = 10.0
    read_timeout: float = 60.0
    max_retries: int = 5
    backoff_base: float = 1.6
    user_agent: str = DEFAULT_USER_AGENT

    @classmethod
    def from_args(cls, args: argparse.Namespace) -> "Config":
        return cls(
            feature_server_url=args.feature_server_url,
            dataset_url=args.dataset_url,
            max_records=args.max_records,
            page_size=args.page_size,
            where=args.where,
            output_dir=Path(args.output_dir),
            state_db=Path(args.state_db),
            resume=args.resume,
            fresh_run=args.fresh_run,
            request_delay=args.request_delay,
            log_level=args.log_level,
            export_current=Path(args.export_current) if args.export_current else None,
            connect_timeout=args.connect_timeout,
            read_timeout=args.read_timeout,
            max_retries=args.max_retries,
        )


# --------------------------------------------------------------------------
# ArcGISClient
# --------------------------------------------------------------------------


class ArcGISClient:
    """
    Thin, retrying HTTP client for ArcGIS REST endpoints.

    Handles: connect/read timeouts, exponential backoff, 429/5xx retries
    (honoring `Retry-After`), malformed-JSON detection, and Esri's
    "200 OK but body is `{"error": {...}}`" error convention.
    """

    def __init__(self, config: Config, session: Optional[requests.Session] = None):
        self._config = config
        self._session = session or requests.Session()
        try:
            self._session.headers.update({"User-Agent": config.user_agent})
        except AttributeError:
            pass  # duck-typed fake sessions in tests may not expose .headers

    def get_service_metadata(self, feature_server_url: str) -> Dict[str, Any]:
        return self._request(feature_server_url, {"f": "json"})

    def get_layer_metadata(self, layer_url: str) -> Dict[str, Any]:
        return self._request(layer_url, {"f": "json"})

    def query(self, layer_url: str, params: Dict[str, Any]) -> Dict[str, Any]:
        return self._request(f"{layer_url.rstrip('/')}/query", params)

    def _request(self, url: str, params: Dict[str, Any]) -> Dict[str, Any]:
        request_params = dict(params)
        request_params.setdefault("f", "json")
        last_exc: Optional[BaseException] = None

        for attempt in range(1, self._config.max_retries + 1):
            try:
                response = self._session.get(
                    url,
                    params=request_params,
                    timeout=(self._config.connect_timeout, self._config.read_timeout),
                )
            except (requests.ConnectionError, requests.Timeout) as exc:
                last_exc = exc
                delay = self._backoff_delay(attempt)
                logger.warning(
                    "Transport error calling %s (%s); retry %d/%d in %.1fs",
                    url, exc, attempt, self._config.max_retries, delay,
                )
                time.sleep(delay)
                continue

            status = response.status_code
            if status == 429 or status >= 500:
                retry_after = _parse_retry_after(response.headers.get("Retry-After"))
                delay = retry_after if retry_after is not None else self._backoff_delay(attempt)
                logger.warning(
                    "HTTP %d from %s; retry %d/%d in %.1fs", status, url, attempt, self._config.max_retries, delay,
                )
                last_exc = ArcGISHTTPError(f"HTTP {status} from {url}")
                time.sleep(delay)
                continue

            if status != 200:
                raise ArcGISHTTPError(f"Unexpected HTTP status {status} from {url}: {response.text[:500]!r}")

            try:
                payload = response.json()
            except ValueError as exc:
                raise ArcGISAPIError(f"Malformed JSON response from {url}: {exc}") from exc

            if isinstance(payload, dict) and "error" in payload:
                err = payload["error"] or {}
                raise ArcGISAPIError(
                    f"ArcGIS API error {err.get('code')} from {url}: {err.get('message')} {err.get('details') or ''}".strip()
                )

            return payload

        raise ArcGISHTTPError(
            f"Exceeded max retries ({self._config.max_retries}) calling {url}"
        ) from last_exc

    def _backoff_delay(self, attempt: int) -> float:
        return min(60.0, self._config.backoff_base ** attempt)


# --------------------------------------------------------------------------
# LayerDiscovery
# --------------------------------------------------------------------------


@dataclass(frozen=True)
class FieldMeta:
    name: str
    alias: str
    type: str
    length: Optional[int] = None


@dataclass(frozen=True)
class LayerInfo:
    layer_id: Any
    name: str
    kind: str  # "layer" | "table"
    query_url: str
    object_id_field: str
    fields: Tuple[FieldMeta, ...]
    max_record_count: int
    supports_pagination: bool
    supports_order_by: bool


def _looks_like_ain_field(f: FieldMeta) -> bool:
    name = f.name.strip().lower()
    alias = (f.alias or "").strip().lower()
    if name == "ain" or alias == "ain":
        return True
    return any(hint in alias for hint in AIN_ALIAS_HINTS)


class LayerDiscovery:
    """
    Finds the layer or table on a FeatureServer that actually holds parcel
    roll data, without assuming its id. Inspects every `layers` and `tables`
    entry, fetches each one's metadata, and picks the one exposing an
    AIN-like field.
    """

    def __init__(self, client: ArcGISClient):
        self._client = client

    def discover(self, feature_server_url: str) -> LayerInfo:
        service_meta = self._client.get_service_metadata(feature_server_url)

        candidates: List[Tuple[str, Dict[str, Any]]] = []
        for kind, key in (("layer", "layers"), ("table", "tables")):
            for entry in service_meta.get(key) or []:
                candidates.append((kind, entry))

        if not candidates:
            raise LayerDiscoveryError(
                f"FeatureServer at {feature_server_url} exposes no layers or tables at all."
            )

        matches: List[LayerInfo] = []
        inspected_summary: List[str] = []

        for kind, entry in candidates:
            layer_id = entry.get("id")
            layer_url = f"{feature_server_url.rstrip('/')}/{layer_id}"
            try:
                meta = self._client.get_layer_metadata(layer_url)
            except ArcGISError as exc:
                inspected_summary.append(f"{kind} id={layer_id} name={entry.get('name')!r} (metadata error: {exc})")
                continue

            fields = tuple(
                FieldMeta(
                    name=f["name"],
                    alias=f.get("alias") or f["name"],
                    type=f.get("type", ""),
                    length=f.get("length"),
                )
                for f in meta.get("fields", [])
            )
            inspected_summary.append(f"{kind} id={layer_id} name={entry.get('name')!r} fields={len(fields)}")

            if any(_looks_like_ain_field(f) for f in fields):
                adv = meta.get("advancedQueryCapabilities", {}) or {}
                matches.append(
                    LayerInfo(
                        layer_id=layer_id,
                        name=entry.get("name", str(layer_id)),
                        kind=kind,
                        query_url=layer_url,
                        object_id_field=meta.get("objectIdField") or "OBJECTID",
                        fields=fields,
                        max_record_count=int(meta.get("maxRecordCount") or 1000),
                        supports_pagination=bool(adv.get("supportsPagination", meta.get("supportsPagination", False))),
                        supports_order_by=bool(adv.get("supportsOrderBy", meta.get("supportsOrderBy", False))),
                    )
                )

        if not matches:
            available = "\n".join(f"  - {s}" for s in inspected_summary) or "  (none)"
            raise LayerDiscoveryError(
                "Could not find a layer/table with parcel roll data (expected an AIN-like field).\n"
                f"Layers/tables found on {feature_server_url}:\n{available}"
            )

        if len(matches) > 1:
            name_matches = [m for m in matches if any(hint in m.name.lower() for hint in NAME_HINTS)]
            if len(name_matches) == 1:
                return name_matches[0]
            names = ", ".join(f"{m.kind}:{m.layer_id}:{m.name}" for m in matches)
            logger.warning("Multiple candidate layers/tables expose an AIN field (%s); using the first one.", names)

        return matches[0]


# --------------------------------------------------------------------------
# ParcelNormalizer
# --------------------------------------------------------------------------


@dataclass(frozen=True)
class ParcelIdentity:
    property_id: str
    snapshot_id: str
    roll_year: Optional[int]
    ain: str


class ParcelNormalizer:
    """
    Maps raw ArcGIS attributes to the stable `normalized` schema, resolves
    parcel identity (property_id / snapshot_id), and produces the
    canonicalized payload that feeds `build_content_hash`.

    Field resolution happens once, at construction time, against the
    service's actual field/alias list -- so a slightly renamed source field
    degrades to "logical field is null" (with a one-time warning) instead of
    crashing every record.
    """

    def __init__(self, layer_info: LayerInfo):
        self._layer_info = layer_info
        self._field_types: Dict[str, str] = {f.name: f.type for f in layer_info.fields}
        self._resolved: Dict[str, Optional[str]] = self._resolve_fields(layer_info.fields)

        missing = sorted(k for k, v in self._resolved.items() if v is None)
        if missing:
            logger.warning(
                "ParcelNormalizer could not map these logical fields to any source field "
                "(they will be emitted as null): %s",
                ", ".join(missing),
            )

    @staticmethod
    def _resolve_fields(fields: Tuple[FieldMeta, ...]) -> Dict[str, Optional[str]]:
        lookup: Dict[str, str] = {}
        for f in fields:
            lookup.setdefault(f.name.strip().lower(), f.name)
            if f.alias:
                lookup.setdefault(f.alias.strip().lower(), f.name)

        resolved: Dict[str, Optional[str]] = {}
        for logical, candidates in FIELD_CANDIDATES.items():
            actual = None
            for candidate in candidates:
                actual = lookup.get(candidate.strip().lower())
                if actual:
                    break
            resolved[logical] = actual
        return resolved

    def resolved_field(self, logical_name: str) -> Optional[str]:
        return self._resolved.get(logical_name)

    def _raw(self, attributes: Dict[str, Any], logical_name: str) -> Any:
        field_name = self._resolved.get(logical_name)
        if field_name is None:
            return None
        return attributes.get(field_name)

    def is_technical_field(self, field_name: str) -> bool:
        if field_name == self._layer_info.object_id_field:
            return True
        return field_name.strip().lower() in TECHNICAL_FIELD_NAMES

    def is_date_field(self, field_name: str) -> bool:
        return self._field_types.get(field_name) == "esriFieldTypeDate"

    def canonical_source_attributes(self, attributes: Dict[str, Any]) -> Dict[str, Any]:
        """The stable, hash-worthy view of a record: technical fields dropped, types normalized."""
        result: Dict[str, Any] = {}
        for field_name, value in attributes.items():
            if self.is_technical_field(field_name):
                continue
            result[field_name] = self._canonical_value(field_name, value)
        return result

    def _canonical_value(self, field_name: str, value: Any) -> Any:
        if value is None:
            return None
        if self.is_date_field(field_name):
            return _esri_epoch_to_iso(value)
        if isinstance(value, str):
            stripped = value.strip()
            return stripped if stripped != "" else None
        if isinstance(value, float):
            return round(value, 9)
        return value

    def normalize(self, attributes: Dict[str, Any]) -> Dict[str, Any]:
        """Build the `normalized` block of the output schema. Never invents values."""
        latitude = _safe_float(self._raw(attributes, "latitude"))
        if latitude is not None and not (-90.0 <= latitude <= 90.0):
            latitude = None
        longitude = _safe_float(self._raw(attributes, "longitude"))
        if longitude is not None and not (-180.0 <= longitude <= 180.0):
            longitude = None

        recording_date_field = self._resolved.get("recording_date")
        recording_date_raw = self._raw(attributes, "recording_date")
        if recording_date_field and self.is_date_field(recording_date_field):
            recording_date = _esri_epoch_to_iso(recording_date_raw)
        else:
            recording_date = _clean_string(recording_date_raw)

        return {
            "ain": _clean_string(self._raw(attributes, "ain")),
            "assessor_id": _clean_string(self._raw(attributes, "assessor_id")),
            "property_location": _clean_string(self._raw(attributes, "property_location")),
            "city": _clean_string(self._raw(attributes, "city")),
            "zip_code": _clean_string(self._raw(attributes, "zip_code")),
            "latitude": latitude,
            "longitude": longitude,
            "property_use_type": _clean_string(self._raw(attributes, "property_use_type")),
            "property_use_code": _clean_string(self._raw(attributes, "property_use_code")),
            "year_built": _safe_number(self._raw(attributes, "year_built")),
            "effective_year": _safe_number(self._raw(attributes, "effective_year")),
            "square_footage": _safe_number(self._raw(attributes, "square_footage")),
            "number_of_buildings": _safe_number(self._raw(attributes, "number_of_buildings")),
            "bedrooms": _safe_number(self._raw(attributes, "bedrooms")),
            "bathrooms": _safe_number(self._raw(attributes, "bathrooms")),
            "units": _safe_number(self._raw(attributes, "units")),
            "recording_date": recording_date,
            "land_value": _safe_number(self._raw(attributes, "land_value")),
            "improvement_value": _safe_number(self._raw(attributes, "improvement_value")),
            "total_value": _safe_number(self._raw(attributes, "total_value")),
            "taxable_value": _safe_number(self._raw(attributes, "taxable_value")),
            "classification": _clean_string(self._raw(attributes, "classification")),
            "legal_description": _clean_string(self._raw(attributes, "legal_description")),
        }

    def extract_identity(self, attributes: Dict[str, Any]) -> ParcelIdentity:
        """
        property_id  = normalized AIN.
        snapshot_id  = Row ID if present, else "{roll_year}:{ain}".
        Raises NormalizationError if neither AIN nor a usable snapshot key exists.
        """
        ain = _clean_string(self._raw(attributes, "ain"))
        if not ain:
            raise NormalizationError("Record is missing AIN; cannot build property_id.")

        roll_year_raw = _safe_number(self._raw(attributes, "roll_year"))
        roll_year = int(roll_year_raw) if roll_year_raw is not None else None

        row_id = _clean_string(self._raw(attributes, "row_id"))
        if row_id:
            snapshot_id = row_id
            if roll_year is None and len(row_id) >= 4 and row_id[:4].isdigit():
                roll_year = int(row_id[:4])
        elif roll_year is not None:
            snapshot_id = f"{roll_year}:{ain}"
        else:
            raise NormalizationError(
                f"Record for AIN={ain} has neither a Row ID nor a Roll Year; cannot build snapshot_id."
            )

        return ParcelIdentity(property_id=ain, snapshot_id=snapshot_id, roll_year=roll_year, ain=ain)


# --------------------------------------------------------------------------
# StateRepository (SQLite)
# --------------------------------------------------------------------------

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS parcel_snapshots (
    snapshot_id TEXT PRIMARY KEY,
    property_id TEXT NOT NULL,
    roll_year INTEGER,
    source_object_id TEXT,
    content_hash TEXT NOT NULL,
    current_json TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'active',
    first_seen_at TEXT NOT NULL,
    last_seen_at TEXT NOT NULL,
    last_seen_run_id TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_parcel_snapshots_property_id ON parcel_snapshots(property_id);
CREATE INDEX IF NOT EXISTS idx_parcel_snapshots_roll_year ON parcel_snapshots(roll_year);
CREATE INDEX IF NOT EXISTS idx_parcel_snapshots_content_hash ON parcel_snapshots(content_hash);
CREATE INDEX IF NOT EXISTS idx_parcel_snapshots_last_seen_run_id ON parcel_snapshots(last_seen_run_id);
CREATE INDEX IF NOT EXISTS idx_parcel_snapshots_status ON parcel_snapshots(status);

CREATE TABLE IF NOT EXISTS parcel_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    snapshot_id TEXT NOT NULL,
    property_id TEXT NOT NULL,
    roll_year INTEGER,
    content_hash TEXT NOT NULL,
    json_data TEXT NOT NULL,
    valid_from TEXT,
    valid_to TEXT,
    run_id TEXT NOT NULL,
    change_type TEXT NOT NULL CHECK (change_type IN ('inserted', 'updated', 'inactivated'))
);
CREATE INDEX IF NOT EXISTS idx_parcel_history_snapshot_id ON parcel_history(snapshot_id);
CREATE INDEX IF NOT EXISTS idx_parcel_history_property_id ON parcel_history(property_id);
CREATE INDEX IF NOT EXISTS idx_parcel_history_run_id ON parcel_history(run_id);

CREATE TABLE IF NOT EXISTS sync_runs (
    run_id TEXT PRIMARY KEY,
    started_at TEXT NOT NULL,
    finished_at TEXT,
    status TEXT NOT NULL CHECK (status IN ('running', 'completed', 'failed')),
    query_where TEXT NOT NULL,
    max_records INTEGER,
    page_size INTEGER NOT NULL,
    last_object_id INTEGER,
    result_offset INTEGER,
    fetched_count INTEGER NOT NULL DEFAULT 0,
    inserted_count INTEGER NOT NULL DEFAULT 0,
    updated_count INTEGER NOT NULL DEFAULT 0,
    unchanged_count INTEGER NOT NULL DEFAULT 0,
    failed_count INTEGER NOT NULL DEFAULT 0,
    error_message TEXT
);
CREATE INDEX IF NOT EXISTS idx_sync_runs_status ON sync_runs(status);
"""


@dataclass
class PreparedRecord:
    """A record that has passed normalization/identity extraction and is ready to be upserted."""

    property_id: str
    snapshot_id: str
    roll_year: Optional[int]
    source_object_id: Any
    content_hash: str
    record: Dict[str, Any]  # full output-schema dict; mutated in place with event_type/status


class StateRepository:
    """
    SQLite-backed state store. WAL mode, one explicit transaction per page,
    parameterized SQL throughout, indexes on the lookup columns the sync
    engine actually needs.
    """

    def __init__(self, db_path: Path):
        self._db_path = db_path
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(db_path), timeout=30, isolation_level=None)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._conn.executescript(SCHEMA_SQL)

    def __enter__(self) -> "StateRepository":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def close(self) -> None:
        self._conn.close()

    # -- sync_runs -----------------------------------------------------

    def start_run(self, run_id: str, where: str, max_records: Optional[int], page_size: int) -> None:
        now = _utc_now_iso()
        self._conn.execute("BEGIN IMMEDIATE")
        self._conn.execute(
            """INSERT INTO sync_runs
               (run_id, started_at, status, query_where, max_records, page_size,
                last_object_id, result_offset, fetched_count, inserted_count,
                updated_count, unchanged_count, failed_count)
               VALUES (?, ?, 'running', ?, ?, ?, NULL, NULL, 0, 0, 0, 0, 0)""",
            (run_id, now, where, max_records, page_size),
        )
        self._conn.execute("COMMIT")

    def get_resumable_run(self) -> Optional[sqlite3.Row]:
        cur = self._conn.execute(
            "SELECT * FROM sync_runs WHERE status = 'running' ORDER BY started_at DESC LIMIT 1"
        )
        return cur.fetchone()

    def abandon_running_runs(self, reason: str) -> int:
        now = _utc_now_iso()
        self._conn.execute("BEGIN IMMEDIATE")
        cur = self._conn.execute(
            "UPDATE sync_runs SET status='failed', finished_at=?, error_message=? WHERE status='running'",
            (now, reason),
        )
        self._conn.execute("COMMIT")
        return cur.rowcount

    def finish_run(self, run_id: str, status: str, error_message: Optional[str] = None) -> None:
        now = _utc_now_iso()
        self._conn.execute("BEGIN IMMEDIATE")
        self._conn.execute(
            "UPDATE sync_runs SET status = ?, finished_at = ?, error_message = ? WHERE run_id = ?",
            (status, now, error_message, run_id),
        )
        self._conn.execute("COMMIT")

    # -- per-page upsert -------------------------------------------------

    def apply_page(
        self,
        run_id: str,
        prepared_records: Sequence[PreparedRecord],
        last_object_id: Optional[int],
        result_offset: Optional[int],
        fetched_increment: int,
        failed_increment: int,
    ) -> List[str]:
        """
        Apply one page of prepared records, and advance the run's checkpoint,
        atomically in a single transaction. Returns the event_type
        ('inserted' | 'updated' | 'unchanged') for each record, in order.
        """
        now = _utc_now_iso()
        event_types: List[str] = []
        counts = {"inserted": 0, "updated": 0, "unchanged": 0}

        self._conn.execute("BEGIN IMMEDIATE")
        try:
            for rec in prepared_records:
                event_type = self._upsert_snapshot(rec, run_id, now)
                counts[event_type] += 1
                event_types.append(event_type)

            self._conn.execute(
                """UPDATE sync_runs SET
                       last_object_id = ?,
                       result_offset = ?,
                       fetched_count = fetched_count + ?,
                       inserted_count = inserted_count + ?,
                       updated_count = updated_count + ?,
                       unchanged_count = unchanged_count + ?,
                       failed_count = failed_count + ?
                   WHERE run_id = ?""",
                (
                    last_object_id,
                    result_offset,
                    fetched_increment,
                    counts["inserted"],
                    counts["updated"],
                    counts["unchanged"],
                    failed_increment,
                    run_id,
                ),
            )
            self._conn.execute("COMMIT")
        except Exception:
            self._conn.execute("ROLLBACK")
            raise

        return event_types

    def _upsert_snapshot(self, rec: PreparedRecord, run_id: str, now: str) -> str:
        cur = self._conn.execute(
            "SELECT content_hash, current_json, status, first_seen_at FROM parcel_snapshots WHERE snapshot_id = ?",
            (rec.snapshot_id,),
        )
        row = cur.fetchone()
        source_object_id = str(rec.source_object_id) if rec.source_object_id is not None else None

        if row is None:
            rec.record["event_type"] = "inserted"
            rec.record["status"] = "active"
            payload = json.dumps(rec.record, ensure_ascii=False)
            self._conn.execute(
                """INSERT INTO parcel_snapshots
                       (snapshot_id, property_id, roll_year, source_object_id, content_hash,
                        current_json, status, first_seen_at, last_seen_at, last_seen_run_id)
                   VALUES (?, ?, ?, ?, ?, ?, 'active', ?, ?, ?)""",
                (rec.snapshot_id, rec.property_id, rec.roll_year, source_object_id, rec.content_hash,
                 payload, now, now, run_id),
            )
            self._conn.execute(
                """INSERT INTO parcel_history
                       (snapshot_id, property_id, roll_year, content_hash, json_data,
                        valid_from, valid_to, run_id, change_type)
                   VALUES (?, ?, ?, ?, ?, ?, NULL, ?, 'inserted')""",
                (rec.snapshot_id, rec.property_id, rec.roll_year, rec.content_hash, payload, now, run_id),
            )
            return "inserted"

        old_hash, old_json, old_status, first_seen_at = row["content_hash"], row["current_json"], row["status"], row["first_seen_at"]

        if old_hash == rec.content_hash and old_status == "active":
            self._conn.execute(
                "UPDATE parcel_snapshots SET last_seen_at = ?, last_seen_run_id = ? WHERE snapshot_id = ?",
                (now, run_id, rec.snapshot_id),
            )
            return "unchanged"

        hist_cur = self._conn.execute(
            "SELECT valid_to FROM parcel_history WHERE snapshot_id = ? ORDER BY id DESC LIMIT 1",
            (rec.snapshot_id,),
        )
        last_hist = hist_cur.fetchone()
        prev_valid_from = last_hist["valid_to"] if (last_hist and last_hist["valid_to"]) else first_seen_at

        self._conn.execute(
            """INSERT INTO parcel_history
                   (snapshot_id, property_id, roll_year, content_hash, json_data,
                    valid_from, valid_to, run_id, change_type)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'updated')""",
            (rec.snapshot_id, rec.property_id, rec.roll_year, old_hash, old_json, prev_valid_from, now, run_id),
        )

        rec.record["event_type"] = "updated"
        rec.record["status"] = "active"
        payload = json.dumps(rec.record, ensure_ascii=False)
        self._conn.execute(
            """UPDATE parcel_snapshots
               SET content_hash = ?, current_json = ?, status = 'active', last_seen_at = ?, last_seen_run_id = ?
               WHERE snapshot_id = ?""",
            (rec.content_hash, payload, now, run_id, rec.snapshot_id),
        )
        return "updated"

    # -- inactivation ------------------------------------------------------

    def inactivate_missing(self, run_id: str, batch_size: int = 500) -> int:
        """
        Mark 'active' snapshots not touched by `run_id` as 'inactive'.
        Streams through candidates in bounded batches; never deletes rows.
        Caller is responsible for only invoking this after a verified,
        unrestricted, error-free full run.
        """
        now = _utc_now_iso()
        count = 0
        self._conn.execute("BEGIN IMMEDIATE")
        try:
            select_cur = self._conn.execute(
                """SELECT snapshot_id, property_id, roll_year, content_hash, current_json, first_seen_at
                   FROM parcel_snapshots WHERE status = 'active' AND last_seen_run_id != ?""",
                (run_id,),
            )
            batch = select_cur.fetchmany(batch_size)
            while batch:
                for row in batch:
                    hist_cur = self._conn.execute(
                        "SELECT valid_to FROM parcel_history WHERE snapshot_id = ? ORDER BY id DESC LIMIT 1",
                        (row["snapshot_id"],),
                    )
                    last_hist = hist_cur.fetchone()
                    prev_valid_from = last_hist["valid_to"] if (last_hist and last_hist["valid_to"]) else row["first_seen_at"]

                    self._conn.execute(
                        """INSERT INTO parcel_history
                               (snapshot_id, property_id, roll_year, content_hash, json_data,
                                valid_from, valid_to, run_id, change_type)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'inactivated')""",
                        (row["snapshot_id"], row["property_id"], row["roll_year"], row["content_hash"],
                         row["current_json"], prev_valid_from, now, run_id),
                    )
                    self._conn.execute(
                        "UPDATE parcel_snapshots SET status = 'inactive' WHERE snapshot_id = ?",
                        (row["snapshot_id"],),
                    )
                    count += 1
                batch = select_cur.fetchmany(batch_size)
            self._conn.execute("COMMIT")
        except Exception:
            self._conn.execute("ROLLBACK")
            raise
        return count

    # -- export --------------------------------------------------------

    def iter_current(self, batch_size: int = 500) -> Iterator[str]:
        """Stream every stored snapshot's current_json (active and inactive), without loading it all into RAM."""
        cur = self._conn.execute(
            "SELECT current_json FROM parcel_snapshots ORDER BY property_id, roll_year"
        )
        rows = cur.fetchmany(batch_size)
        while rows:
            for row in rows:
                yield row["current_json"]
            rows = cur.fetchmany(batch_size)


# --------------------------------------------------------------------------
# NDJSONWriter
# --------------------------------------------------------------------------


class NDJSONWriter:
    """Streaming newline-delimited JSON writer. One JSON object per line, flushed as it's written."""

    def __init__(self, path: Path, mode: str = "w"):
        self._path = path
        self._mode = mode
        self._file = None

    def __enter__(self) -> "NDJSONWriter":
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._file = open(self._path, self._mode, encoding="utf-8", newline="\n")
        return self

    def write(self, obj: Dict[str, Any]) -> None:
        assert self._file is not None, "NDJSONWriter used outside of its context manager"
        self._file.write(json.dumps(obj, ensure_ascii=False))
        self._file.write("\n")
        self._file.flush()

    def write_raw(self, line: str) -> None:
        """Write an already-serialized JSON line verbatim (used for streaming DB exports)."""
        assert self._file is not None, "NDJSONWriter used outside of its context manager"
        self._file.write(line)
        if not line.endswith("\n"):
            self._file.write("\n")

    def __exit__(self, exc_type, exc, tb) -> None:
        if self._file is not None:
            self._file.close()
            self._file = None


# --------------------------------------------------------------------------
# SyncEngine
# --------------------------------------------------------------------------


@dataclass
class Stats:
    fetched: int = 0
    inserted: int = 0
    updated: int = 0
    unchanged: int = 0
    failed: int = 0


@dataclass
class RunResult:
    run_id: str
    stats: Stats
    changes_path: Path
    errors_path: Path
    inactivated: int
    is_full_run: bool


class SyncEngine:
    """
    Orchestrates discovery -> pagination -> normalize -> hash -> upsert ->
    NDJSON, one page (and one DB transaction) at a time. Never accumulates
    the dataset in memory.
    """

    def __init__(
        self,
        config: Config,
        client: ArcGISClient,
        layer_info: LayerInfo,
        normalizer: ParcelNormalizer,
        state: StateRepository,
    ):
        self._config = config
        self._client = client
        self._layer_info = layer_info
        self._normalizer = normalizer
        self._state = state

    def run(self) -> RunResult:
        run_id, resumed, last_object_id, result_offset, prior = self._resolve_run()

        effective_where = prior["query_where"] if resumed else self._config.where
        effective_max_records = prior["max_records"] if resumed else self._config.max_records
        requested_page_size = prior["page_size"] if resumed else self._config.page_size
        effective_page_size = min(requested_page_size, self._layer_info.max_record_count)
        if effective_page_size < requested_page_size:
            logger.info(
                "Requested page-size %d exceeds server maxRecordCount %d; using %d.",
                requested_page_size, self._layer_info.max_record_count, effective_page_size,
            )

        use_keyset = self._layer_info.supports_order_by and self._layer_info.supports_pagination
        if resumed and prior["result_offset"] is not None:
            use_keyset = False  # a prior invocation of this run already fell back to offset mode

        total_hint = self._try_count(effective_where)
        if total_hint is not None:
            logger.info("Source reports approximately %d matching record(s) for where=%r", total_hint, effective_where)

        self._config.output_dir.mkdir(parents=True, exist_ok=True)
        changes_path = self._config.output_dir / f"changes_{run_id}.ndjson"
        errors_path = self._config.output_dir / f"errors_{run_id}.ndjson"
        changes_mode = "a" if (resumed and changes_path.exists()) else "w"
        errors_mode = "a" if (resumed and errors_path.exists()) else "w"

        stats = Stats(
            fetched=prior["fetched_count"] if resumed else 0,
            inserted=prior["inserted_count"] if resumed else 0,
            updated=prior["updated_count"] if resumed else 0,
            unchanged=prior["unchanged_count"] if resumed else 0,
            failed=prior["failed_count"] if resumed else 0,
        )

        logger.info(
            "run_id=%s resumed=%s layer=%s:%s object_id_field=%s page_size=%d max_records=%s where=%r",
            run_id, resumed, self._layer_info.kind, self._layer_info.layer_id,
            self._layer_info.object_id_field, effective_page_size, effective_max_records, effective_where,
        )

        start_time = time.monotonic()
        page_number = 0

        with NDJSONWriter(changes_path, mode=changes_mode) as changes_writer, \
                NDJSONWriter(errors_path, mode=errors_mode) as errors_writer:
            try:
                while True:
                    if effective_max_records is not None:
                        remaining = effective_max_records - stats.fetched
                        if remaining <= 0:
                            break
                        request_count = min(effective_page_size, remaining)
                    else:
                        request_count = effective_page_size

                    page_number += 1
                    if page_number > MAX_PAGES_SAFETY:
                        raise ArcGISPaginationError(
                            f"Exceeded the safety limit of {MAX_PAGES_SAFETY} pages; aborting."
                        )

                    params = self._build_query_params(
                        effective_where, request_count, use_keyset, last_object_id, result_offset
                    )
                    response = self._client.query(self._layer_info.query_url, params)
                    features = response.get("features") or []

                    if not features:
                        logger.info("Received an empty page; source exhausted.")
                        break

                    object_id_field = self._layer_info.object_id_field
                    page_max_object_id = last_object_id
                    prepared: List[PreparedRecord] = []

                    for feature in features:
                        attributes = feature.get("attributes") or {}
                        source_object_id = attributes.get(object_id_field)
                        if use_keyset and isinstance(source_object_id, (int, float)):
                            page_max_object_id = (
                                source_object_id if page_max_object_id is None
                                else max(page_max_object_id, source_object_id)
                            )
                        try:
                            prepared.append(self._prepare_record(run_id, attributes, source_object_id))
                        except NormalizationError as exc:
                            errors_writer.write(
                                {
                                    "run_id": run_id,
                                    "source_object_id": str(source_object_id) if source_object_id is not None else None,
                                    "error": str(exc),
                                    "raw_attributes": attributes,
                                    "recorded_at": _utc_now_iso(),
                                }
                            )

                    if use_keyset:
                        if page_max_object_id is not None and last_object_id is not None and page_max_object_id <= last_object_id:
                            raise ArcGISPaginationError(
                                f"ObjectID did not advance past {last_object_id} "
                                f"(page reported max {page_max_object_id}); aborting to avoid an infinite loop."
                            )
                        last_object_id = page_max_object_id
                        new_offset = None
                    else:
                        result_offset = (result_offset or 0) + len(features)
                        new_offset = result_offset

                    event_types = self._state.apply_page(
                        run_id=run_id,
                        prepared_records=prepared,
                        last_object_id=last_object_id if use_keyset else None,
                        result_offset=new_offset,
                        fetched_increment=len(features),
                        failed_increment=len(features) - len(prepared),
                    )

                    for rec, event_type in zip(prepared, event_types):
                        if event_type in ("inserted", "updated"):
                            changes_writer.write(rec.record)
                        if event_type == "inserted":
                            stats.inserted += 1
                        elif event_type == "updated":
                            stats.updated += 1
                        else:
                            stats.unchanged += 1

                    stats.fetched += len(features)
                    stats.failed += len(features) - len(prepared)

                    elapsed = max(time.monotonic() - start_time, 1e-6)
                    rate = stats.fetched / elapsed
                    logger.info(
                        "page=%d fetched=%d inserted=%d updated=%d unchanged=%d failed=%d "
                        "rate=%.1f rec/s last_object_id=%s offset=%s",
                        page_number, stats.fetched, stats.inserted, stats.updated,
                        stats.unchanged, stats.failed, rate, last_object_id, result_offset,
                    )

                    if len(features) < request_count and not response.get("exceededTransferLimit", False):
                        logger.info("Received a partial page smaller than requested; source exhausted.")
                        break

                    if self._config.request_delay > 0:
                        time.sleep(self._config.request_delay)

            except Exception as exc:
                self._state.finish_run(run_id, "failed", error_message=str(exc))
                logger.error("Sync run %s failed: %s", run_id, exc)
                raise

        is_full_run = effective_max_records is None and effective_where.strip() == DEFAULT_WHERE
        inactivated = 0
        if is_full_run:
            inactivated = self._state.inactivate_missing(run_id)
            logger.info("Marked %d snapshot(s) inactive (no longer present in a full, unrestricted sync).", inactivated)
        else:
            logger.info(
                "Skipping inactivation: not an unrestricted full run (max_records=%s, where=%r).",
                effective_max_records, effective_where,
            )

        self._state.finish_run(run_id, "completed")

        return RunResult(
            run_id=run_id,
            stats=stats,
            changes_path=changes_path,
            errors_path=errors_path,
            inactivated=inactivated,
            is_full_run=is_full_run,
        )

    # -- helpers ---------------------------------------------------------

    def _resolve_run(self) -> Tuple[str, bool, Optional[int], Optional[int], Optional[sqlite3.Row]]:
        if self._config.fresh_run:
            abandoned = self._state.abandon_running_runs(reason="superseded by --fresh-run")
            if abandoned:
                logger.info("Marked %d previously-running run(s) as failed (--fresh-run).", abandoned)

        if self._config.resume:
            row = self._state.get_resumable_run()
            if row is None:
                logger.warning("--resume was given but no interrupted run was found; starting a new run instead.")
            else:
                logger.info(
                    "Resuming run %s from last_object_id=%s offset=%s (fetched so far: %d)",
                    row["run_id"], row["last_object_id"], row["result_offset"], row["fetched_count"],
                )
                return row["run_id"], True, row["last_object_id"], row["result_offset"], row

        run_id = f"run_{uuid.uuid4().hex[:12]}"
        self._state.start_run(run_id, self._config.where, self._config.max_records, self._config.page_size)
        return run_id, False, None, None, None

    def _try_count(self, where: str) -> Optional[int]:
        try:
            result = self._client.query(self._layer_info.query_url, {"where": where, "returnCountOnly": "true"})
            return result.get("count")
        except ArcGISError as exc:
            logger.warning("Could not obtain a total record count (continuing without it): %s", exc)
            return None

    def _build_query_params(
        self,
        where: str,
        result_record_count: int,
        use_keyset: bool,
        last_object_id: Optional[int],
        result_offset: Optional[int],
    ) -> Dict[str, Any]:
        object_id_field = self._layer_info.object_id_field
        params: Dict[str, Any] = {
            "outFields": "*",
            "returnGeometry": "false",
            "resultRecordCount": result_record_count,
            "orderByFields": f"{object_id_field} ASC",
        }
        if use_keyset:
            if last_object_id is not None:
                params["where"] = f"({where}) AND ({object_id_field} > {int(last_object_id)})"
            else:
                params["where"] = where
        else:
            params["where"] = where
            params["resultOffset"] = result_offset or 0
        return params

    def _prepare_record(self, run_id: str, attributes: Dict[str, Any], source_object_id: Any) -> PreparedRecord:
        identity = self._normalizer.extract_identity(attributes)
        normalized = self._normalizer.normalize(attributes)
        hash_payload = self._normalizer.canonical_source_attributes(attributes)
        content_hash = build_content_hash(hash_payload)

        record = {
            "schema_version": SCHEMA_VERSION,
            "event_type": None,  # filled in by StateRepository once insert/update is decided
            "run_id": run_id,
            "source": {
                "name": SOURCE_NAME,
                "dataset_url": self._config.dataset_url,
                "feature_service_url": self._layer_info.query_url,
                "layer_id": self._layer_info.layer_id,
                "source_object_id": str(source_object_id) if source_object_id is not None else None,
            },
            "property_id": identity.property_id,
            "snapshot_id": identity.snapshot_id,
            "roll_year": identity.roll_year,
            "content_hash": content_hash,
            "status": "active",
            "ingested_at": _utc_now_iso(),
            "normalized": normalized,
            "raw_attributes": attributes,
        }

        return PreparedRecord(
            property_id=identity.property_id,
            snapshot_id=identity.snapshot_id,
            roll_year=identity.roll_year,
            source_object_id=source_object_id,
            content_hash=content_hash,
            record=record,
        )


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="la_county_parcel_sync.py",
        description="Sync LA County Assessor Parcel Data (ArcGIS FeatureServer) into NDJSON, with SQLite-backed resumable state.",
    )
    parser.add_argument(
        "--max-records", dest="max_records", type=_parse_max_records, default=None,
        help="Maximum records to fetch this run. 'none' (default) means a full sync.",
    )
    parser.add_argument("--page-size", type=int, default=1000, help="Requested page size; clamped to the server's maxRecordCount.")
    parser.add_argument("--where", type=str, default=DEFAULT_WHERE, help="ArcGIS WHERE clause. Anything other than '1=1' disables inactivation.")
    parser.add_argument("--output-dir", type=str, default="./output", help="Directory for NDJSON/summary output files.")
    parser.add_argument("--state-db", type=str, default="./state/parcels.sqlite3", help="Path to the SQLite state database.")
    parser.add_argument("--resume", action="store_true", help="Resume the most recent interrupted ('running') sync run.")
    parser.add_argument("--fresh-run", action="store_true", help="Force a brand-new run, marking any 'running' run as failed first.")
    parser.add_argument("--request-delay", type=float, default=0.1, help="Delay in seconds between page requests.")
    parser.add_argument("--log-level", type=str, default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    parser.add_argument(
        "--export-current", dest="export_current", type=str, default=None,
        help="Stream the current SQLite state to this NDJSON path and exit (no sync is performed).",
    )
    parser.add_argument("--feature-server-url", type=str, default=DEFAULT_FEATURE_SERVER_URL, help="Override the ArcGIS FeatureServer base URL.")
    parser.add_argument("--dataset-url", type=str, default=DEFAULT_DATASET_URL, help="Dataset landing page URL, recorded in output metadata only.")
    parser.add_argument("--connect-timeout", type=float, default=10.0, help="HTTP connect timeout in seconds.")
    parser.add_argument("--read-timeout", type=float, default=60.0, help="HTTP read timeout in seconds.")
    parser.add_argument("--max-retries", type=int, default=5, help="Max attempts per HTTP request before giving up.")
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    config = Config.from_args(args)

    logging.basicConfig(
        level=getattr(logging, config.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
    )

    config.state_db.parent.mkdir(parents=True, exist_ok=True)

    with StateRepository(config.state_db) as state:
        if config.export_current is not None:
            logger.info("Exporting current parcel state to %s", config.export_current)
            count = 0
            with NDJSONWriter(config.export_current, mode="w") as writer:
                for json_line in state.iter_current():
                    writer.write_raw(json_line)
                    count += 1
            logger.info("Exported %d snapshot(s) to %s", count, config.export_current)
            return 0

        client = ArcGISClient(config)
        discovery = LayerDiscovery(client)
        try:
            layer_info = discovery.discover(config.feature_server_url)
        except LayerDiscoveryError as exc:
            logger.error("%s", exc)
            return 2

        logger.info(
            "Discovered %s id=%s name=%r objectIdField=%s maxRecordCount=%d fields=%d supportsPagination=%s supportsOrderBy=%s",
            layer_info.kind, layer_info.layer_id, layer_info.name, layer_info.object_id_field,
            layer_info.max_record_count, len(layer_info.fields), layer_info.supports_pagination, layer_info.supports_order_by,
        )

        normalizer = ParcelNormalizer(layer_info)
        engine = SyncEngine(config, client, layer_info, normalizer, state)

        try:
            result = engine.run()
        except (ArcGISError, sqlite3.Error) as exc:
            logger.error("Sync run failed: %s", exc)
            return 1

    config.output_dir.mkdir(parents=True, exist_ok=True)
    summary = {
        "schema_version": SCHEMA_VERSION,
        "run_id": result.run_id,
        "is_full_run": result.is_full_run,
        "fetched": result.stats.fetched,
        "inserted": result.stats.inserted,
        "updated": result.stats.updated,
        "unchanged": result.stats.unchanged,
        "failed": result.stats.failed,
        "inactivated": result.inactivated,
        "changes_file": str(result.changes_path),
        "errors_file": str(result.errors_path),
        "generated_at": _utc_now_iso(),
    }
    summary_path = config.output_dir / f"sync_summary_{result.run_id}.json"
    summary_path.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")

    logger.info("Sync complete: %s", json.dumps(summary))
    logger.info("Changes file: %s", result.changes_path)
    logger.info("Errors file:  %s", result.errors_path)
    logger.info("Summary file: %s", summary_path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
