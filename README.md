# LA County Assessor Parcel Sync

Synchronizes **LA County Assessor Parcel Data** (Annual Assessment Roll
Data, 2021 and later, cumulative) from the public ArcGIS FeatureServer into
local NDJSON, with SQLite-backed state so repeated runs are safe, resumable,
and never lose history.

- Dataset page: https://data.lacounty.gov/datasets/785f54236d1644dc975a55af19b3dd70/about
- FeatureServer: https://services.arcgis.com/RmCCgQtiZLDCtblq/arcgis/rest/services/Parcel_Data_2021_Table/FeatureServer

Everything lives in one file, `la_county_parcel_sync.py`, split into
independent classes/functions (`Config`, `ArcGISClient`, `LayerDiscovery`,
`ParcelNormalizer`, `StateRepository`, `NDJSONWriter`, `SyncEngine`,
`build_content_hash`, `main`) so each piece can be lifted into its own
module in a real ETL project later without a rewrite.

## How identity works

The single biggest correctness risk with assessor data is treating "same
address" as "same record" -- addresses get reused, parcels split and merge,
and a single parcel has a different row for every roll year it appears in.
This script never dedupes on address. It uses three explicit layers:

1. **`property_id`** -- the normalized **AIN** (Assessor's Identification
   Number), kept as a zero-padded *string* so leading zeros survive. This
   identifies the land parcel itself, independent of any given year's data.

2. **`snapshot_id`** -- one parcel's state **in one roll year**. Preferably
   the source's own `Row ID` (`rowID`, alias "Row ID"), which the county
   already builds as `RollYear + AIN`. If `Row ID` is missing, the script
   falls back to `"{roll_year}:{ain}"`. A single AIN legitimately has one
   `snapshot_id` per roll year it appears in (e.g. 2024 and 2025 are two
   separate, both-valid snapshots) -- these are never treated as duplicates
   of each other.

3. **`content_hash`** -- a SHA-256 over the canonicalized source attributes
   (sorted keys, stable null/date/number/string representation, with
   ArcGIS-technical fields like `OBJECTID` excluded). This is what tells the
   sync engine whether a given snapshot actually changed since last time, as
   opposed to just being re-read from the source again.

AINs are **never** merged with other AINs just because they share an
address. Real parcel splits/merges are an entity-resolution problem for a
separate module later -- out of scope here by design.

## Why NDJSON, and why a full re-read is sometimes unavoidable

NDJSON (one JSON object per line) is used instead of one giant JSON array
because it can be streamed: written one line at a time as pages arrive, and
read back one line at a time, without ever holding the whole file in memory.
A multi-million-row JSON array would require loading and re-serializing the
entire array to append anything.

The ArcGIS service does not expose a reliable `updated_at`/edit-timestamp
field to filter on, so a full sync has to re-read every record from the API
to know whether anything changed -- there's no cheaper way to detect edits
server-side. This is fine because the *output* stays cheap: a re-read record
whose `content_hash` hasn't changed becomes an `unchanged` outcome that only
touches `last_seen_at`/`last_seen_run_id` in SQLite. It is never re-written
to the changes NDJSON file and never creates a new history row.

## State model (SQLite)

- **`parcel_snapshots`** -- one row per `snapshot_id`, holding the current
  `content_hash`/`current_json`, `status` (`active`/`inactive`), and
  `first_seen_at`/`last_seen_at`/`last_seen_run_id`.
- **`parcel_history`** -- an append-only log. Every insert, update, or
  inactivation writes one row here. On an update, the row that gets archived
  is the *old* version (the one being replaced) -- so history always holds
  what a snapshot looked like *before* the change that superseded it.
- **`sync_runs`** -- one row per run: status (`running`/`completed`/
  `failed`), the where-clause/max-records/page-size it was launched with,
  its pagination checkpoint (`last_object_id` or `result_offset`), and
  running counters. `--resume` looks for the most recent `running` row here.

SQLite runs in WAL mode, every page of records is applied in a single
transaction (upserts + checkpoint update together), and all queries are
parameterized.

## Inactivation is deliberately conservative

Records that disappear from the source are marked `status = 'inactive'` --
**never deleted**. This only happens when *all* of the following hold for
the run that just finished:

- it was launched with `--max-records none` (the default: a full sync),
- it used the default, unrestricted `--where 1=1` (any other `--where`
  disables inactivation entirely, since a filtered run can't tell "missing"
  from "outside the filter"),
- it finished with `status = 'completed'` (a run that raised an exception,
  including a stuck/looping-pagination guard tripping, is left `'failed'`
  and never reaches the inactivation step),
- and it actually reached the end of the dataset.

A `--max-records 10000` test run, or a run interrupted mid-way, can never
mark anything inactive, by construction -- the inactivation call sits after
the code path that only executes on a clean, unrestricted, full completion.

## Pagination

Primary strategy is **keyset pagination** on the object id field discovered
from the service's own metadata (`OBJECTID` on this dataset):
`orderByFields=OBJECTID ASC` plus `WHERE ... AND OBJECTID > {last_object_id}`,
with `last_object_id` checkpointed to SQLite after every page. If the
discovered layer doesn't advertise `supportsOrderBy`/`supportsPagination`,
the engine falls back to `resultOffset`-based paging instead, checkpointing
`result_offset` the same way.

Either way, after each page the script verifies the checkpoint actually
advanced. If it doesn't (a server bug, a `where`-clause that resets on every
call, a broken proxy echoing the same page back) it raises immediately
instead of looping forever, and the run is marked `failed`.

Page size is always clamped to the server's own `maxRecordCount` (discovered
from metadata, not assumed).

## Layer/table discovery

The script never assumes the parcel data lives at layer id `0`. On startup
it fetches `FeatureServer?f=json`, enumerates *every* entry under `layers`
**and** `tables`, fetches each one's own metadata, and picks the one whose
fields include an AIN-like field (by name or alias). If none qualify, it
raises an error that lists every layer/table it found (id, name, field
count) so you can see exactly what's on the service.

## Setup

Requires Python 3.11+ (tested on 3.13/3.14) and `requests`; everything else
is the standard library. No API key or `.env` file is needed -- this is a
public, anonymous-access ArcGIS service.

```bash
pip install -r requirements.txt
```

## Running it

Small smoke test (100 records):

```bash
python la_county_parcel_sync.py --max-records 100
```

A larger test batch (10,000 records, explicit page size):

```bash
python la_county_parcel_sync.py --max-records 10000 --page-size 1000
```

Full sync (all ~12M+ rows -- this will take a while and is resumable, see
below):

```bash
python la_county_parcel_sync.py --max-records none
```

Resume the most recent interrupted run (Ctrl-C, crash, lost connection --
whatever stopped it, `sync_runs` still has it as `status='running'`):

```bash
python la_county_parcel_sync.py --resume
```

`--resume` restores the *original* run's `where`/`max-records`/`page-size`
from `sync_runs` and continues from its checkpoint; it ignores those flags
if you also pass them alongside `--resume`. Note that resuming re-appends to
that run's own `changes_<run_id>.ndjson`/`errors_<run_id>.ndjson` files
rather than starting new ones.

Force a brand-new run even if one is still marked `running` (marks the
stale one `failed` first, then starts fresh):

```bash
python la_county_parcel_sync.py --fresh-run
```

Export the full current state (active *and* inactive snapshots) from
SQLite, streamed line-by-line, without touching the source API:

```bash
python la_county_parcel_sync.py --export-current output/current_parcels.ndjson
```

### All options

```
--max-records N|none   Records to fetch this run. 'none' (default) = full sync.
--page-size N           Requested page size; clamped to the server's maxRecordCount. Default 1000.
--where CLAUSE           ArcGIS WHERE clause. Anything but '1=1' disables inactivation. Default '1=1'.
--output-dir PATH        Directory for NDJSON/summary output. Default ./output
--state-db PATH          SQLite state database path. Default ./state/parcels.sqlite3
--resume                 Resume the most recent 'running' sync_runs row.
--fresh-run              Force a new run; marks any 'running' run 'failed' first.
--request-delay SECONDS  Delay between page requests. Default 0.1
--log-level LEVEL        DEBUG|INFO|WARNING|ERROR. Default INFO
--export-current PATH    Stream current state to PATH and exit (no sync performed).
--feature-server-url URL Override the FeatureServer base URL (defaults to the LA County one).
--dataset-url URL        Dataset landing page, recorded in output metadata only.
--connect-timeout SEC    HTTP connect timeout. Default 10
--read-timeout SEC       HTTP read timeout. Default 60
--max-retries N          Max attempts per HTTP request. Default 5
```

## Output

Each run writes, under `--output-dir`:

- **`changes_<run_id>.ndjson`** -- only `inserted`/`updated` events, one
  JSON object per line, streamed as pages are processed. `unchanged`
  records never appear here.
- **`errors_<run_id>.ndjson`** -- records that failed normalization (e.g.
  missing AIN with no usable Row ID/Roll Year), with the ArcGIS
  `OBJECTID`, the error message, and the raw attributes, so the run can
  keep going instead of aborting on one bad row.
- **`sync_summary_<run_id>.json`** -- final counters (`fetched`, `inserted`,
  `updated`, `unchanged`, `failed`, `inactivated`) plus file paths.

A `changes_*.ndjson` record looks like:

```json
{
  "schema_version": "1.0",
  "event_type": "inserted",
  "run_id": "run_035a06fc5cc4",
  "source": {
    "name": "LA County Assessor Parcel Data",
    "dataset_url": "https://data.lacounty.gov/datasets/785f54236d1644dc975a55af19b3dd70/about",
    "feature_service_url": "https://services.arcgis.com/RmCCgQtiZLDCtblq/arcgis/rest/services/Parcel_Data_2021_Table/FeatureServer/0",
    "layer_id": 0,
    "source_object_id": "47837643"
  },
  "property_id": "2004013020",
  "snapshot_id": "20212004013020",
  "roll_year": 2021,
  "content_hash": "5a9fa7ef81e685ac111028a29e722b51fcd6b6720372ec28459d38b0a57cc979",
  "status": "active",
  "ingested_at": "2026-08-26T13:47:54.066242Z",
  "normalized": { "ain": "2004013020", "property_location": "8418 PONCE AVE  LOS ANGELES CA  91304", "...": "..." },
  "raw_attributes": { "...": "every original ArcGIS attribute, unmodified" }
}
```

`normalized` fields are resolved against the service's *actual* field
aliases at discovery time (see `FIELD_CANDIDATES` in the script) -- if LA
County renames a source field slightly, add the new name as another
candidate rather than rewriting the normalizer. A field that can't be
resolved becomes `null`, never a guessed value. `raw_attributes` always
keeps every original attribute the API returned, untouched.

## Web UI

A small local dashboard, `ui/server.py`, sits on top of the same state
database for two things: running/monitoring syncs, and reviewing the
inserted/updated parcels a run produced before accepting them.

```bash
python ui/server.py
# open http://127.0.0.1:8765
```

It's stdlib-only (`http.server` + `sqlite3` + `subprocess`), reuses
`la_county_parcel_sync.py`'s own `StateRepository`/schema for everything
sync-related, and adds one extra table of its own, `parcel_reviews`, purely
for the review workflow -- the core script stays untouched and still works
standalone from the CLI.

By default it points at the same `./state/parcels.sqlite3` and `./output`
the CLI uses, so a sync you already ran from the terminal shows up
immediately; override with `--state-db` / `--output-dir` / `--host` /
`--port` if needed. It binds to `127.0.0.1` only and has no
authentication -- it's a local admin tool, not meant to be exposed on a
network.

What it does:

- **Overview** -- parcel counts (total/active/inactive/pending review/
  accepted/rejected), a form to launch a sync (`max-records`, `page-size`,
  `where`, `resume`, `fresh-run`) with a live-tailing log console, and
  recent run history.
- **Review Queue** -- every snapshot whose last change was an insert/update
  and hasn't been reviewed since (or has changed again since it *was*
  reviewed -- it automatically falls back into the queue). Search, filter
  by event type, bulk or per-row Accept/Reject, and a detail view that
  shows every normalized field plus, for updates, a from&rarr;to diff
  against the previous version pulled from `parcel_history`.
- **Reviewed** -- the accepted/rejected log with reviewer, timestamp, note,
  and a one-click reset back to pending.
- **Runs** -- the full `sync_runs` history with per-run counters.
- **Errors** -- pick a run and browse its `errors_<run_id>.ndjson`.
- **Export current** -- streams the same NDJSON `--export-current` would
  produce, as a browser download.

A snapshot's review state never touches `parcel_snapshots`/`parcel_history`
-- accept/reject is tracked by `snapshot_id` + the `content_hash` at the
time of the decision, so editing an already-accepted parcel in a later sync
correctly reopens it for review instead of silently keeping a stale
approval.

## Tests

```bash
pip install -r requirements.txt
pytest tests/test_sync.py -v
```

Tests never touch the real network -- `ArcGISClient` is exercised against a
duck-typed fake `requests.Session` (for retry/backoff/error-handling
behavior), and `SyncEngine` is exercised against a fake ArcGIS client that
reproduces real keyset-pagination semantics in memory (sort by `OBJECTID`,
filter `OBJECTID > N`, slice to `resultRecordCount`). They cover: no
duplicates on a repeat sync, updated-event + history archiving, one AIN
across two roll years producing two snapshots under one property_id, exact
`--max-records` accounting, a final undersized page, resume-after-
interruption, partial/failed runs never inactivating, retry after 429/503,
malformed-JSON and ArcGIS-error-object handling, a bad record routing to
the errors file without stopping the run, a stuck/non-advancing pagination
guard, and a streamed `--export-current`.
