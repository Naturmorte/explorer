"""
ui/insights.py
===============

Investor-oriented signals derived from LA County assessor fields that every
synced snapshot already carries in `raw_attributes`, but that the core
normalizer doesn't surface: the Homeowners' Exemption and the Prop 13
assessment base year.

The idea:

- California's Homeowners' Exemption is only granted on an owner's
  *primary residence*. Its absence on a parcel is a direct signal that the
  parcel is not owner-occupied -- i.e. it already behaves like rental or
  investment stock, not a "will they sell" candidate.
- Under Prop 13, a property's assessed value (and thus its tax bill) only
  resets to market value on a sale or major new construction, then grows
  at most ~2%/year until the next reset. `Roll_LandBaseYear` is that reset
  year, so `roll_year - Roll_LandBaseYear` is a solid proxy for how many
  years the current tax basis has stood -- roughly, tenure since purchase.
  The longer that gap, the bigger the "Prop 13 lock-in": selling means
  giving up a tax basis a new purchase can't replicate, which is a
  well-documented reason long-time CA owners convert a home to a rental
  instead of selling it when they move.

None of this is a prediction of a specific owner's intent -- there is no
listing, mortgage, or owner-identity data here, only public assessment
fields. Every label below is phrased as an inferred lean, not a fact, and
callers should keep it that way.
"""

from __future__ import annotations

import statistics
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Tuple

# Raw ArcGIS field names on this specific FeatureServer (see FIELD_CANDIDATES
# in la_county_parcel_sync.py for the discovery pattern applied to the core
# normalized schema). These are analytics-only extras read straight out of
# raw_attributes rather than duplicated into the sync engine's own schema.
HOME_OWNERS_EXEMPTION_FIELDS: Tuple[str, ...] = ("Roll_HomeOwnersExemp",)
LAND_BASE_YEAR_FIELDS: Tuple[str, ...] = ("Roll_LandBaseYear",)
IMP_BASE_YEAR_FIELDS: Tuple[str, ...] = ("Roll_ImpBaseYear",)
USE_CATEGORY_FIELDS: Tuple[str, ...] = ("UseCodeDescChar1",)
USE_SUBCATEGORY_FIELDS: Tuple[str, ...] = ("UseCodeDescChar2",)

# (key, label, inclusive-lower-bound years, exclusive-upper-bound years or None)
TENURE_BUCKETS: Tuple[Tuple[str, str, float, Optional[float]], ...] = (
    ("new", "New (< 2 yrs)", 0, 2),
    ("moderate", "Moderate (2-7 yrs)", 2, 7),
    ("established", "Established (7-15 yrs)", 7, 15),
    ("long_held", "Long-held (15+ yrs)", 15, None),
)


def _first_present(d: Dict[str, Any], names: Sequence[str]) -> Any:
    for name in names:
        if name in d:
            return d[name]
    return None


def _safe_int(v: Any) -> Optional[int]:
    if v is None or v == "":
        return None
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return None


def tenure_bucket(years: Optional[float]) -> str:
    if years is None:
        return "unknown"
    for key, _label, lo, hi in TENURE_BUCKETS:
        if years >= lo and (hi is None or years < hi):
            return key
    return "unknown"


@dataclass
class ParcelSignals:
    snapshot_id: str
    property_id: str
    roll_year: Optional[int]
    city: Optional[str]
    use_category: Optional[str]
    use_subcategory: Optional[str]
    property_use_type: Optional[str]
    total_value: Optional[float]
    square_footage: Optional[float]
    value_per_sqft: Optional[float]
    home_owners_exemption: Optional[int]
    owner_occupied: Optional[bool]
    tax_basis_year: Optional[int]
    years_since_basis_reset: Optional[float]
    tenure_bucket: str
    behavior_label: str
    behavior_detail: str


def _behavior_indicator(owner_occupied: Optional[bool], bucket: str) -> Tuple[str, str]:
    if owner_occupied is None or bucket == "unknown":
        return (
            "Not enough data",
            "Missing homeowner's exemption or assessment base-year data for this record.",
        )
    if not owner_occupied:
        return (
            "Likely investment / rental",
            "No homeowner's exemption on file, so this isn't the owner's primary residence -- "
            "it already behaves like rental/investment stock rather than a personal-sale candidate.",
        )
    if bucket == "long_held":
        return (
            "Owner-occupied, long-held — rent-over-sell lean",
            "Owner-occupied for 15+ years since the assessment base year. Under Prop 13, selling "
            "resets the tax basis to current market value; owners this deep into a low basis more "
            "often convert the home to a rental than sell it when they move, to keep that basis.",
        )
    if bucket == "established":
        return (
            "Owner-occupied, established tenure",
            "7-15 years since the assessment base year. Some Prop 13 lock-in has built up, but not "
            "enough on its own for a strong lean either way.",
        )
    if bucket == "moderate":
        return (
            "Owner-occupied, moderate tenure",
            "2-7 years since the assessment base year. Limited Prop 13 benefit built up so far.",
        )
    return (
        "Owner-occupied, recent purchase — sell-if-moving lean",
        "Purchased or reassessed within the last 2 years. Little Prop 13 lock-in yet, so a move is "
        "more likely to result in a sale than a rental conversion.",
    )


def extract_signals(record: Dict[str, Any]) -> ParcelSignals:
    """Derive investor-relevant signals from one stored sync record (the same
    shape written to changes_*.ndjson and parcel_snapshots.current_json)."""
    normalized = record.get("normalized") or {}
    raw = record.get("raw_attributes") or {}
    roll_year = record.get("roll_year")

    hoe = _safe_int(_first_present(raw, HOME_OWNERS_EXEMPTION_FIELDS))
    owner_occupied = (hoe > 0) if hoe is not None else None

    land_base = _safe_int(_first_present(raw, LAND_BASE_YEAR_FIELDS))
    imp_base = _safe_int(_first_present(raw, IMP_BASE_YEAR_FIELDS))
    # Land base year is the more reliable "last sale" proxy -- improvement
    # base year also resets on major new construction, not just a sale.
    tax_basis_year = land_base if land_base is not None else imp_base

    years_since: Optional[float] = None
    if tax_basis_year is not None and roll_year is not None:
        candidate = roll_year - tax_basis_year
        if candidate >= 0:
            years_since = candidate  # negative gap is a data anomaly; don't guess

    bucket = tenure_bucket(years_since)

    total_value = normalized.get("total_value")
    sqft = normalized.get("square_footage")
    value_per_sqft = None
    if total_value and sqft and sqft > 0:
        value_per_sqft = round(total_value / sqft, 2)

    behavior_label, behavior_detail = _behavior_indicator(owner_occupied, bucket)

    return ParcelSignals(
        snapshot_id=record.get("snapshot_id"),
        property_id=record.get("property_id"),
        roll_year=roll_year,
        city=normalized.get("city"),
        use_category=_first_present(raw, USE_CATEGORY_FIELDS),
        use_subcategory=_first_present(raw, USE_SUBCATEGORY_FIELDS),
        property_use_type=normalized.get("property_use_type"),
        total_value=total_value,
        square_footage=sqft,
        value_per_sqft=value_per_sqft,
        home_owners_exemption=hoe,
        owner_occupied=owner_occupied,
        tax_basis_year=tax_basis_year,
        years_since_basis_reset=years_since,
        tenure_bucket=bucket,
        behavior_label=behavior_label,
        behavior_detail=behavior_detail,
    )


def summarize(signals: List[ParcelSignals]) -> Dict[str, Any]:
    """Cohort-level aggregates over a (possibly sampled) list of signals."""
    n = len(signals)
    owner_occ = sum(1 for s in signals if s.owner_occupied is True)
    rental = sum(1 for s in signals if s.owner_occupied is False)
    unknown_owner = n - owner_occ - rental

    vpsf = [s.value_per_sqft for s in signals if s.value_per_sqft]
    tenure_years = [s.years_since_basis_reset for s in signals if s.years_since_basis_reset is not None]

    bucket_counts: Dict[str, int] = {key: 0 for key, *_ in TENURE_BUCKETS}
    bucket_counts["unknown"] = 0
    for s in signals:
        bucket_counts[s.tenure_bucket] = bucket_counts.get(s.tenure_bucket, 0) + 1

    distribution = [{"key": key, "label": label, "count": bucket_counts[key]} for key, label, *_ in TENURE_BUCKETS]
    distribution.append({"key": "unknown", "label": "Unknown", "count": bucket_counts["unknown"]})

    return {
        "count": n,
        "owner_occupied": owner_occ,
        "likely_rental": rental,
        "unknown_ownership": unknown_owner,
        "median_value_per_sqft": round(statistics.median(vpsf), 2) if vpsf else None,
        "median_years_held": round(statistics.median(tenure_years), 1) if tenure_years else None,
        "tenure_distribution": distribution,
    }
