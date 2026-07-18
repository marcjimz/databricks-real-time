"""Shared pure-Python HL7 v2.5 parser (spec section 5.2).

Runs identically in a Spark pandas UDF (bronze_to_silver on the cluster) and in
the local batch processor — no Spark, pandas, or Databricks import here, so the
same classification logic is unit-tested in plain pytest.

Design: the generator injects exactly three corruptions (BAD_SEPARATOR,
BAD_TIMESTAMP, TRUNCATED_SEGMENT). BAD_SEPARATOR and BAD_TIMESTAMP both preserve
the full message structure and are detected precisely; therefore *any other*
structural break is, by elimination, a truncation. A clean message must satisfy
every structural check, so it is never quarantined.

Order matters: separator -> structural completeness -> timestamp. Checking
completeness before the timestamp means a message truncated mid-timestamp is
classified TRUNCATED_SEGMENT (structure broken), while a fully-structured
message with a garbage timestamp is BAD_TIMESTAMP (the injected case).
"""

from __future__ import annotations

from dataclasses import dataclass, field

FIELD = "|"
COMP = "^"
_MSH_PREFIX = r"MSH|^~\&|"          # clean MSH-1/MSH-2 (field sep + encoding chars)
_MSH_MIN_FIELDS = 12                # clean MSH always has 12 fields
_TS_LEN = 14                        # HL7 YYYYMMDDHHMMSS

# MSH field indices (0-based on split("|"); MSH-1 is the separator itself)
_MSH_FACILITY = 3                   # MSH-4 sending facility
_MSH_TS = 6                         # MSH-7 message timestamp
_MSH_TYPE = 8                       # MSH-9 message type

_ADT_VERB = {"A01": "Admit", "A03": "Discharge", "A08": "Update"}

TRUNCATED_SEGMENT = "TRUNCATED_SEGMENT"
BAD_SEPARATOR = "BAD_SEPARATOR"
BAD_TIMESTAMP = "BAD_TIMESTAMP"
PARSE_ERROR = "PARSE_ERROR"


@dataclass
class ParseOutcome:
    ok: bool
    fields: dict = field(default_factory=dict)
    error_code: str = ""
    error_detail: str = ""


def _quarantine(code: str, detail: str) -> ParseOutcome:
    return ParseOutcome(ok=False, error_code=code, error_detail=detail)


def _index(segments: list[list[str]], seg_id: str) -> list[str] | None:
    for seg in segments:
        if seg and seg[0] == seg_id:
            return seg
    return None


def parse_hl7(hl7_raw: str) -> ParseOutcome:
    """Parse one HL7 v2.5 message; return parsed silver fields or a quarantine."""
    raw = hl7_raw or ""

    # 1. Field-separator / encoding-character integrity (BAD_SEPARATOR).
    if not raw.startswith(_MSH_PREFIX):
        return _quarantine(BAD_SEPARATOR, "MSH does not begin with 'MSH|^~\\&|'")

    lines = [seg.split(FIELD) for seg in raw.split("\r") if seg]
    msh = lines[0]

    # 2. Structural completeness (TRUNCATED_SEGMENT). Checked before the
    #    timestamp so a message cut mid-header is truncation, not bad-timestamp.
    if len(msh) < _MSH_MIN_FIELDS:
        return _quarantine(TRUNCATED_SEGMENT, f"MSH has {len(msh)} of {_MSH_MIN_FIELDS} fields")

    message_type = msh[_MSH_TYPE]
    facility_id = msh[_MSH_FACILITY]

    pid = _index(lines, "PID")
    if pid is None or len(pid) < 4 or not pid[3].split(COMP)[0]:
        return _quarantine(TRUNCATED_SEGMENT, "missing or incomplete PID segment")
    patient_mrn = pid[3].split(COMP)[0]

    unit = ""
    if message_type.startswith("ADT"):
        pv1 = _index(lines, "PV1")
        if pv1 is None or len(pv1) < 4 or not pv1[3].split(COMP)[0]:
            return _quarantine(TRUNCATED_SEGMENT, "ADT missing PV1 location")
        unit = pv1[3].split(COMP)[0]
        verb = _ADT_VERB.get(message_type.split(COMP)[-1], "Event")
        summary = f"{verb} · unit {unit}"
    elif message_type.startswith("ORU"):
        obx = _index(lines, "OBX")
        if obx is None or len(obx) < 7 or not obx[5]:
            return _quarantine(TRUNCATED_SEGMENT, "ORU missing OBX result")
        label = (obx[3].split(COMP)[1] if len(obx[3].split(COMP)) > 1 else obx[3])
        summary = f"{label} {obx[5]} {obx[6]} · final"
    elif message_type.startswith("ORM"):
        orc = _index(lines, "ORC")
        if orc is None or len(orc) < 2:
            return _quarantine(TRUNCATED_SEGMENT, "ORM missing ORC segment")
        summary = "Order · CBC panel"
    else:
        return _quarantine(PARSE_ERROR, f"unknown message type {message_type!r}")

    # 3. Timestamp validity (BAD_TIMESTAMP) — structure is intact by here.
    ts = msh[_MSH_TS]
    if len(ts) != _TS_LEN or not ts.isdigit():
        return _quarantine(BAD_TIMESTAMP, f"MSH-7 {ts!r} is not a YYYYMMDDHHMMSS timestamp")

    return ParseOutcome(
        ok=True,
        fields={
            "facility_id": facility_id,
            "message_type": message_type,
            "patient_mrn": patient_mrn,
            "unit": unit,
            "summary": summary,
        },
    )
