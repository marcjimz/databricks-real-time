"""Tests for the shared HL7 parser (pipelines/lib/hl7_parser.py).

TDD anchor for Phase 1: the parser is the one piece of business logic that must
never silently deviate. We keep the suite lean — a valid message per family, one
case per injected corruption, and a factory round-trip that asserts the parser's
classification matches the generator's ground-truth `_expected_error`.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.generator.hl7_factory import HL7Factory  # noqa: E402
from app.generator.profiles import LAB_RESULTS, PATIENT_MOVEMENT  # noqa: E402
from pipelines.lib.hl7_parser import parse_hl7  # noqa: E402

CR = "\r"


def _adt() -> str:
    return CR.join([
        r"MSH|^~\&|HL7GEN|FAC-01|RTI_DEMO|DATABRICKS|20260714120000||ADT^A01|evt1|P|2.5",
        "EVN|A01|20260714120000",
        "PID|1||MRN-911234^^^FAC-01^MR||DOE^JANE||19700101|F",
        "PV1|1|I|4W^312^A|||||||||||||||V1234",
    ])


def _oru() -> str:
    return CR.join([
        r"MSH|^~\&|HL7GEN|LAB-01|RTI_DEMO|DATABRICKS|20260714120500||ORU^R01|evt2|P|2.5",
        "PID|1||MRN-955678^^^LAB-01^MR||ROE^JOHN||19800202|M",
        "OBR|1|||CBC^Complete Blood Count^L",
        "OBX|1|NM|2823-3^Potassium^LN||4.1|mmol/L||||||F",
    ])


# -- valid messages ----------------------------------------------------------
def test_parses_valid_adt():
    out = parse_hl7(_adt())
    assert out.ok
    assert out.fields["message_type"] == "ADT^A01"
    assert out.fields["facility_id"] == "FAC-01"
    assert out.fields["patient_mrn"] == "MRN-911234"
    assert out.fields["unit"] == "4W"
    assert "Admit" in out.fields["summary"]


def test_parses_valid_oru():
    out = parse_hl7(_oru())
    assert out.ok
    assert out.fields["message_type"] == "ORU^R01"
    assert out.fields["patient_mrn"] == "MRN-955678"
    assert "Potassium" in out.fields["summary"]
    assert "4.1" in out.fields["summary"]


# -- injected corruptions ----------------------------------------------------
def test_bad_separator_quarantined():
    corrupt = _adt().replace("|", "\u00a6", 3)  # broken field separator (matches factory)
    out = parse_hl7(corrupt)
    assert not out.ok
    assert out.error_code == "BAD_SEPARATOR"


def test_bad_timestamp_quarantined():
    corrupt = _adt().replace("20260714120000", "NOT-A-TIMESTAMP", 1)
    out = parse_hl7(corrupt)
    assert not out.ok
    assert out.error_code == "BAD_TIMESTAMP"


def test_truncated_segment_quarantined():
    raw = _adt()
    corrupt = raw[: max(20, int(len(raw) * 0.4))]  # matches factory truncation
    out = parse_hl7(corrupt)
    assert not out.ok
    assert out.error_code == "TRUNCATED_SEGMENT"


# -- factory round-trip: parser classification must match ground truth -------
def test_factory_roundtrip_classification():
    for profile in (PATIENT_MOVEMENT, LAB_RESULTS):
        factory = HL7Factory(profile, worker_id="test", malformed_pct=50.0)
        for _ in range(200):
            rec = factory.make()
            out = parse_hl7(rec["hl7_raw"])
            expected = rec["_expected_error"]
            if expected:
                assert not out.ok, f"expected quarantine for {expected}: {rec['hl7_raw']!r}"
                assert out.error_code == expected, (
                    f"misclassified {expected} as {out.error_code}: {rec['hl7_raw']!r}"
                )
            else:
                assert out.ok, f"clean message wrongly quarantined: {rec['hl7_raw']!r}"
