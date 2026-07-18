"""HL7 v2.5 message factory (spec section 3).

Builds pipe-delimited HL7 v2.5 messages (MSH/EVN/PID/PV1/OBR/OBX) with
obviously-synthetic patient data and LOINC-coded observations, then wraps each
in the bronze envelope record that Path A POSTs to Zerobus and Path B produces
to Event Hubs.

A malformed injector corrupts a tunable fraction of messages in one of three
ways so the downstream parser has something to quarantine — the error codes
here match the parser's classification in pipelines/lib/hl7_parser.py.
"""

from __future__ import annotations

import random
import uuid
from datetime import datetime, timezone

from .profiles import Profile

# HL7 encoding characters: field | component ^ repetition ~ escape \ subcomponent &
FIELD, COMP = "|", "^"
ENCODING_CHARS = r"^~\&"

# LOINC-coded observations for ORU^R01 (code, label, unit, low, high)
_LOINC = [
    ("2823-3", "Potassium", "mmol/L", 3.5, 5.1),
    ("718-7", "Hemoglobin", "g/dL", 11.0, 16.0),
    ("2951-2", "Sodium", "mmol/L", 135.0, 145.0),
    ("2345-7", "Glucose", "mg/dL", 70.0, 110.0),
    ("2160-0", "Creatinine", "mg/dL", 0.6, 1.3),
]
_UNITS = ["4W", "2E", "5N", "ICU", "3S"]
_ERROR_CODES = ("TRUNCATED_SEGMENT", "BAD_SEPARATOR", "BAD_TIMESTAMP")


def _hl7_ts(dt: datetime) -> str:
    return dt.strftime("%Y%m%d%H%M%S")


def _weighted_choice(mix: dict[str, float]) -> str:
    types = list(mix.keys())
    weights = list(mix.values())
    return random.choices(types, weights=weights, k=1)[0]


class HL7Factory:
    """Generates bronze envelope records for one generator worker."""

    def __init__(self, profile: Profile, worker_id: str, malformed_pct: float = 0.0):
        self.profile = profile
        self.worker_id = worker_id
        self.malformed_pct = malformed_pct
        self._seq = 0

    def set_profile(self, profile: Profile) -> None:
        self.profile = profile

    # -- segment builders ----------------------------------------------------
    def _msh(self, facility: str, msg_type: str, event_id: str, ts: str) -> str:
        return FIELD.join(
            ["MSH", ENCODING_CHARS, "HL7GEN", facility, "RTI_DEMO", "DATABRICKS",
             ts, "", msg_type, event_id, "P", "2.5"]
        )

    def _pid(self, mrn: str, facility: str) -> str:
        sex = random.choice(["M", "F"])
        dob = f"19{random.randint(40, 99)}{random.randint(1, 12):02d}{random.randint(1, 28):02d}"
        pid3 = COMP.join([mrn, "", "", facility, "MR"])
        name = COMP.join([random.choice(["DOE", "ROE", "TESTPATIENT"]), random.choice(["JANE", "JOHN", "PAT"])])
        return FIELD.join(["PID", "1", "", pid3, "", name, "", dob, sex])

    def _pv1(self, unit: str) -> str:
        loc = COMP.join([unit, str(random.randint(100, 599)), "A"])
        return FIELD.join(["PV1", "1", "I", loc, "", "", "", "", "", "", "", "", "", "", "", "", "", "V" + str(random.randint(1000, 9999))])

    def _obx(self, idx: int) -> tuple[str, str]:
        code, label, unit, low, high = random.choice(_LOINC)
        value = round(random.uniform(low * 0.85, high * 1.15), 1)
        obs = COMP.join([code, label, "LN"])
        seg = FIELD.join(["OBX", str(idx), "NM", obs, "", str(value), unit, "", "", "", "", "F"])
        return seg, f"{label} {value} {unit}"

    # -- record assembly -----------------------------------------------------
    def make(self) -> dict:
        """Return a bronze envelope record dict (JSON-serialisable)."""
        self._seq += 1
        prof = self.profile
        msg_type = _weighted_choice(prof.mix)
        facility = random.choice(prof.facilities)
        mrn = f"{prof.mrn_prefix}{random.randint(1000, 9999)}"
        unit = random.choice(_UNITS)
        now = datetime.now(timezone.utc)
        event_id = uuid.uuid4().hex
        ts = _hl7_ts(now)

        segments = [self._msh(facility, msg_type, event_id, ts)]
        summary_bits: list[str] = []

        if msg_type.startswith("ADT"):
            segments.append(FIELD.join(["EVN", msg_type.split(COMP)[1], ts]))
            segments.append(self._pid(mrn, facility))
            segments.append(self._pv1(unit))
            verb = {"A01": "Admit", "A03": "Discharge", "A08": "Update"}.get(msg_type.split(COMP)[1], "Event")
            summary_bits.append(f"{verb} · unit {unit}")
        elif msg_type.startswith("ORU"):
            segments.append(self._pid(mrn, facility))
            segments.append(FIELD.join(["OBR", "1", "", "", COMP.join(["CBC", "Complete Blood Count", "L"])]))
            n_obx = random.randint(max(1, prof.obx_min), max(1, prof.obx_max))
            for i in range(1, n_obx + 1):
                seg, desc = self._obx(i)
                segments.append(seg)
                if i == 1:
                    summary_bits.append(desc + " · final")
        else:  # ORM^O01 order
            segments.append(self._pid(mrn, facility))
            segments.append(FIELD.join(["ORC", "NW", "ORD" + str(random.randint(10000, 99999))]))
            summary_bits.append("Order · CBC panel")

        hl7_raw = "\r".join(segments)
        error_code = self._maybe_corrupt(msg_type)
        if error_code:
            hl7_raw = self._corrupt(hl7_raw, error_code)

        return {
            "event_id": event_id,
            "source_path": "",  # stamped by supervisor per active path
            "facility_id": facility,
            "message_type": msg_type,
            "hl7_raw": hl7_raw,
            "gen_worker_id": self.worker_id,
            "ts_generated": now.isoformat().replace("+00:00", "Z"),
            "_summary": " ".join(summary_bits) or msg_type,  # underscore = generator-only hint
            "_expected_error": error_code or "",
        }

    # -- malformed injection -------------------------------------------------
    def _maybe_corrupt(self, msg_type: str) -> str | None:
        if self.malformed_pct <= 0 or random.random() >= self.malformed_pct / 100.0:
            return None
        return random.choice(_ERROR_CODES)

    def _corrupt(self, hl7_raw: str, error_code: str) -> str:
        if error_code == "TRUNCATED_SEGMENT":
            return hl7_raw[: max(20, int(len(hl7_raw) * 0.4))]
        if error_code == "BAD_SEPARATOR":
            return hl7_raw.replace("|", "¦", 3)  # broken field separator
        if error_code == "BAD_TIMESTAMP":
            parts = hl7_raw.split("\r", 1)
            fields = parts[0].split(FIELD)
            if len(fields) > 6:
                fields[6] = "NOT-A-TIMESTAMP"
            head = FIELD.join(fields)
            return head + ("\r" + parts[1] if len(parts) > 1 else "")
        return hl7_raw
