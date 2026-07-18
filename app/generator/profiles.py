"""Per-path data profiles (spec section 2.2).

Each ingestion path emits a distinct, recognisable clinical feed so the switch
is visible on the dashboard. These are presets — the live mix sliders still
override the weights at runtime.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class Profile:
    name: str
    facilities: tuple[str, ...]
    mrn_prefix: str
    # message_type -> relative weight
    mix: dict[str, float]
    # LOINC-coded observations available to ORU^R01 messages
    obx_min: int = 1
    obx_max: int = 1


# Path A — hospital patient-movement (ADT-heavy)
PATIENT_MOVEMENT = Profile(
    name="patient_movement",
    facilities=("FAC-01", "FAC-02", "FAC-03"),
    mrn_prefix="MRN-91",
    mix={"ADT^A01": 40.0, "ADT^A03": 25.0, "ADT^A08": 30.0, "ORM^O01": 5.0},
    obx_min=0,
    obx_max=0,
)

# Path B — reference-lab results (ORU-heavy, 1-5 OBX segments)
LAB_RESULTS = Profile(
    name="lab_results",
    facilities=("LAB-01", "LAB-02"),
    mrn_prefix="MRN-95",
    mix={"ORU^R01": 70.0, "ORM^O01": 20.0, "ADT^A08": 10.0},
    obx_min=1,
    obx_max=5,
)

PROFILE_BY_PATH: dict[str, Profile] = {
    "zerobus": PATIENT_MOVEMENT,
    "eventhub": LAB_RESULTS,
}


def profile_for(path: str) -> Profile:
    return PROFILE_BY_PATH.get(path, PATIENT_MOVEMENT)
