"""Pytest path bootstrap.

The Databricks App is deployed with the *contents* of ``app/`` flattened to the
runtime source root, so the app modules import each other with flat top-level
names (``from config import ...``, ``from generator.supervisor import ...``).
To keep those same modules importable under the local package name (the tests
and scripts use ``from app.xxx import ...``), put BOTH the repo root and the
``app/`` directory on ``sys.path`` for the test session.
"""

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent
for _p in (_ROOT, _ROOT / "app"):
    _s = str(_p)
    if _s not in sys.path:
        sys.path.insert(0, _s)
