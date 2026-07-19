"""Central configuration for the HL7 Real-Time Intelligence demo.

Every tunable is read from the environment so the same code runs locally, in a
Databricks App, and on a jobs cluster. Defaults target the reference workspace
(``fevm-marcjimz-demo-ws-2``, us-west-2, workspace id 7474655909926918) but are
overridable — future consumers bring their own catalog, workspace, and Azure
subscription.

A local ``.env`` (git-ignored) is loaded on import for developer convenience.
In a deployed Databricks App the values arrive as real environment variables
(including the app service principal's ``DATABRICKS_CLIENT_ID`` /
``DATABRICKS_CLIENT_SECRET``).
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path


def _load_dotenv() -> None:
    """Minimal .env loader (no third-party dependency).

    Only sets keys that are not already present in the environment, so real
    environment variables always win over the file.
    """
    for candidate in (Path(__file__).resolve().parent.parent / ".env", Path.cwd() / ".env"):
        if not candidate.is_file():
            continue
        for raw in candidate.read_text().splitlines():
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            key, value = key.strip(), value.strip().strip('"').strip("'")
            os.environ.setdefault(key, value)


_load_dotenv()


def _env(name: str, default: str = "") -> str:
    return os.environ.get(name, default)


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    return int(raw) if raw else default


def _normalize_url(url: str) -> str:
    """Ensure a URL carries an https:// scheme and no trailing slash.

    Databricks Apps inject DATABRICKS_HOST as a bare hostname (e.g.
    ``dbc-x.cloud.databricks.com``); local CLI profiles supply it with a scheme.
    Normalize both so downstream URL building (OAuth token endpoint, Zerobus
    insert) always produces a valid absolute URL."""
    url = (url or "").strip().rstrip("/")
    if not url:
        return url
    if not url.startswith(("http://", "https://")):
        url = f"https://{url}"
    return url


@dataclass(frozen=True)
class Config:
    # --- Databricks workspace -------------------------------------------------
    # Databricks Apps auto-inject DATABRICKS_HOST as a BARE hostname (no scheme),
    # which makes the OAuth token_url schemeless and httpx rejects it ("Request
    # URL is missing an 'http://' or 'https://' protocol"). Normalize to always
    # carry https:// so the same code works whether the host arrives bare (App),
    # scheme-prefixed (local CLI profile), or from the default below.
    workspace_url: str = _normalize_url(_env(
        "DATABRICKS_HOST", "https://fevm-real-time-mode-demo.cloud.databricks.com"
    ))
    workspace_id: str = _env("DATABRICKS_WORKSPACE_ID", "7474653348487172")
    region: str = _env("CLOUD_REGION", "us-east-1")

    # --- Service principal (app identity; client-credentials OAuth) -----------
    client_id: str = _env("DATABRICKS_CLIENT_ID")
    client_secret: str = _env("DATABRICKS_CLIENT_SECRET")

    # --- Unity Catalog --------------------------------------------------------
    # Catalog is a pre-existing prerequisite; we only create the schema/tables.
    catalog: str = _env("HL7_CATALOG", "real_time_mode_demo_catalog")
    schema: str = _env("HL7_SCHEMA", "rti_demo")

    # --- Ingestion path -------------------------------------------------------
    ingest_path: str = _env("INGEST_PATH", "zerobus")  # {zerobus, eventhub}

    # --- Event Hubs (Path B; populated by Terraform outputs in Phase 3) -------
    eventhub_bootstrap: str = _env("EVENTHUB_BOOTSTRAP")  # host:9093
    eventhub_connection_string: str = _env("EVENTHUB_CONNECTION_STRING")
    eventhub_topic: str = _env("EVENTHUB_TOPIC", "hl7-events")

    # --- Lakebase (Postgres) --------------------------------------------------
    # In a deployed App with a Lakebase `database` resource attached, the runtime
    # auto-injects PGHOST/PGUSER/PGPORT/PGDATABASE (but never PGPASSWORD — the app
    # mints an OAuth token per connection). Prefer those; fall back to LAKEBASE_*
    # for the jobs cluster and local dev.
    lakebase_host: str = _env("PGHOST") or _env("LAKEBASE_HOST")
    lakebase_port: int = _env_int("PGPORT", 0) or _env_int("LAKEBASE_PORT", 5432)
    lakebase_database: str = _env("PGDATABASE") or _env("LAKEBASE_DATABASE", "rti_demo")
    lakebase_user: str = _env("PGUSER") or _env("LAKEBASE_USER")  # SP application id
    lakebase_instance: str = _env("LAKEBASE_INSTANCE", "rti-demo")
    # Autoscaling endpoint path used to mint the short-lived OAuth password.
    endpoint_name: str = _env("ENDPOINT_NAME")

    @property
    def zerobus_endpoint(self) -> str:
        """Derived Zerobus Direct Write host for this workspace + region."""
        override = _env("ZEROBUS_ENDPOINT")
        if override:
            return override
        return f"https://{self.workspace_id}.zerobus.{self.region}.cloud.databricks.com"

    @property
    def zerobus_resource(self) -> str:
        """OAuth ``resource`` audience for the Zerobus Direct Write API."""
        return f"api://databricks/workspaces/{self.workspace_id}/zerobusDirectWriteApi"

    @property
    def token_url(self) -> str:
        return f"{self.workspace_url.rstrip('/')}/oidc/v1/token"

    def table(self, name: str) -> str:
        """Fully-qualified UC table name, e.g. ``catalog.schema.bronze_hl7_raw``."""
        return f"{self.catalog}.{self.schema}.{name}"


CONFIG = Config()
