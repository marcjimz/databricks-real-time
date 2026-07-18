"""Path A front door: Zerobus Direct Write via the REST ingest API.

The app writes bronze directly with batched HTTP POSTs — no broker, no ingest
job. We use the stateless REST interface (not the gRPC SDK) so the app stays
pure-Python with no native/Rust build step.

Contract (verified against docs.databricks.com/aws/en/ingestion/zerobus-ingest):

  Token:  POST {workspace_url}/oidc/v1/token
          basic-auth (client_id:client_secret)
          grant_type=client_credentials, scope=all-apis,
          resource=api://databricks/workspaces/{id}/zerobusDirectWriteApi,
          authorization_details=[ UC privilege grants for the target table ]
          -> { access_token, expires_in, token_type }  (hourly expiry)

  Insert: POST {zerobus_endpoint}/zerobus/v1/tables/{cat}.{schema}.{table}/insert
          Authorization: Bearer {token}, Content-Type: application/json
          body = raw JSON array of record objects  [ {...}, {...} ]
          -> HTTP 200 + empty {} once the batch is durably committed

The token is table-scoped: the authorization_details request the exact Unity
Catalog privileges (USE CATALOG / USE SCHEMA / SELECT+MODIFY) the ingest needs,
and the minted token carries only those claims.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass

import httpx

from config import Config


def _authorization_details(catalog: str, schema: str, table_fqn: str) -> list[dict]:
    """UC privilege grants for a Zerobus token scoped to one table."""
    return [
        {"type": "unity_catalog_privileges", "privileges": ["USE CATALOG"],
         "object_type": "CATALOG", "object_full_path": catalog},
        {"type": "unity_catalog_privileges", "privileges": ["USE SCHEMA"],
         "object_type": "SCHEMA", "object_full_path": f"{catalog}.{schema}"},
        {"type": "unity_catalog_privileges", "privileges": ["SELECT", "MODIFY"],
         "object_type": "TABLE", "object_full_path": table_fqn},
    ]


@dataclass
class InsertResult:
    """Outcome of one batched insert POST."""

    ok: bool
    count: int
    latency_ms: float
    status_code: int
    error: str = ""


class ZerobusTokenManager:
    """Fetches and caches the Zerobus Direct Write OAuth token.

    Client-credentials M2M flow with the workspace-scoped ``resource`` audience.
    Tokens are refreshed a minute before expiry.
    """

    _REFRESH_SKEW_S = 60

    def __init__(self, cfg: Config, client: httpx.AsyncClient, authorization_details: list[dict]):
        if not cfg.client_id or not cfg.client_secret:
            raise ValueError(
                "DATABRICKS_CLIENT_ID / DATABRICKS_CLIENT_SECRET are required for "
                "Zerobus ingest (the app service principal credentials)."
            )
        self._cfg = cfg
        self._client = client
        self._authz = json.dumps(authorization_details)
        self._token: str = ""
        self._expires_at: float = 0.0

    async def token(self) -> str:
        if self._token and time.monotonic() < self._expires_at:
            return self._token
        resp = await self._client.post(
            self._cfg.token_url,
            auth=(self._cfg.client_id, self._cfg.client_secret),
            data={
                "grant_type": "client_credentials",
                "scope": "all-apis",
                "resource": self._cfg.zerobus_resource,
                "authorization_details": self._authz,
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        resp.raise_for_status()
        payload = resp.json()
        self._token = payload["access_token"]
        self._expires_at = time.monotonic() + int(payload.get("expires_in", 3600)) - self._REFRESH_SKEW_S
        return self._token


class ZerobusRestClient:
    """Async batched writer to the Zerobus REST insert endpoint for one table."""

    def __init__(self, cfg: Config, table: str, client: httpx.AsyncClient | None = None):
        self._cfg = cfg
        # HTTP/2 + keep-alive connection pooling is what makes REST viable at
        # rate — every worker reuses persistent connections to the endpoint.
        self._client = client or httpx.AsyncClient(
            http2=True,
            timeout=httpx.Timeout(30.0, connect=10.0),
            limits=httpx.Limits(max_connections=64, max_keepalive_connections=32),
        )
        self._owns_client = client is None
        authz = _authorization_details(cfg.catalog, cfg.schema, table)
        self._tokens = ZerobusTokenManager(cfg, self._client, authz)
        self._insert_url = f"{cfg.zerobus_endpoint}/zerobus/v1/tables/{table}/insert"

    async def insert(self, records: list[dict]) -> InsertResult:
        """POST a batch; HTTP 200 is the durability ack. Retries once on 401."""
        if not records:
            return InsertResult(ok=True, count=0, latency_ms=0.0, status_code=200)

        start = time.perf_counter()
        for attempt in (1, 2):
            token = await self._tokens.token()
            try:
                resp = await self._client.post(
                    self._insert_url,
                    json=records,
                    headers={
                        "Authorization": f"Bearer {token}",
                        "Content-Type": "application/json",
                    },
                )
            except httpx.HTTPError as exc:
                return InsertResult(
                    ok=False, count=len(records),
                    latency_ms=(time.perf_counter() - start) * 1000,
                    status_code=0, error=f"{type(exc).__name__}: {exc}",
                )
            if resp.status_code == 401 and attempt == 1:
                self._tokens._token = ""  # force refresh, retry once
                continue
            latency_ms = (time.perf_counter() - start) * 1000
            if resp.status_code == 200:
                return InsertResult(ok=True, count=len(records), latency_ms=latency_ms, status_code=200)
            return InsertResult(
                ok=False, count=len(records), latency_ms=latency_ms,
                status_code=resp.status_code, error=resp.text[:500],
            )
        return InsertResult(ok=False, count=len(records), latency_ms=0.0, status_code=401, error="auth failed")

    async def aclose(self) -> None:
        if self._owns_client:
            await self._client.aclose()
