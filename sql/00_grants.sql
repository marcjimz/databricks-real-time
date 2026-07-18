-- Grant the demo service principal the access Zerobus Direct Write + the
-- streaming pipelines need. Rendered by scripts/run_sql.py with {catalog},
-- {schema}, {principal} (the SP application id).

GRANT USE CATALOG ON CATALOG {catalog} TO `{principal}`;
GRANT USE SCHEMA ON SCHEMA {catalog}.{schema} TO `{principal}`;
GRANT SELECT, MODIFY ON SCHEMA {catalog}.{schema} TO `{principal}`;
GRANT READ VOLUME, WRITE VOLUME ON VOLUME {catalog}.{schema}.checkpoints TO `{principal}`;
