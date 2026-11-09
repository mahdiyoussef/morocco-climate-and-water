# ETL guardrail rollout

The Airflow DAG runs source acquisition, raw Landing, normalization, normalized Landing, Spark Bronze and Silver, dbt Gold, lineage generation, and publication. The quality gates use those boundaries to stop failed runs before they replace the dashboard snapshot.

## Enforced today

| Boundary | Check | Failure behavior |
| --- | --- | --- |
| Acquisition and decoding | CHIRPS must have the expected ordered months and valid rainfall samples. ERA5-Land must have the exact UTC hourly sequence and one temperature and soil moisture value per hour. | Stop before downstream transformations. |
| MinIO normalized Landing | Require all seven tables, safe and distinct Parquet paths, and matching byte sizes and SHA-256 hashes. Write content-addressed objects before committing the manifest. Bronze reads the exact committed list. | Reject a malformed inventory or corrupt upload. The prior manifest continues to reference its prior Parquet files. |
| Bronze and Silver | Require rows in each input. Silver contracts quarantine invalid values and apply deterministic deduplication. | Stop when a required input is empty or a Silver record violates its contract. |
| Gold | Reconcile ERA5-Land hours in both directions at the site-month grain. Require each audited climate year to have both providers and all 12 calendar months. Keep existing dbt key, range, relationship and reconciliation tests. | Fail dbt and block the Airflow publication task. |
| Serving | PostgreSQL reconciles row counts inside the publishing transaction. The MinIO current pointer advances after the PostgreSQL commit. | A PostgreSQL transaction failure preserves the previous publication. |

## Rollout

| Phase | Work | Acceptance criteria |
| --- | --- | --- |
| 1. Identify each run | Put the Airflow run ID, requested years, site list version, manifest digest, and dbt invocation ID in a receipt. Bind the MinIO and dbt artifacts to that run. Require the full set of enabled critical tests from the corresponding dbt manifest. | Inject a previous run receipt, a skipped test, and a missing dataset. Each run fails before publication with a named rule. |
| 2. Isolate retries and promotion | Write raw inputs to versioned keys. Stage Gold against a run-specific snapshot and verify exports before promotion. Reconcile PostgreSQL, MinIO and local publication IDs. Provide a repair command for a commit that succeeds in PostgreSQL but fails while updating MinIO or local exports. | Interrupt publication at each boundary. A retry completes the same run or restores the previous snapshot. All readers agree on the published ID after repair. |
| 3. Monitor data changes | Record expected versus actual site, month and partition counts. Track rejects, duplicate keys, missing hours and source freshness by provider. Compare measurements against seasonal baselines and observe alerts before enforcing distribution thresholds. | Inject missing partitions and extreme values. Airflow reports the affected table and period. Blocking errors stop promotion and accepted seasonal variation stays visible in warnings. |
| 4. Rehearse recovery | Run a small real Spark, MinIO and PostgreSQL fixture on CI or a schedule. Test retry, concurrent publication, rollback and reader visibility. Assign a pipeline owner and retain inputs and reports to reproduce failed runs. | Demonstrate two successful runs, a deliberate fault preserving the last publication, and an operator recovering from the receipt. |

Block broken contracts, missing coverage, integrity failures and mismatched publication IDs. Warn on trends while thresholds are calibrated. Report the run ID, rule name, expected value, actual value and affected partition. Keep provider payloads and credentials out of alerts.

Run the default suite with the project dependencies installed.

    python -m pytest tests

Set the PostgreSQL connection variables and enable OBSERVATORY_POSTGRES_TESTS=1 to run the integration tests against a disposable database. Use the Airflow DAG and Docker stack to verify live source acquisition and the distributed data path.

The local Silver run defaults to a 1 GB DuckDB memory limit. Set OBSERVATORY_DUCKDB_MEMORY_LIMIT to adjust it for the available memory and the size of the input.

## Running the implemented guardrails

The Airflow DAG passes its run ID and logical date to every stage. Raw Landing objects are scoped to the run and addressed by SHA-256. The normalized manifest records the raw manifest digest used during decoding. Bronze and Silver store results in separate schemas and Delta paths for each run. A successful dbt build records its manifest, run results, invocation ID, input digests, expected years and site configuration under data/runtime/guardrails. A fresh dbt target directory prevents artifacts from previous builds from satisfying the gate.

Publication checks the exact site-month key set and expected leap-year-aware hourly coverage. It verifies exported objects before committing PostgreSQL. The transaction stores the export receipt with the Gold tables, and a session advisory lock serializes concurrent writers and repairs. Retrying a completed Airflow run leaves the committed publication in place. Retrying an incomplete promotion replays the verified objects from its PostgreSQL receipt.

If promotion is interrupted, run this command from the Compose project with the production service configuration.

    docker compose --env-file .env.morocco exec airflow-scheduler python -m observatory.publish repair

The repair command uses the current PostgreSQL receipt and checks the MinIO and local publication IDs after replay. Local dashboard loading pauses while the export files are being replaced. PostgreSQL can serve its newly committed snapshot before MinIO and local exports finish. A partial promotion is recoverable by retrying or running repair. If a committed export object is missing or corrupt, restore that exact object before retrying.

Quality reports live in data/metadata/quality_*.json. Prometheus exposes the latest stage status, duplicate counts, provider verification times, expected and actual coverage, hourly discrepancy and pending promotion. Seasonal checks compare mean values by site and calendar month with the prior PostgreSQL publication. They warn at an absolute change of 50 mm rainfall, 3 °C temperature, or 0.15 volumetric soil moisture. The thresholds are initial observation settings.

The CI recovery job uses a small local Spark fixture and disposable MinIO and PostgreSQL services. It exercises a failed MinIO pointer write, reader visibility, serialized writers and two repair attempts. The complete Airflow DAG against the production Spark, Hive and MinIO network remains the deployment verification. Retain run-scoped MinIO objects, dbt artifacts and the active publication's Delta tables so a failed run can be investigated or repaired.
