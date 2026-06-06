# Control Panel Testing Plan

Assessment date: 2026-04-27 UTC (source paths updated 2026-06-06)

This page defines how to test the Medical Warehouse control panel based on the current implementation in `src/medwarehouse/platform/`, the UI template, the JSON endpoints, and the existing automated tests.

## Scope

The control panel currently includes:

- a Flask web UI served by `python -m medwarehouse platform serve`
- JSON status and control APIs
- infrastructure start/stop actions
- job start/stop actions
- status reporting for environment, warehouse, orchestration, coverage, roles, and local data artifacts

Primary source files:

- `src/medwarehouse/platform/api/app.py` — Flask application factory
- `src/medwarehouse/platform/api/routes.py` — API + page route blueprints
- `src/medwarehouse/platform/control/` — Job runner, infra actions, `ProcessSupervisor`, job catalog
- `src/medwarehouse/platform/services/status.py` — `StatusService` (TTL cache + probes)
- `src/medwarehouse/platform/services/alerts/` — Alert engine and rule modules
- `src/medwarehouse/platform/probes/` — `InfraProbe`, `WarehouseProbe`, `PipelineProbe`, etc.
- `src/medwarehouse/platform/ui/templates/` — Jinja2 templates (base.html + page templates)
- `src/medwarehouse/platform/control/catalog.py` — `JOB_SPECS` catalog
- `tests/test_platform.py`

## Test objectives

1. Confirm that the control panel loads and exposes the expected status information.
2. Confirm that UI controls and JSON APIs trigger the correct backend actions.
3. Confirm that the control panel behaves safely when infrastructure, warehouse, or jobs are unavailable.
4. Confirm that the control panel remains usable, understandable, and responsive in normal local-development conditions.

## Test environments

### Minimum local setup

```bash
export PYTHONPATH=src
set -a && source .env && set +a
python -m medwarehouse platform serve --host 127.0.0.1 --port 8787
```

### Recommended environment variants

- Variant A: Docker available, all services stopped
- Variant B: Docker available, core services running
- Variant C: Docker unavailable or intentionally mocked as unavailable
- Variant D: Analytics DB reachable
- Variant E: Analytics DB unreachable or wrong credentials
- Variant F: Jobs idle
- Variant G: One long-running job active, such as `inventory_bronze`

## Functional requirements

| ID | Requirement | Expected behavior | Test type | Current coverage |
| --- | --- | --- | --- | --- |
| FR-01 | The control panel root page shall render successfully | `GET /` returns HTTP 200 and shows the main control-plane sections | Automated + manual | Partial |
| FR-02 | A health endpoint shall be available | `GET /health` returns a simple healthy response | Manual + automated | Partial |
| FR-03 | A JSON status endpoint shall expose snapshot, jobs, and infra state | `GET /api/status` returns structured JSON with those top-level keys | Manual + automated | Partial |
| FR-04 | The page shall display infrastructure overall status and per-service status | UI shows overall state and service rows from Docker inspection | Manual | Gap |
| FR-05 | The page shall allow starting infrastructure | `POST /api/infra/start` and form flow trigger `start_services.sh` through the infra controller | Manual + automated | Partial |
| FR-06 | The page shall allow stopping infrastructure | `POST /api/infra/stop` and form flow trigger `stop_services.sh` through the infra controller | Manual + automated | Partial |
| FR-07 | The page shall display environment metadata | UI shows environment name, `.env` presence, Kafka target, DB targets, and project root | Automated + manual | Partial |
| FR-08 | The page shall display domain coverage information | UI shows inventory, procurement, and sales coverage cards | Manual | Gap |
| FR-09 | The page shall display configured warehouse role names and detected roles | UI shows configured role names and warehouse-detected roles or `unavailable` | Manual | Gap |
| FR-10 | The page shall display orchestration metadata | UI shows DAG id, DAG path, and DAG existence status | Partial automated + manual | Partial |
| FR-11 | The page shall display warehouse reachability and key row counts | UI shows reachability, target JDBC URL, snapshot row count, fact row count, staging row count, and schemas | Manual | Gap |
| FR-12 | The page shall display Bronze, Silver, and Quarantine artifact summaries | UI shows existence, file count, updated time, and path for each artifact | Manual | Gap |
| FR-13 | The page shall list all configured pipeline jobs | All entries from `JOB_SPECS` appear in the UI and status payload | Manual + automated | Gap |
| FR-14 | The page shall allow starting configured jobs | `POST /api/jobs/<job_id>/start` starts the requested command and returns updated job state | Manual + automated | Partial |
| FR-15 | The page shall allow stopping long-running jobs | `POST /api/jobs/<job_id>/stop` stops a running long job and returns updated job state | Manual + automated | Partial |
| FR-16 | The page shall prevent duplicate starts of the same running job | Repeated start requests for an active job return `ok=false` with a clear message | Manual + automated | Gap |
| FR-17 | The page shall reject stop requests for jobs that are not running | Stop request returns `ok=false` with a clear message | Manual + automated | Gap |
| FR-18 | The page shall reject unknown job ids gracefully | Unknown job requests return a non-crashing JSON response with `ok=false` | Manual + automated | Gap |
| FR-19 | The page shall reject unknown infra actions gracefully | Unknown infra action returns a non-crashing JSON response with `ok=false` | Manual + automated | Complete — `_ALLOWED_INFRA_ACTIONS` whitelist enforced in `api/routes.py` |
| FR-20 | The page shall show recent job logs and infra logs | Latest log lines appear in the UI after command execution | Manual | Gap |
| FR-21 | The refresh button shall reload the page | Clicking refresh reloads the current page without breaking state | Manual | Gap |
| FR-22 | The button-based UI controls shall call the correct JSON endpoints | Clicking start/stop/refresh buttons performs the intended action and reload flow | Manual | Gap |
| FR-23 | Warehouse connection failures shall be surfaced with actionable hints | UI shows an error note rather than crashing when DB is unreachable or auth fails | Manual | Partial |
| FR-24 | Missing Docker or Compose availability shall be surfaced clearly | Infrastructure section shows `unavailable` and explanatory text | Automated + manual | Partial |

### Functional test checklist

#### Page and API smoke

1. Start the control panel.
2. Open `http://127.0.0.1:8787/`.
3. Confirm the page renders with sections for Infrastructure, Environment, Coverage, Roles, Orchestration, Warehouse, Artifacts, and Pipeline Controls.
4. Call `GET /health` and confirm a healthy JSON response.
5. Call `GET /api/status` and confirm the JSON includes `snapshot`, `jobs`, and `infra`.

#### Infrastructure controls

1. With Docker available and services stopped, click `Start Services`.
2. Confirm the infra action enters `running`, then reaches `succeeded` or a clear `failed` state.
3. Confirm recent infra logs appear in the infrastructure log panel.
4. Confirm per-service badges update after refresh.
5. Repeat with `Stop Services`.
6. Attempt a second infra action while one is already running and confirm it is rejected cleanly.

#### Job controls

1. Start a short job such as `Inventory Producer`.
2. Confirm the job badge changes from `idle` to `running`, then to `succeeded` or `failed`.
3. Confirm PID, timestamps, and logs update.
4. Start a long-running job such as `Inventory Bronze Stream`.
5. Confirm the `Stop` button is available for long-running jobs.
6. Stop the job and confirm the state transitions through `stopping` to `stopped`.
7. Try starting the same long-running job twice and confirm the second request is rejected.
8. Try stopping an idle job and confirm the request is rejected without crashing the page.

#### Status content

1. With `.env` present, confirm the environment section shows `yes`.
2. Temporarily test with `.env` absent or a mocked project root and confirm the badge changes appropriately.
3. With services stopped, confirm warehouse reachability is `no` and an explanatory note is displayed.
4. With services running and warehouse bootstrapped, confirm counts and schemas populate.
5. Confirm Bronze, Silver, and Quarantine artifact summaries reflect actual filesystem state.
6. Confirm coverage cards correctly show:
   - inventory as end-to-end implemented
   - procurement as end-to-end implemented
   - sales as end-to-end implemented

#### Failure handling

1. Run with Docker unavailable and confirm the infrastructure section shows `unavailable`.
2. Run with wrong analytics DB credentials and confirm warehouse failure is shown as a note, not a server error.
3. Call `POST /api/jobs/unknown/start` and confirm a safe JSON error response.
4. Call `POST /api/infra/unknown` and confirm a safe JSON error response.

## Non-functional requirements

| ID | Requirement | Target expectation | How to test | Current confidence |
| --- | --- | --- | --- | --- |
| NFR-01 | Availability | Control panel should start locally with a single command when Python dependencies are installed | Start server from CLI and open `/` and `/health` | Medium |
| NFR-02 | Fault tolerance | Backend should return controlled error states instead of crashing when Docker or warehouse is unavailable | Test stopped infra, wrong DB credentials, unknown job ids, unknown infra actions | Medium |
| NFR-03 | Responsiveness | Page load and JSON status should feel fast for a local setup, ideally within about 1 to 2 seconds for typical local data volumes | Measure browser load time and `curl` timing for `/` and `/api/status` | Low |
| NFR-04 | Concurrency safety | Repeated user clicks should not create duplicate job starts or overlapping infra actions | Attempt repeated starts/stops rapidly and verify lock-based rejection | Medium |
| NFR-05 | Observability | Recent logs, PID, timestamps, and status badges should provide enough context to debug local runs | Execute jobs and review logs shown in UI | Medium |
| NFR-06 | Usability | UI labels and statuses should be understandable to an operator without reading source code | Manual UX review with stopped and running states | Medium |
| NFR-07 | Accessibility | Basic keyboard usage, readable contrast, and responsive layout should work on desktop and mobile widths | Manual keyboard and viewport testing | Low |
| NFR-08 | Security posture | UI should avoid arbitrary command execution; only predefined job specs and fixed infra scripts should be invokable | Review job catalog and request surface; confirm unknown ids are rejected | Medium |
| NFR-09 | Data accuracy | Status values shown in the UI should match the underlying Docker, filesystem, and database state at refresh time | Cross-check UI values with CLI commands and local files | Medium |
| NFR-10 | Maintainability | Control panel behavior should remain covered by automated tests as routes and sections evolve | Review and extend `tests/test_platform.py` | Low to medium |
| NFR-11 | Portability | The control panel should work across local environments where Python, Docker, and Compose path variants differ | Test with `docker compose` and `docker-compose` availability | Medium |
| NFR-12 | Resource usage | Idle control-panel operation should remain lightweight in CPU and memory | Observe process usage while idle and while refreshing status | Low |

## Non-functional test checklist

### Performance and responsiveness

1. Measure `GET /` and `GET /api/status` response times with services stopped.
2. Repeat with services running and warehouse reachable.
3. Confirm the page remains usable when artifact directories contain many parquet files.

### Concurrency and stability

1. Double-click `Start` on a long-running job.
2. Rapidly issue repeated `POST /api/jobs/<job_id>/start` requests.
3. Rapidly issue repeated `POST /api/infra/start` requests.
4. Confirm only one active process exists for each protected action.

### Usability and layout

1. Verify the page at desktop width.
2. Verify the page below `900px` width and confirm the grid collapses cleanly into one column.
3. Verify logs, code blocks, and long file paths wrap without breaking layout.
4. Check that status colors remain distinguishable.

### Security and safety

1. Confirm only known jobs from `JOB_SPECS` can be started.
2. Confirm only `start` and `stop` are accepted as infra actions.
3. Confirm command strings shown in the UI are descriptive and match the actual backend action.
4. Review whether any sensitive values are unintentionally exposed in the UI, especially full connection targets or auth-related error text.

## Existing automated coverage

Current tests in `tests/test_platform.py` cover:

- snapshot structure exists
- index page renders
- `GET /health`
- `GET /api/status`
- `POST /api/jobs/<job_id>/start`
- `POST /api/jobs/<job_id>/stop`
- `POST /api/infra/<action>`
- CLI `platform serve` calls the platform server
- Docker-missing path returns an unavailable status
- control-plane service duplicate-start rejection for jobs
- control-plane service idle-stop rejection for jobs
- control-plane service unknown infra-action rejection

Current automated coverage does not yet cover:

- unknown job or infra action handling
- duplicate-start handling through the public API
- stop-on-idle handling through the public API
- UI rendering of dynamic job lists
- responsive UI behavior
- live warehouse status behavior
- actual subprocess launch/stop lifecycle
- persistence and restart recovery behavior

## Recommended automation backlog

1. Add API tests for unknown job ids and unknown infra actions.
2. Add API-level tests for duplicate job starts and stop-on-idle behavior.
3. Add direct tests for actual subprocess launch, log capture, and stop escalation in the shared supervisor/runtime.
4. Add persistence and restart-recovery tests for the SQLite-backed control-plane store.
5. Add tests that assert the index includes key sections and at least one pipeline job card.
6. Add tests for warehouse error-hint formatting when authentication fails.
7. Add tests for live warehouse status behavior with mocked database cursors and partial failures.
8. Add browser-level checks for responsive layout and operator feedback on failed actions.

## Suggested release gate for the control panel

The control panel should be considered test-ready for routine local use when all of the following are true:

1. Functional smoke tests pass for `/`, `/health`, `/api/status`, job start, job stop, infra start, and infra stop.
2. The page renders correctly with services both stopped and running.
3. Unknown actions are rejected safely.
4. Long-running jobs can be stopped from the UI.
5. Basic responsiveness and mobile-layout checks pass.
6. Automated tests cover all public routes and the main controller edge cases.

## Overall assessment

- Current implementation maturity: good local-operator prototype
- Current automated test maturity: early
- Biggest functional testing gap: API route behavior and controller edge cases
- Biggest non-functional testing gap: responsiveness, usability, and concurrency verification
