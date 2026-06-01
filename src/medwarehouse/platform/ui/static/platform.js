(function () {
  const page = document.body.dataset.page || "dashboard";
  const initialStatus = JSON.parse(
    document.getElementById("initial-status").textContent || "{}",
  );

  const state = {
    page,
    status: initialStatus,
    alertSeverity: "all",
    alertSource: "all",
    refreshInFlight: false,
  };

  function byId(id) {
    return document.getElementById(id);
  }

  function escapeHtml(value) {
    return String(value ?? "")
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;")
      .replaceAll('"', "&quot;")
      .replaceAll("'", "&#39;");
  }

  function toneForSeverity(severity) {
    if (severity === "CRITICAL") {
      return "critical";
    }
    if (severity === "WARNING") {
      return "warning";
    }
    return "neutral";
  }

  function toneForStatus(status) {
    const normalized = String(status || "").toLowerCase();
    if (["critical", "failed", "unavailable", "error", "orphaned"].includes(normalized)) {
      return "critical";
    }
    if (
      ["warning", "partial", "stopped", "missing", "idle", "unknown", "stop_requested", "stopping"].includes(
        normalized,
      )
    ) {
      return "warning";
    }
    if (["running", "healthy", "ok", "succeeded", "fresh"].includes(normalized)) {
      return "healthy";
    }
    return "neutral";
  }

  function formatNumber(value) {
    if (value === null || value === undefined) {
      return "n/a";
    }
    return Number(value).toLocaleString();
  }

  function formatValue(value) {
    if (value === null || value === undefined || value === "") {
      return "n/a";
    }
    return escapeHtml(value);
  }

  function formatLag(value) {
    if (value === null || value === undefined) {
      return "n/a";
    }
    const seconds = Number(value);
    if (Number.isNaN(seconds)) {
      return "n/a";
    }
    if (seconds < 60) {
      return `${Math.round(seconds)}s`;
    }
    if (seconds < 3600) {
      return `${(seconds / 60).toFixed(1)}m`;
    }
    if (seconds < 86400) {
      return `${(seconds / 3600).toFixed(1)}h`;
    }
    return `${(seconds / 86400).toFixed(1)}d`;
  }

  function listOrFallback(values) {
    if (!values || values.length === 0) {
      return "n/a";
    }
    return escapeHtml(values.join(", "));
  }

  function setText(id, value) {
    const element = byId(id);
    if (element) {
      element.textContent = value;
    }
  }

  function setHTML(id, html) {
    const element = byId(id);
    if (element) {
      element.innerHTML = html;
    }
  }

  function showFeedback(message, tone) {
    const banner = byId("feedbackBanner");
    if (!banner) {
      return;
    }
    banner.textContent = message;
    banner.className = `feedback tone-${tone}`;
    banner.classList.remove("hidden");
  }

  function clearFeedback() {
    const banner = byId("feedbackBanner");
    if (!banner) {
      return;
    }
    banner.textContent = "";
    banner.className = "feedback hidden";
  }

  function renderAlertCard(alert) {
    return `
      <article class="alert-card severity-${escapeHtml(alert.severity.toLowerCase())}">
        <div class="row-spread">
          <strong>${escapeHtml(alert.name)}</strong>
          <span class="pill tone-${toneForSeverity(alert.severity)}">${escapeHtml(alert.severity)}</span>
        </div>
        <p>${escapeHtml(alert.message)}</p>
        <p class="muted compact">${escapeHtml(alert.source)} · ${escapeHtml(alert.timestamp)}</p>
      </article>
    `;
  }

  function filteredActiveAlerts() {
    const alerts = state.status.alerts?.active || [];
    return alerts.filter((alert) => {
      const severityMatch =
        state.alertSeverity === "all" || alert.severity === state.alertSeverity;
      const sourceMatch = state.alertSource === "all" || alert.source === state.alertSource;
      return severityMatch && sourceMatch;
    });
  }

  function renderGlobal() {
    const status = state.status;
    setText("globalHealthBadge", status.health.status);
    byId("globalHealthBadge")?.setAttribute("class", `pill tone-${toneForStatus(status.health.status)}`);
    setText("globalHealthSummary", status.health.summary);
    setText("globalAlertCount", String(status.alerts.summary.total));
    setText("globalCriticalCount", String(status.alerts.summary.critical));
    setText("globalWarningCount", String(status.alerts.summary.warning));
    setText("globalGeneratedAt", status.generated_at);
    setText("summaryBronzeStatus", status.metrics.pipeline.bronze.status);
    setText("summaryBronzeLag", formatLag(status.metrics.pipeline.bronze.lag_seconds));
    setText("summaryQuarantine", formatNumber(status.metrics.pipeline.silver.quarantine));
    setText("summaryFactCount", formatNumber(status.metrics.pipeline.warehouse.fact_count));
    setText("summaryRunningJobs", formatNumber(status.metrics.jobs.running));

    const cacheStatus = byId("cacheStatus");
    if (cacheStatus) {
      cacheStatus.textContent = status.cache.served_from_cache ? "cached" : "fresh";
      cacheStatus.className = `pill tone-${status.cache.served_from_cache ? "neutral" : "healthy"}`;
    }
  }

  function renderDashboard() {
    const status = state.status;
    const alerts = filteredActiveAlerts();
    setHTML(
      "dashboardAlerts",
      alerts.length
        ? alerts.map(renderAlertCard).join("")
        : '<div class="empty-state">No alerts match the current filters.</div>',
    );

    setHTML(
      "dashboardMetrics",
      [
        ["Bronze Files", status.metrics.pipeline.bronze.count],
        ["Silver Files", status.metrics.pipeline.silver.count],
        ["Staging Rows", status.metrics.pipeline.staging.count],
        ["Fact Rows", status.metrics.pipeline.warehouse.fact_count],
        ["Snapshot Rows", status.metrics.pipeline.warehouse.snapshot_count],
        ["Running Jobs", status.metrics.jobs.running],
      ]
        .map(
          ([label, value]) => `
            <div class="metric-card">
              <span>${escapeHtml(label)}</span>
              <strong>${formatNumber(value)}</strong>
            </div>
          `,
        )
        .join(""),
    );

    setHTML(
      "dashboardCoverage",
      status.coverage
        .map(
          (item) => `
            <div class="item-card">
              <div class="row-spread">
                <strong>${escapeHtml(item.domain)}</strong>
                <span class="pill tone-${item.coverage.toLowerCase().includes("implemented") ? "healthy" : "warning"}">${escapeHtml(item.coverage)}</span>
              </div>
              <p class="muted compact">${escapeHtml(item.details)}</p>
            </div>
          `,
        )
        .join("") +
        `
          <div class="item-card">
            <div class="row-spread">
              <strong>${escapeHtml(status.orchestration.dag_id || "inventory_gold_pipeline")}</strong>
              <span class="pill tone-${status.orchestration.dag_exists ? "healthy" : "warning"}">${status.orchestration.dag_exists ? "defined" : "missing"}</span>
            </div>
            <p class="muted compact">${escapeHtml(status.orchestration.dag_path || "n/a")}</p>
          </div>
        `,
    );
  }

  function renderPipeline() {
    const pipeline = state.status.metrics.pipeline;
    const stages = [
      ["bronze", pipeline.bronze, pipeline.bronze.count],
      ["silver", pipeline.silver, pipeline.silver.count],
      ["staging", pipeline.staging, pipeline.staging.count],
      ["warehouse", pipeline.warehouse, pipeline.warehouse.fact_count],
    ];
    setHTML(
      "pipelineStages",
      stages
        .map(
          ([name, stage, value]) => `
            <article class="stage-card tone-${toneForStatus(stage.status)}">
              <p class="eyebrow">${escapeHtml(name)}</p>
              <strong>${formatNumber(value)}</strong>
              <p class="muted compact">Status ${escapeHtml(stage.status)}</p>
              <p class="muted compact">Last update ${formatValue(stage.last_update)}</p>
              <p class="muted compact">Lag ${formatLag(stage.lag_seconds)}</p>
            </article>
          `,
        )
        .join(""),
    );

    setHTML(
      "pipelineConsistency",
      Object.entries(pipeline.consistency)
        .map(
          ([key, value]) => `
            <div class="kv-row">
              <span>${escapeHtml(key.replaceAll("_", " "))}</span>
              <code>${formatNumber(value)}</code>
            </div>
          `,
        )
        .join(""),
    );

    const pipelineAlerts = (state.status.alerts.active || []).filter((alert) =>
      ["pipeline", "bronze", "silver", "warehouse"].includes(alert.source),
    );
    setHTML(
      "pipelineAlerts",
      pipelineAlerts.length
        ? pipelineAlerts.map(renderAlertCard).join("")
        : '<div class="empty-state">No pipeline-specific alerts are active.</div>',
    );
  }

  function renderWarehouse() {
    const warehouse = state.status.metrics.warehouse;
    setHTML(
      "warehouseOverview",
      `
        <div class="kv-row"><span>Reachable</span><code>${warehouse.reachable ? "yes" : "no"}</code></div>
        <div class="kv-row"><span>Target</span><code>${formatValue(warehouse.target)}</code></div>
        <div class="kv-row"><span>Staging Rows</span><code>${formatNumber(warehouse.staging_count)}</code></div>
        <div class="kv-row"><span>Fact Rows</span><code>${formatNumber(warehouse.fact_count)}</code></div>
        <div class="kv-row"><span>Snapshot Rows</span><code>${formatNumber(warehouse.snapshot_count)}</code></div>
        <div class="kv-row"><span>Schemas</span><code>${listOrFallback(warehouse.schemas)}</code></div>
        <div class="kv-row"><span>Roles</span><code>${listOrFallback(warehouse.roles)}</code></div>
      `,
    );
    setHTML(
      "warehouseSchemaCoverage",
      `
        <div class="kv-row"><span>Missing Tables</span><code>${listOrFallback(warehouse.missing_tables)}</code></div>
        <div class="kv-row"><span>Missing Views</span><code>${listOrFallback(warehouse.missing_views)}</code></div>
        <div class="kv-row"><span>Detected Tables</span><code>${listOrFallback(warehouse.present_tables)}</code></div>
        <div class="kv-row"><span>Detected Views</span><code>${listOrFallback(warehouse.present_views)}</code></div>
      `,
    );
    const issues = warehouse.quality_issues || [];
    setHTML(
      "warehouseQuality",
      issues.length
        ? issues
            .map(
              (issue) => `
                <div class="item-card">
                  <div class="row-spread">
                    <strong>${escapeHtml(issue.issue_name)}</strong>
                    <span class="pill tone-${issue.issue_count ? "warning" : "healthy"}">${formatNumber(issue.issue_count)}</span>
                  </div>
                </div>
              `,
            )
            .join("")
        : '<div class="empty-state">No quality-check output was collected.</div>',
    );
  }

  function renderArtifacts() {
    const artifacts = state.status.metrics.artifacts;
    setHTML(
      "artifactsGrid",
      Object.entries(artifacts)
        .map(
          ([name, artifact]) => `
            <article class="metric-card tall">
              <div class="row-spread">
                <strong>${escapeHtml(name)}</strong>
                <span class="pill tone-${artifact.exists ? "healthy" : "warning"}">${artifact.exists ? "present" : "missing"}</span>
              </div>
              <div class="kv-list">
                <div class="kv-row"><span>Parquet Files</span><code>${formatNumber(artifact.parquet_files)}</code></div>
                <div class="kv-row"><span>Updated</span><code>${formatValue(artifact.updated_at)}</code></div>
                <div class="kv-row"><span>Lag</span><code>${formatLag(artifact.lag_seconds)}</code></div>
                <div class="kv-row"><span>Path</span><code>${formatValue(artifact.path)}</code></div>
              </div>
            </article>
          `,
        )
        .join(""),
    );
  }

  function renderInfrastructure() {
    const infra = state.status.metrics.infra;
    const control = state.status.controls.infra;
    setHTML(
      "infraServices",
      infra.services
        .map(
          (service) => `
            <div class="row-spread item-card">
              <strong>${escapeHtml(service.name)}</strong>
              <span class="pill tone-${service.status === "running" ? "healthy" : "warning"}">${escapeHtml(service.status)}</span>
            </div>
          `,
        )
        .join(""),
    );
    setHTML(
      "infraControl",
      `
        <div class="kv-row"><span>Current Action</span><code>${formatValue(control.current_action)}</code></div>
        <div class="kv-row"><span>Status</span><code>${formatValue(control.status)}</code></div>
        <div class="kv-row"><span>Started</span><code>${formatValue(control.started_at)}</code></div>
        <div class="kv-row"><span>Finished</span><code>${formatValue(control.finished_at)}</code></div>
        <div class="kv-row"><span>PID</span><code>${formatValue(control.pid)}</code></div>
      `,
    );
    setText("infraLogs", (control.logs || []).slice(-12).join("\n"));
  }

  function renderJobs() {
    const jobs = state.status.controls.jobs || [];
    setHTML(
      "jobsGrid",
      jobs
        .map(
          (job) => `
            <article class="job-card">
              <div class="row-spread">
                <div>
                  <strong>${escapeHtml(job.name)}</strong>
                  <p class="muted compact">${escapeHtml(job.stage)} · ${escapeHtml(job.domain)}</p>
                </div>
                <span class="pill tone-${toneForStatus(job.status)}">${escapeHtml(job.status)}</span>
              </div>
              <p>${escapeHtml(job.description)}</p>
              <div class="kv-list">
                <div class="kv-row"><span>Command</span><code>${formatValue(job.command)}</code></div>
                <div class="kv-row"><span>PID</span><code>${formatValue(job.pid)}</code></div>
                <div class="kv-row"><span>Started</span><code>${formatValue(job.started_at)}</code></div>
                <div class="kv-row"><span>Finished</span><code>${formatValue(job.finished_at)}</code></div>
              </div>
              <div class="button-row">
                <button class="button button-primary" type="button" data-job-action="start" data-job-id="${escapeHtml(job.job_id)}">Start</button>
                ${job.long_running ? `<button class="button button-secondary" type="button" data-job-action="stop" data-job-id="${escapeHtml(job.job_id)}">Stop</button>` : ""}
              </div>
              ${job.message ? `<div class="note note-info">${escapeHtml(job.message)}</div>` : ""}
              <div class="log-box">${escapeHtml((job.logs || []).slice(-8).join("\n"))}</div>
            </article>
          `,
        )
        .join(""),
    );
  }

  function renderRunbook() {
    const alerts = state.status.alerts?.active || [];
    setHTML(
      "runbookAlerts",
      alerts.length
        ? alerts.map(renderAlertCard).join("")
        : '<div class="empty-state">No active alerts. Use this page as an operational checklist when conditions change.</div>',
    );
  }

  function render() {
    renderGlobal();
    if (state.page === "dashboard") {
      renderDashboard();
    } else if (state.page === "pipeline") {
      renderPipeline();
    } else if (state.page === "warehouse") {
      renderWarehouse();
    } else if (state.page === "artifacts") {
      renderArtifacts();
    } else if (state.page === "infrastructure") {
      renderInfrastructure();
    } else if (state.page === "jobs") {
      renderJobs();
    } else if (state.page === "runbook") {
      renderRunbook();
    }
  }

  async function refresh(forceRefresh) {
    if (state.refreshInFlight) {
      return;
    }
    state.refreshInFlight = true;
    clearFeedback();
    try {
      const response = await fetch(
        forceRefresh ? "/api/status/full?refresh=hard" : "/api/status/full",
      );
      if (!response.ok) {
        throw new Error(`Status request failed with ${response.status}`);
      }
      state.status = await response.json();
      render();
    } catch (error) {
      showFeedback(`Refresh failed: ${error.message}`, "critical");
    } finally {
      state.refreshInFlight = false;
    }
  }

  async function postAction(url) {
    clearFeedback();
    try {
      const response = await fetch(url, { method: "POST" });
      const payload = await response.json();
      if (!response.ok || payload.ok === false) {
        throw new Error(payload.message || `Request failed with ${response.status}`);
      }
      showFeedback(payload.message || "Action completed.", "healthy");
      await refresh(true);
    } catch (error) {
      showFeedback(error.message, "critical");
    }
  }

  document.addEventListener("click", (event) => {
    const hardRefreshButton = event.target.closest("#hardRefreshButton");
    if (hardRefreshButton) {
      refresh(true);
      return;
    }

    const jobButton = event.target.closest("[data-job-action]");
    if (jobButton) {
      const jobId = jobButton.dataset.jobId;
      const action = jobButton.dataset.jobAction;
      postAction(`/api/jobs/${encodeURIComponent(jobId)}/${encodeURIComponent(action)}`);
      return;
    }

    const infraButton = event.target.closest("[data-infra-action]");
    if (infraButton) {
      const action = infraButton.dataset.infraAction;
      postAction(`/api/infra/${encodeURIComponent(action)}`);
    }
  });

  byId("alertSeverityFilter")?.addEventListener("change", (event) => {
    state.alertSeverity = event.target.value;
    renderDashboard();
  });

  byId("alertSourceFilter")?.addEventListener("change", (event) => {
    state.alertSource = event.target.value;
    renderDashboard();
  });

  window.setInterval(() => {
    refresh(false);
  }, 30000);

  render();
})();

