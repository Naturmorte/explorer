"use strict";

/* LA Parcel Sync UI -- vanilla JS, no build step. ------------------------ */

const FIELD_LABELS = {
  ain: "AIN",
  assessor_id: "Assessor ID",
  property_location: "Address",
  city: "City",
  zip_code: "ZIP",
  latitude: "Latitude",
  longitude: "Longitude",
  property_use_type: "Use type",
  property_use_code: "Use code",
  year_built: "Year built",
  effective_year: "Effective year",
  square_footage: "Square footage",
  number_of_buildings: "Buildings",
  bedrooms: "Bedrooms",
  bathrooms: "Bathrooms",
  units: "Units",
  recording_date: "Recording date",
  land_value: "Land value",
  improvement_value: "Improvement value",
  total_value: "Total value",
  taxable_value: "Taxable value",
  classification: "Classification",
  legal_description: "Legal description",
};

const MONEY_FIELDS = new Set(["land_value", "improvement_value", "total_value", "taxable_value"]);

const state = {
  review: { status: "pending", event: "all", q: "", limit: 25, offset: 0, total: 0, selected: new Set() },
  reviewed: { status: "accepted", limit: 25, offset: 0, total: 0 },
  jobPollTimer: null,
  refreshTimer: null,
};

/* ---- tiny helpers -------------------------------------------------- */

function $(sel, root) { return (root || document).querySelector(sel); }
function $all(sel, root) { return Array.from((root || document).querySelectorAll(sel)); }

function escapeHtml(s) {
  if (s === null || s === undefined) return "";
  return String(s).replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
}

function fmtMoney(v) {
  if (v === null || v === undefined || v === "") return "—";
  const n = Number(v);
  if (Number.isNaN(n)) return escapeHtml(v);
  return "$" + n.toLocaleString("en-US", { maximumFractionDigits: 0 });
}

function fmtNum(v) {
  if (v === null || v === undefined || v === "") return "—";
  return String(v);
}

function fmtDate(iso) {
  if (!iso) return "—";
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return escapeHtml(iso);
  return d.toLocaleString(undefined, { year: "numeric", month: "short", day: "2-digit", hour: "2-digit", minute: "2-digit" });
}

function fieldLabel(key) { return FIELD_LABELS[key] || key; }

function fieldValue(key, v) {
  if (v === null || v === undefined || v === "") return "—";
  if (MONEY_FIELDS.has(key)) return fmtMoney(v);
  if (key === "recording_date") return fmtDate(v);
  return escapeHtml(v);
}

async function api(path, opts) {
  const res = await fetch(path, opts);
  let data = {};
  try { data = await res.json(); } catch (_) { /* no body */ }
  if (!res.ok) throw new Error(data.error || `HTTP ${res.status}`);
  return data;
}

function toast(message) {
  const el = $("#toast");
  el.textContent = message;
  el.hidden = false;
  clearTimeout(toast._t);
  toast._t = setTimeout(() => { el.hidden = true; }, 2600);
}

function reviewerName() {
  return ($("#reviewer-name").value || "").trim();
}

/* ---- tabs ------------------------------------------------------------ */

function activateTab(name) {
  $all(".tab").forEach((t) => t.classList.toggle("active", t.dataset.tab === name));
  $all(".tab-panel").forEach((p) => p.classList.toggle("active", p.id === `tab-${name}`));
  if (name === "overview") { loadOverview(); loadRecentRuns(); }
  if (name === "review") loadReviewQueue();
  if (name === "reviewed") loadReviewed();
  if (name === "runs") loadRuns();
  if (name === "errors") loadErrorsRunList();
}

/* ---- overview ---------------------------------------------------------- */

function statCard(label, value, cls) {
  return `<div class="stat-card ${cls || ""}"><div class="stat-value">${value}</div><div class="stat-label">${label}</div></div>`;
}

async function loadOverview() {
  const data = await api("/api/overview");
  const grid = $("#stat-grid");
  grid.innerHTML = [
    statCard("Total parcels", data.total_snapshots.toLocaleString(), "accent"),
    statCard("Active", data.active.toLocaleString(), "ok"),
    statCard("Inactive", data.inactive.toLocaleString(), "muted"),
    statCard("Pending review", data.pending_review.toLocaleString(), "warn"),
    statCard("Accepted", data.accepted.toLocaleString(), "ok"),
    statCard("Rejected", data.rejected.toLocaleString(), ""),
  ].join("");

  const badge = $("#pending-badge");
  if (data.pending_review > 0) {
    badge.hidden = false;
    badge.textContent = data.pending_review > 999 ? "999+" : data.pending_review;
  } else {
    badge.hidden = true;
  }
}

function runStatusPill(status) {
  const cls = status === "completed" ? "pill-success" : status === "failed" ? "pill-danger" : "pill-accent";
  return `<span class="pill ${cls}">${escapeHtml(status)}</span>`;
}

async function loadRecentRuns() {
  const data = await api("/api/runs?limit=8");
  const tbody = $("#recent-runs-table tbody");
  if (!data.runs.length) {
    tbody.innerHTML = `<tr class="empty-row"><td colspan="7">No sync runs yet. Start one above.</td></tr>`;
    return;
  }
  tbody.innerHTML = data.runs
    .map(
      (r) => `<tr>
        <td class="mono">${escapeHtml(r.run_id)}</td>
        <td>${fmtDate(r.started_at)}</td>
        <td>${runStatusPill(r.status)}</td>
        <td>${fmtNum(r.fetched_count)}</td>
        <td>${fmtNum(r.inserted_count)}</td>
        <td>${fmtNum(r.updated_count)}</td>
        <td>${r.failed_count ? `<span class="pill pill-danger">${r.failed_count}</span>` : "0"}</td>
      </tr>`
    )
    .join("");
}

/* ---- sync launcher ------------------------------------------------------ */

function renderJob(job) {
  const badge = $("#job-status-badge");
  const log = $("#job-log");
  const hint = $("#job-hint");
  const startBtn = $("#start-btn");

  if (!job) {
    badge.hidden = true;
    log.hidden = true;
    hint.hidden = true;
    startBtn.disabled = false;
    return;
  }

  badge.hidden = false;
  if (job.running) {
    badge.className = "pill pill-accent";
    badge.textContent = "running";
    startBtn.disabled = true;
  } else {
    badge.className = "pill " + (job.returncode === 0 ? "pill-success" : "pill-danger");
    badge.textContent = job.returncode === 0 ? "completed" : `failed (code ${job.returncode})`;
    startBtn.disabled = false;
  }

  hint.hidden = false;
  hint.textContent = `Job ${job.job_id} · ${job.args.join(" ")}`;

  log.hidden = false;
  log.textContent = job.log.join("\n");
  log.scrollTop = log.scrollHeight;
}

async function pollJobStatus(once) {
  try {
    const data = await api("/api/sync/status");
    renderJob(data.job);
    if (data.job && data.job.running) {
      clearTimeout(state.jobPollTimer);
      if (!once) state.jobPollTimer = setTimeout(() => pollJobStatus(false), 1200);
    } else if (data.job && !data.job.running) {
      loadOverview();
      loadRecentRuns();
    }
  } catch (err) {
    // server not reachable yet / transient -- stay quiet, next tick retries
  }
}

async function startSync(ev) {
  ev.preventDefault();
  const payload = {
    max_records: $("#f-max-records").value.trim(),
    page_size: Number($("#f-page-size").value || 1000),
    where: $("#f-where").value.trim() || "1=1",
    request_delay: $("#f-request-delay").value,
    resume: $("#f-resume").checked,
    fresh_run: $("#f-fresh-run").checked,
  };
  try {
    $("#start-btn").disabled = true;
    const data = await api("/api/sync/start", { method: "POST", body: JSON.stringify(payload) });
    toast("Sync started");
    renderJob(data);
    pollJobStatus(false);
  } catch (err) {
    toast(`Could not start sync: ${err.message}`);
    $("#start-btn").disabled = false;
  }
}

/* ---- review queue --------------------------------------------------- */

function eventPill(evt) {
  if (evt === "inserted") return `<span class="pill pill-accent">inserted</span>`;
  if (evt === "updated") return `<span class="pill pill-warning">updated</span>`;
  return `<span class="pill pill-neutral">${escapeHtml(evt || "—")}</span>`;
}

function renderPager(el, s, onGo) {
  const from = s.total === 0 ? 0 : s.offset + 1;
  const to = Math.min(s.offset + s.limit, s.total);
  el.innerHTML = `
    <span>${from}–${to} of ${s.total}</span>
    <button class="btn btn-ghost btn-small" id="${el.id}-prev" ${s.offset === 0 ? "disabled" : ""}>Prev</button>
    <button class="btn btn-ghost btn-small" id="${el.id}-next" ${s.offset + s.limit >= s.total ? "disabled" : ""}>Next</button>
  `;
  $(`#${el.id}-prev`).addEventListener("click", () => onGo(Math.max(0, s.offset - s.limit)));
  $(`#${el.id}-next`).addEventListener("click", () => onGo(s.offset + s.limit));
}

function updateBulkBar() {
  const n = state.review.selected.size;
  const bar = $("#bulk-actions");
  bar.hidden = n === 0;
  $("#bulk-count").textContent = `${n} selected`;
}

async function loadReviewQueue() {
  const s = state.review;
  const params = new URLSearchParams({
    status: s.status, event: s.event, q: s.q, limit: s.limit, offset: s.offset,
  });
  const data = await api(`/api/changes?${params}`);
  s.total = data.total;
  const tbody = $("#review-tbody");

  if (!data.items.length) {
    tbody.innerHTML = `<tr class="empty-row"><td colspan="9">Nothing pending review. New inserted/updated parcels from a sync will show up here.</td></tr>`;
  } else {
    tbody.innerHTML = data.items
      .map((item) => {
        const n = item.normalized || {};
        return `<tr data-id="${escapeHtml(item.snapshot_id)}">
          <td><input type="checkbox" class="row-check" ${state.review.selected.has(item.snapshot_id) ? "checked" : ""}/></td>
          <td class="mono">${escapeHtml(item.property_id)}</td>
          <td>${fmtNum(item.roll_year)}</td>
          <td>${eventPill(item.event_type)}</td>
          <td class="wrap">${escapeHtml(n.property_location) || "—"}</td>
          <td>${escapeHtml(n.city) || "—"}</td>
          <td>${fmtMoney(n.total_value)}</td>
          <td class="faint">${fmtDate(item.last_seen_at)}</td>
          <td class="col-actions">
            <button class="btn btn-ghost btn-small view-btn">View</button>
            <button class="btn btn-accept btn-small accept-btn">Accept</button>
            <button class="btn btn-reject btn-small reject-btn">Reject</button>
          </td>
        </tr>`;
      })
      .join("");
  }

  $all(".row-check", tbody).forEach((cb) => {
    cb.addEventListener("change", () => {
      const id = cb.closest("tr").dataset.id;
      if (cb.checked) state.review.selected.add(id); else state.review.selected.delete(id);
      updateBulkBar();
    });
  });
  $all(".view-btn", tbody).forEach((btn) => btn.addEventListener("click", () => openSnapshotModal(btn.closest("tr").dataset.id)));
  $all(".accept-btn", tbody).forEach((btn) => btn.addEventListener("click", () => submitReview([btn.closest("tr").dataset.id], "accepted")));
  $all(".reject-btn", tbody).forEach((btn) => btn.addEventListener("click", () => submitReview([btn.closest("tr").dataset.id], "rejected")));

  $("#review-select-all").checked = false;
  renderPager($("#review-pager"), s, (offset) => { s.offset = offset; loadReviewQueue(); });
  updateBulkBar();
}

async function submitReview(snapshotIds, decision, note) {
  if (!snapshotIds.length) return;
  try {
    await api("/api/review", {
      method: "POST",
      body: JSON.stringify({ snapshot_ids: snapshotIds, decision, reviewer: reviewerName(), note: note || "" }),
    });
    toast(`${snapshotIds.length} ${decision}`);
    snapshotIds.forEach((id) => state.review.selected.delete(id));
    loadReviewQueue();
    loadOverview();
  } catch (err) {
    toast(`Failed: ${err.message}`);
  }
}

/* ---- reviewed ---------------------------------------------------------- */

async function loadReviewed() {
  const s = state.reviewed;
  const params = new URLSearchParams({ status: s.status, limit: s.limit, offset: s.offset });
  const data = await api(`/api/changes?${params}`);
  s.total = data.total;
  const tbody = $("#reviewed-tbody");

  if (!data.items.length) {
    tbody.innerHTML = `<tr class="empty-row"><td colspan="7">Nothing here yet.</td></tr>`;
  } else {
    tbody.innerHTML = data.items
      .map(
        (item) => `<tr data-id="${escapeHtml(item.snapshot_id)}">
          <td class="mono">${escapeHtml(item.property_id)}</td>
          <td>${fmtNum(item.roll_year)}</td>
          <td>${eventPill(item.event_type)}</td>
          <td>${escapeHtml(item.review.reviewer) || "—"}</td>
          <td class="faint">${fmtDate(item.review.reviewed_at)}</td>
          <td class="wrap">${escapeHtml(item.review.note) || "—"}</td>
          <td class="col-actions">
            <button class="btn btn-ghost btn-small view-btn">View</button>
            <button class="btn btn-ghost btn-small reset-btn">Reset</button>
          </td>
        </tr>`
      )
      .join("");
  }

  $all(".view-btn", tbody).forEach((btn) => btn.addEventListener("click", () => openSnapshotModal(btn.closest("tr").dataset.id)));
  $all(".reset-btn", tbody).forEach((btn) =>
    btn.addEventListener("click", async () => {
      const id = btn.closest("tr").dataset.id;
      await api("/api/review/reset", { method: "POST", body: JSON.stringify({ snapshot_ids: [id] }) });
      toast("Reset to pending");
      loadReviewed();
      loadOverview();
    })
  );

  renderPager($("#reviewed-pager"), s, (offset) => { s.offset = offset; loadReviewed(); });
}

/* ---- runs tab ------------------------------------------------------------*/

async function loadRuns() {
  const data = await api("/api/runs?limit=200");
  const tbody = $("#runs-tbody");
  if (!data.runs.length) {
    tbody.innerHTML = `<tr class="empty-row"><td colspan="13">No sync runs yet.</td></tr>`;
    return;
  }
  tbody.innerHTML = data.runs
    .map(
      (r) => `<tr>
        <td class="mono">${escapeHtml(r.run_id)}</td>
        <td>${fmtDate(r.started_at)}</td>
        <td>${fmtDate(r.finished_at)}</td>
        <td>${runStatusPill(r.status)}</td>
        <td class="mono wrap">${escapeHtml(r.query_where)}</td>
        <td>${r.max_records === null ? "all" : fmtNum(r.max_records)}</td>
        <td>${fmtNum(r.fetched_count)}</td>
        <td>${fmtNum(r.inserted_count)}</td>
        <td>${fmtNum(r.updated_count)}</td>
        <td>${fmtNum(r.unchanged_count)}</td>
        <td>${r.failed_count ? `<span class="pill pill-danger">${r.failed_count}</span>` : "0"}</td>
        <td>${fmtNum(r.inactivated_count)}</td>
        <td>${r.has_errors ? `<button class="btn btn-ghost btn-small errors-link" data-run="${escapeHtml(r.run_id)}">Errors</button>` : ""}</td>
      </tr>`
    )
    .join("");

  $all(".errors-link", tbody).forEach((btn) =>
    btn.addEventListener("click", () => {
      activateTab("errors");
      setTimeout(() => {
        $("#errors-run-select").value = btn.dataset.run;
        loadErrors(btn.dataset.run);
      }, 0);
    })
  );
}

/* ---- errors tab ------------------------------------------------------- */

async function loadErrorsRunList() {
  const data = await api("/api/runs?limit=200");
  const select = $("#errors-run-select");
  const runsWithErrors = data.runs.filter((r) => r.has_errors);
  const current = select.value;
  select.innerHTML =
    `<option value="">Select a run with errors&hellip;</option>` +
    runsWithErrors
      .map((r) => `<option value="${escapeHtml(r.run_id)}">${escapeHtml(r.run_id)} · ${fmtDate(r.started_at)} · ${r.failed_count} failed</option>`)
      .join("");
  if (runsWithErrors.some((r) => r.run_id === current)) select.value = current;
  $("#errors-tbody").innerHTML = `<tr class="empty-row"><td colspan="4">Pick a run above to see its errors.</td></tr>`;
}

async function loadErrors(runId) {
  const tbody = $("#errors-tbody");
  if (!runId) {
    tbody.innerHTML = `<tr class="empty-row"><td colspan="4">Pick a run above to see its errors.</td></tr>`;
    return;
  }
  const data = await api(`/api/errors?run_id=${encodeURIComponent(runId)}`);
  if (!data.items.length) {
    tbody.innerHTML = `<tr class="empty-row"><td colspan="4">No parsable error records found.</td></tr>`;
    return;
  }
  tbody.innerHTML = data.items
    .map(
      (e, i) => `<tr>
        <td class="mono">${escapeHtml(e.source_object_id)}</td>
        <td class="wrap">${escapeHtml(e.error)}</td>
        <td class="faint">${fmtDate(e.recorded_at)}</td>
        <td class="col-actions"><button class="btn btn-ghost btn-small raw-btn" data-idx="${i}">Raw</button></td>
      </tr>`
    )
    .join("");
  $all(".raw-btn", tbody).forEach((btn) =>
    btn.addEventListener("click", () => {
      const rec = data.items[Number(btn.dataset.idx)];
      openRawModal("Error record", rec);
    })
  );
}

/* ---- modal: snapshot detail + diff + review --------------------------- */

function diffNormalized(prev, curr) {
  const a = prev || {};
  const b = curr || {};
  const keys = new Set([...Object.keys(a), ...Object.keys(b)]);
  const changes = [];
  for (const k of keys) {
    if (JSON.stringify(a[k]) !== JSON.stringify(b[k])) changes.push({ field: k, from: a[k], to: b[k] });
  }
  return changes;
}

function closeModal() {
  $("#detail-modal").hidden = true;
  $("#modal-body").innerHTML = "";
}

function openRawModal(title, obj) {
  $("#modal-title").textContent = title;
  $("#modal-body").innerHTML = `<pre class="mono" style="white-space:pre-wrap;overflow:auto;max-height:60vh;">${escapeHtml(JSON.stringify(obj, null, 2))}</pre>`;
  $("#detail-modal").hidden = false;
}

async function openSnapshotModal(snapshotId) {
  const data = await api(`/api/snapshot?id=${encodeURIComponent(snapshotId)}`);
  const current = data.current;
  const previous = data.previous;
  const n = current.normalized || {};
  const review = data.review;
  const effectiveStatus = review ? review.effective_status : "pending";

  $("#modal-title").innerHTML = `${escapeHtml(current.property_id)} ${eventPill(current.event_type)}`;

  const factRows = Object.keys(FIELD_LABELS)
    .map((k) => `<div class="detail-row"><span class="k">${fieldLabel(k)}</span><span class="v">${fieldValue(k, n[k])}</span></div>`)
    .join("");

  let diffHtml = "";
  if (previous) {
    const changes = diffNormalized(previous.normalized, current.normalized);
    diffHtml = `
      <div>
        <div class="section-title">Changes since previous version</div>
        ${
          changes.length
            ? `<table class="diff-table"><thead><tr><th>Field</th><th>From</th><th>To</th></tr></thead><tbody>
                ${changes
                  .map(
                    (c) => `<tr><td>${fieldLabel(c.field)}</td>
                      <td class="diff-old">${fieldValue(c.field, c.from)}</td>
                      <td class="diff-new">${fieldValue(c.field, c.to)}</td></tr>`
                  )
                  .join("")}
              </tbody></table>`
            : `<p class="muted">No normalized-field differences (only raw/technical attributes changed).</p>`
        }
      </div>`;
  }

  const statusPillCls = { pending: "pill-warning", accepted: "pill-success", rejected: "pill-danger" }[effectiveStatus] || "pill-neutral";

  $("#modal-body").innerHTML = `
    <div class="detail-grid">${factRows}</div>
    ${diffHtml}
    <div class="review-panel">
      <div class="section-title">Review</div>
      <div>
        <span class="pill ${statusPillCls}">${escapeHtml(effectiveStatus)}</span>
        ${review && review.reviewed_at ? `<span class="muted"> by ${escapeHtml(review.reviewer || "?")} on ${fmtDate(review.reviewed_at)}</span>` : ""}
        ${review && review.note ? `<div class="muted" style="margin-top:6px;">${escapeHtml(review.note)}</div>` : ""}
      </div>
      <textarea id="modal-note" placeholder="Note (optional)"></textarea>
      <div class="actions">
        <button class="btn btn-accept" id="modal-accept">Accept</button>
        <button class="btn btn-reject" id="modal-reject">Reject</button>
        ${review && review.reviewed_at ? `<button class="btn btn-ghost" id="modal-reset">Reset to pending</button>` : ""}
      </div>
    </div>
    <details class="raw-json">
      <summary>Raw record JSON</summary>
      <pre>${escapeHtml(JSON.stringify(current, null, 2))}</pre>
    </details>
  `;

  $("#modal-accept").addEventListener("click", async () => {
    await submitReview([snapshotId], "accepted", $("#modal-note").value.trim());
    closeModal();
    loadReviewed();
  });
  $("#modal-reject").addEventListener("click", async () => {
    await submitReview([snapshotId], "rejected", $("#modal-note").value.trim());
    closeModal();
    loadReviewed();
  });
  const resetBtn = $("#modal-reset");
  if (resetBtn) {
    resetBtn.addEventListener("click", async () => {
      await api("/api/review/reset", { method: "POST", body: JSON.stringify({ snapshot_ids: [snapshotId] }) });
      toast("Reset to pending");
      closeModal();
      loadReviewQueue();
      loadReviewed();
      loadOverview();
    });
  }

  $("#detail-modal").hidden = false;
}

/* ---- wiring ------------------------------------------------------------ */

function init() {
  $("#reviewer-name").value = localStorage.getItem("parcelSyncReviewer") || "";
  $("#reviewer-name").addEventListener("input", (e) => localStorage.setItem("parcelSyncReviewer", e.target.value));

  $all(".tab").forEach((t) => t.addEventListener("click", () => activateTab(t.dataset.tab)));

  $("#sync-form").addEventListener("submit", startSync);
  $("#export-btn").addEventListener("click", () => { window.location.href = "/api/export"; });

  let searchDebounce;
  $("#review-search").addEventListener("input", (e) => {
    clearTimeout(searchDebounce);
    searchDebounce = setTimeout(() => {
      state.review.q = e.target.value.trim();
      state.review.offset = 0;
      loadReviewQueue();
    }, 300);
  });
  $("#review-event-filter").addEventListener("change", (e) => {
    state.review.event = e.target.value;
    state.review.offset = 0;
    loadReviewQueue();
  });
  $("#review-select-all").addEventListener("change", (e) => {
    $all("#review-tbody tr").forEach((tr) => {
      const cb = $(".row-check", tr);
      if (!cb) return;
      cb.checked = e.target.checked;
      if (e.target.checked) state.review.selected.add(tr.dataset.id);
      else state.review.selected.delete(tr.dataset.id);
    });
    updateBulkBar();
  });
  $("#bulk-accept").addEventListener("click", () => submitReview([...state.review.selected], "accepted", $("#bulk-note").value.trim()));
  $("#bulk-reject").addEventListener("click", () => submitReview([...state.review.selected], "rejected", $("#bulk-note").value.trim()));

  $("#reviewed-status-filter").addEventListener("change", (e) => {
    state.reviewed.status = e.target.value;
    state.reviewed.offset = 0;
    loadReviewed();
  });

  $("#errors-run-select").addEventListener("change", (e) => loadErrors(e.target.value));

  $("#modal-close").addEventListener("click", closeModal);
  $("#modal-backdrop").addEventListener("click", closeModal);
  document.addEventListener("keydown", (e) => { if (e.key === "Escape") closeModal(); });

  activateTab("overview");
  pollJobStatus(false);

  state.refreshTimer = setInterval(() => {
    loadOverview().catch(() => {});
  }, 20000);
}

document.addEventListener("DOMContentLoaded", init);
