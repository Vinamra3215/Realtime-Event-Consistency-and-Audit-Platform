"use strict";
const selectedRun = new URLSearchParams(location.search).get("run") || "main";
const $ = (id) => document.getElementById(id);
const pretty = (v) => String(v ?? "—").replaceAll("_", " ");
const number = (v, digits = 0) => v == null ? "—" : Number(v).toLocaleString(undefined, {maximumFractionDigits: digits});
const seconds = (v) => v == null ? "—" : `${number(v, 3)} s`;
const percent = (v) => v == null ? "—" : `${number(v * 100, 1)}%`;
function element(tag, text, cls) {
  const e = document.createElement(tag);
  if (text != null) e.textContent = text;
  if (cls) e.className = cls;
  return e;
}
async function api(path) {
  const url = new URL(path, location.origin);
  url.searchParams.set("run", selectedRun);
  const response = await fetch(url);
  const data = await response.json();
  if (!response.ok) throw new Error(data.error || "Could not load saved results");
  return data;
}
function showError(error) { $("error").textContent = error.message; $("error").hidden = false; }
function badge(value) { return element("span", value ?? "Unavailable", `badge ${value || ""}`); }
function details(id, pairs) {
  for (const [label, value] of pairs) {
    const row = element("div");
    row.append(element("dt", label), element("dd", value ?? "—"));
    $(id).append(row);
  }
}
function bars(id, values) {
  const total = Object.values(values || {}).reduce((a,b) => a+b, 0);
  for (const [key, count] of Object.entries(values || {})) {
    const row = element("div", null, "bar-row"), label = element("div", null, "bar-label");
    label.append(element("span", pretty(key)), element("strong", number(count)));
    const track = element("div", null, "track"), fill = element("div", null, "fill");
    fill.style.width = `${total ? 100 * count / total : 0}%`;
    track.append(fill); row.append(label, track); $(id).append(row);
  }
  if (!total) $(id).append(element("p", "No observations in this run.", "muted"));
}
let report, kind = "transactions", page = 1, requestVersion = 0;
function renderSummary(data) {
  report = data;
  for (const run of data.available_runs || []) {
    const option = element("option", `${run.name} · ${run.rows} input rows`);
    option.value = run.key; option.selected = run.key === data.selected_run;
    $("run-picker").append(option);
  }
  $("source-kind").textContent = data.manifest.input_catalog?.source || "Historical input; no source catalog attached.";

  const m = data.metrics, manifest = data.manifest, config = manifest.config;
  document.title = `RECAP · ${manifest.dataset.name}`;
  $("subtitle").textContent = `${manifest.dataset.name} · ${number(m.source_rows_read)} input rows · ${new Date(manifest.prepared_at).toLocaleString()}`;
  $("status").textContent = pretty(m.run_status);
  $("execution").textContent = `${pretty(m.execution?.transport)} / ${pretty(m.execution?.database)}`;
  $("policy").textContent = `Policy: ${pretty(m.policy)}`;
  $("run-id").textContent = `Run ${m.run_id}`;
  const cards = [
    ["Successful transactions", number(m.state_counts.SUCCESS || 0), `${number(m.logical_transactions)} logical transactions`],
    ["Recovered successfully", number(m.recovered_successfully), `${number(m.ever_unresolved)} ever uncertain · ${percent(m.recovery_success_rate)} recovered`],
    ["Duplicate payment effects", number(m.duplicate_provider_effects), "Extra provider effects for the same key"],
    ["Reconciliation agreement", percent(m.reconciliation_match_rate), `${number(m.reconciliation_counts.match || 0)} outcomes match the provider`],
  ];
  for (const [label, value, hint] of cards) {
    const card = element("article", null, "card");
    card.append(element("div", label, "card-label"), element("div", value, "card-value"), element("div", hint, "card-detail"));
    $("cards").append(card);
  }
  for (const [label, value] of [["Rows read", m.source_rows_read], ["Accepted", m.source_rows_accepted], ["Rejected", m.source_rows_rejected], ["Succeeded", m.state_counts.SUCCESS || 0]]) {
    const item = element("div", null, "flow-item"); item.append(element("strong", number(value)), element("span", label)); $("flow").append(item);
  }
  $("flow-note").textContent = `${number(m.processed_unique_wire_events)} of ${number(m.expected_wire_events)} expected wire events processed. ${number(m.wire_events_unaccounted)} unaccounted; ${number(m.wire_events_rejected)} rejected at consumption.`;
  for (const [label, value, warning] of [
    ["Still unresolved", m.unresolved_transactions, m.unresolved_transactions > 0],
    ["Provider submissions / status checks", `${m.provider_submit_calls} / ${m.provider_status_checks}`, false],
    ["Recovery attempts", m.recovery_attempts, false],
    ["Source balance inconsistencies", m.balance_inconsistent_rows, m.balance_inconsistent_rows > 0],
    ["Audit entries", m.audit_records, false],
  ]) {
    const row = element("div", null, "health-row"); row.append(element("span", label), element("strong", value, warning ? "warn" : "good")); $("health").append(row);
  }
  bars("scenarios", m.observed_scenarios); bars("states", m.state_counts);
  details("source", [["File", manifest.dataset.name], ["Starting row", manifest.slice.start_row], ["Requested row limit", manifest.slice.limit], ["Rows read", m.source_rows_read], ["Input SHA-256", manifest.dataset.sha256], ["Prepared", new Date(manifest.prepared_at).toLocaleString()], ["Parent dataset", manifest.input_catalog?.parent_dataset?.file || "Not recorded"], ["Parent SHA-256", manifest.input_catalog?.parent_dataset?.sha256 || "Not recorded"]]);
  details("config", [["Policy", pretty(config.policy)], ["Seed", config.seed], ["Maximum attempts", config.max_attempts], ["Base / max delay", `${config.base_delay} s / ${config.max_delay} s`], ["Lease duration", `${config.lease_seconds} s`], ["Input rate", config.input_rate ? `${config.input_rate} events/s` : "Unthrottled"], ["Currency", config.currency || "Unspecified"], ["Failure mix", Object.entries(config.failure_mix || {}).map(([k,v]) => `${pretty(k)}: ${percent(v)}`).join("; ")]]);
  for (const [label, value] of [["Measured run duration", seconds(m.run_elapsed_seconds)], ["Accepted events / second", number(m.accepted_events_per_second, 2)], ["Recovery success", percent(m.recovery_success_rate)]]) {
    const item = element("div"); item.append(element("strong", value), element("span", label)); $("timing-summary").append(item);
  }
  for (const [key, label] of [["resolution_latency_seconds", "Resolution latency"], ["pipeline_latency_seconds", "Pipeline latency"], ["recovery_time_seconds", "Recovery time"], ["unresolved_dwell_seconds", "Time spent unresolved"]]) {
    const values = m[key] || {}, row = element("tr");
    for (const value of [label, number(values.count), ...["mean", "p50", "p95", "p99"].map(k => seconds(values[k]))]) row.append(element("td", value));
    $("latencies").append(row);
  }
  $("output-path").textContent = data.output_path;
  for (const name of data.artifacts) {
    const link = element("a", null, "download"); link.href = `/download?file=${encodeURIComponent(name)}&run=${encodeURIComponent(selectedRun)}`;
    link.append(element("span", name), element("span", "↓")); $("downloads").append(link);
  }
  $("raw").textContent = JSON.stringify({metrics:m, provenance:data.provenance}, null, 2);
  $("reported").textContent = `Reported ${new Date(data.provenance.reported_at).toLocaleString()}`;
}
const columns = {
  cases: [["Source row",r=>r.row],["Edge case",r=>pretty(r.case)],["Expected validation",r=>r.expected_validation],["Detection / rule",r=>r.explanation],["Actual validation",r=>r.actual_validation],["Outcome",r=>badge(r.status)],["Actual handling",r=>r.handling]],
  transactions: [["Row", r=>r.row], ["Input case",r=>pretty(r.case)], ["Type", r=>r.type], ["From → To", r=>`${r.originator} → ${r.destination}`], ["Amount", r=>`${number(r.amount,2)}${r.currency ? ` ${r.currency}` : ""}`], ["Injected scenario", r=>pretty(r.scenario)], ["Scenario meaning", r=>r.scenario_explanation], ["Actual handling",r=>r.handling], ["Outcome", r=>badge(r.status)], ["Recovered", r=>r.recovered == null ? "Not recorded" : r.recovered ? "Yes" : "No"], ["Comparison", r=>badge(r.reconciliation)], ["Source findings", r=>r.quality.map(pretty).join(", ") || "None"]],
  rejected: [["Source row", r=>r.row], ["Edge case",r=>pretty(r.case)], ["Type", r=>r.raw.type], ["Raw amount", r=>r.raw.amount], ["Rejection reason", r=>r.reason]],
  reconciliation: [["Transaction ID", r=>r.transaction_id], ["Internal outcome", r=>badge(r.internal_outcome)], ["Provider outcome", r=>badge(r.reference_outcome)], ["Comparison", r=>badge(r.classification)], ["Source findings", r=>(r.details.data_quality || []).map(pretty).join(", ") || "None"]],
  audit: [["Time", r=>new Date(r.timestamp*1000).toLocaleTimeString()], ["Transaction ID", r=>r.transaction_id], ["Event", r=>pretty(r.event_type)], ["Previous state", r=>r.previous_state || "—"], ["Next state", r=>badge(r.next_state)], ["Attempt", r=>r.attempt_number]],
};
async function loadTable() {
  const version = ++requestVersion;
  const params = new URLSearchParams({kind, page, q:$("search").value, status:kind === "transactions" ? $("filter").value : ""});
  $("count").textContent = "Loading…";
  try {
    const data = await api(`/api/rows?${params}`);
    if (version !== requestVersion) return;
    $("table-head").replaceChildren(); $("table-body").replaceChildren();
    const header = element("tr");
    for (const [title] of columns[kind]) header.append(element("th", title));
    header.append(element("th", "Details")); $("table-head").append(header);
    $("status-label").hidden = kind !== "transactions";
    $("table-note").textContent = kind === "transactions" && !report.detailed_transactions
      ? "Historical report: input and reconciliation are available. Per-transaction attempts and recovery flags were not exported in this version. New runs include them."
      : kind === "audit" && !report.audit_available
        ? "This historical report did not export individual audit entries. New runs include the full history."
        : kind === "transactions" ? "Normalized input linked to processing outcomes. Source-quality findings are separate from payment outcomes."
        : kind === "rejected" ? "These rows were rejected before publication. View details for their original CSV fields."
        : kind === "cases" ? "Added input cases only. Expected validation is documentation; actual handling comes from rejection, transaction, reconciliation and audit records. Switch to the separate edge-case run to inspect its 17 cases."
        : "Saved evidence for this run. Search a transaction ID to follow one transaction.";
    for (const record of data.items) {
      const row = element("tr");
      for (const [,get] of columns[kind]) {
        const cell = element("td"), value = get(record);
        if (value instanceof Node) cell.append(value); else cell.textContent = value ?? "—";
        row.append(cell);
      }
      const cell = element("td"), button = element("button", "View ↗", "record-button");
      button.addEventListener("click", () => { $("handling-summary").textContent = [record.scenario_explanation, record.handling || record.reason].filter(Boolean).join(" ") || "See saved evidence below."; $("detail-json").textContent = JSON.stringify(record,null,2); $("detail").showModal(); });
      cell.append(button); row.append(cell); $("table-body").append(row);
    }
    if (!data.items.length) { const row = element("tr"), cell = element("td", "No records to display for this selection."); cell.colSpan = columns[kind].length + 1; row.append(cell); $("table-body").append(row); }
    $("count").textContent = `${number(data.total)} records · Page ${page} of ${Math.max(1,Math.ceil(data.total/data.size))}`;
    $("prev").disabled = page <= 1; $("next").disabled = page * data.size >= data.total;
  } catch (error) { if (version === requestVersion) showError(error); }
}
document.querySelectorAll("[data-kind]").forEach(button => button.addEventListener("click", () => {
  kind = button.dataset.kind; page = 1;
  document.querySelectorAll("[data-kind]").forEach(b => b.classList.toggle("selected",b===button)); loadTable();
}));
let timer;
$("search").addEventListener("input", () => { clearTimeout(timer); requestVersion++; timer=setTimeout(()=>{page=1;loadTable();},200); });
$("filter").addEventListener("change",()=>{page=1;loadTable();});
$("prev").addEventListener("click",()=>{page--;loadTable();});
$("next").addEventListener("click",()=>{page++;loadTable();});
$("close-detail").addEventListener("click",()=>$("detail").close());
document.querySelectorAll("nav a").forEach(link=>link.addEventListener("click",()=>{document.querySelectorAll("nav a").forEach(a=>a.classList.toggle("active",a===link));}));
api("/api/summary").then(data=>{renderSummary(data);return loadTable();}).catch(showError);

$("run-picker").addEventListener("change",()=>{ const url = new URL(location.href); url.searchParams.set("run",$("run-picker").value); url.hash=""; location.href=url.href; });
