"use strict";
/* Review UI. All upstream-derived strings are inserted with textContent:
   record payloads, scan ids and ordering names are untrusted data. */

const state = {
  sessionId: null,
  data: null,
  tab: "run",
  selectedRecord: null,
  compareChoices: { overlap_backfill: "", rebaseline: "" },
};

const $ = (sel, root = document) => root.querySelector(sel);
const el = (tag, attrs = {}, ...children) => {
  const node = document.createElement(tag);
  for (const [k, v] of Object.entries(attrs)) {
    if (v === null || v === undefined || v === false) continue;
    if (k === "class") node.className = v;
    else if (k === "text") node.textContent = v;
    else if (k.startsWith("on")) node.addEventListener(k.slice(2), v);
    else if (k === "style") node.setAttribute("style", v);
    else node.setAttribute(k, v === true ? "" : String(v));
  }
  for (const c of children.flat()) {
    if (c === null || c === undefined || c === false) continue;
    node.append(c instanceof Node ? c : document.createTextNode(String(c)));
  }
  return node;
};

async function api(method, path, body) {
  const res = await fetch(path, {
    method,
    headers: body ? { "Content-Type": "application/json" } : {},
    body: body ? JSON.stringify(body) : undefined,
  });
  const payload = await res.json();
  if (!res.ok) throw new Error(payload.error || res.statusText);
  return payload;
}

/* ------------------------------------------------------------------ charts */
const SERIES = ["var(--series-1)", "var(--series-2)", "var(--series-3)", "var(--series-4)"];
const tooltip = $("#tooltip");
function showTip(evt, title, rows) {
  tooltip.replaceChildren();
  tooltip.append(el("div", { class: "t-title", text: title }));
  for (const r of rows) {
    tooltip.append(el("div", { class: "t-row" },
      el("span", { class: "t-key", style: `background:${r.color}` }),
      el("span", { class: "secondary", text: r.label }),
      el("span", { class: "t-val", text: r.value })));
  }
  tooltip.dataset.show = "1";
  const pad = 14;
  const w = tooltip.offsetWidth, h = tooltip.offsetHeight;
  const x = Math.min(evt.clientX + pad, window.innerWidth - w - 8);
  const y = Math.min(evt.clientY + pad, window.innerHeight - h - 8);
  tooltip.style.left = `${x}px`;
  tooltip.style.top = `${y}px`;
}
function hideTip() { tooltip.dataset.show = "0"; }

/* Grouped horizontal bars: planned requests per strategy, plus the response
   split (fresh pages vs overlap re-read). Labels ride each bar end. */
function planChart(plans) {
  const entries = Object.entries(plans);
  if (!entries.length) return el("p", { class: "muted small", text: "No rehearsed plan yet." });
  const W = 520, rowH = 54, padL = 118, padR = 46, padT = 8, padB = 22;
  const H = padT + padB + entries.length * rowH;
  const max = Math.max(...entries.map(([, p]) => Math.max(p.planned_pages, p.baseline_pages)));
  const x = (v) => padL + (v / max) * (W - padL - padR);
  const svg = document.createElementNS("http://www.w3.org/2000/svg", "svg");
  svg.setAttribute("viewBox", `0 0 ${W} ${H}`);
  svg.setAttribute("role", "img");
  svg.setAttribute("aria-label", "Planned pages per recovery strategy compared with a naive continuation");
  const add = (tag, attrs, text) => {
    const n = document.createElementNS("http://www.w3.org/2000/svg", tag);
    for (const [k, v] of Object.entries(attrs)) n.setAttribute(k, v);
    if (text !== undefined) n.textContent = text;
    svg.append(n);
    return n;
  };
  // gridlines at whole pages
  for (let v = 0; v <= max; v += Math.max(1, Math.ceil(max / 5))) {
    add("line", { x1: x(v), x2: x(v), y1: padT, y2: H - padB, stroke: "var(--grid)", "stroke-width": 1 });
    add("text", { x: x(v), y: H - 6, "text-anchor": "middle", class: "axis-label" }, String(v));
  }
  entries.forEach(([key, p], i) => {
    const y0 = padT + i * rowH;
    const label = key === "rebaseline" ? "Rebaseline" : "Overlap backfill";
    add("text", { x: padL - 10, y: y0 + 15, "text-anchor": "end", class: "cat-label" }, label);
    const bars = [
      { label: "plan pages", value: p.planned_pages, y: y0 + 4, color: SERIES[0] },
      { label: "naive continuation", value: p.baseline_pages, y: y0 + 26, color: "var(--axis)" },
    ];
    for (const b of bars) {
      const bw = Math.max(0, x(b.value) - padL);
      const r = add("rect", { x: padL, y: b.y, width: Math.max(bw, b.value ? 2 : 0), height: 16,
        rx: 4, fill: b.color });
      r.style.cursor = "default";
      const hit = add("rect", { x: padL, y: b.y - 3, width: Math.max(bw, 12), height: 22,
        fill: "transparent" });
      hit.addEventListener("pointermove", (e) => showTip(e, label, [
        { label: b.label, value: String(b.value), color: b.color },
        { label: "probe requests", value: String(p.probe_requests), color: "var(--series-4)" },
        { label: "extra vs naive", value: `${p.extra_requests >= 0 ? "+" : ""}${p.extra_requests}`, color: "var(--series-2)" },
      ]));
      hit.addEventListener("pointerleave", hideTip);
      hit.setAttribute("tabindex", "0");
      hit.setAttribute("role", "img");
      hit.setAttribute("aria-label", `${label}: ${b.label} ${b.value} pages`);
      hit.addEventListener("focus", (e) => showTip({ clientX: e.target.getBoundingClientRect().right, clientY: e.target.getBoundingClientRect().top }, label, [
        { label: b.label, value: String(b.value), color: b.color },
      ]));
      hit.addEventListener("blur", hideTip);
    }
    add("text", { x: x(p.planned_pages) + 6, y: y0 + 15, class: "value-label" }, `${p.planned_pages} pages`);
    add("text", { x: x(p.baseline_pages) + 6, y: y0 + 37, class: "value-label", fill: "var(--text-secondary)" }, `${p.baseline_pages}`);
  });
  const wrap = el("div", { class: "chart" });
  wrap.append(el("p", { class: "small muted", style: "margin:0 0 2px",
    text: "Pages fetched, measured against the live source" }));
  wrap.append(el("div", { class: "legend" },
    el("span", { class: "item" }, el("span", { class: "key rect", style: `background:${SERIES[0]}` }), "Plan pages (measured in rehearsal)"),
    el("span", { class: "item" }, el("span", { class: "key rect", style: "background:var(--axis)" }), "Naive continuation from cursor")));
  wrap.append(svg);
  return wrap;
}

/* Stacked coverage bar: verified / stale / unverified / unobserved, per scan set. */
function coverageBar(v) {
  if (!v) return el("p", { class: "muted small", text: "Verification has not run." });
  const parts = [
    { key: "verified", label: "Verified", value: v.counts.verified, color: SERIES[2] },
    { key: "stale", label: "Stale revision", value: v.counts.stale, color: SERIES[3] },
    { key: "unverified", label: "Unverified", value: v.counts.unverified, color: SERIES[1] },
    { key: "unobserved", label: "Unobserved", value: v.counts.unobserved, color: SERIES[0] },
  ];
  const total = parts.reduce((a, b) => a + b.value, 0) || 1;
  const bar = el("div", { class: "segbar", role: "img",
    "aria-label": `Coverage of ${total} upstream records: ` + parts.map((p) => `${p.label} ${p.value}`).join(", ") });
  for (const p of parts) {
    if (!p.value) continue;
    const pct = (p.value / total) * 100;
    const seg = el("div", { class: "seg", style: `width:${pct}%;background:${p.color}`,
      title: `${p.label}: ${p.value}` }, pct > 12 ? String(p.value) : "");
    seg.addEventListener("pointermove", (e) => showTip(e, "Verification", parts.map((q) => ({
      label: q.label, value: `${q.value}`, color: q.color }))));
    seg.addEventListener("pointerleave", hideTip);
    bar.append(seg);
  }
  return el("div", { class: "stack" },
    bar,
    el("div", { class: "legend" }, parts.map((p) =>
      el("span", { class: "item" },
        el("span", { class: "key rect", style: `background:${p.color}` }),
        `${p.label} ${p.value}`))));
}

/* ------------------------------------------------------------------ views */
function statusBadge(status) {
  const cls = status === "complete" || status === "recovered" ? "good"
    : status === "requires_review" || status === "refused" ? "bad" : "warn";
  return el("span", { class: `badge ${cls}` }, el("span", { class: "dot" }), status.replace(/_/g, " "));
}

function statTile(label, value, sub) {
  return el("div", { class: "stat" },
    el("span", { class: "label", text: label }),
    el("span", { class: "value", text: String(value) }),
    sub ? el("span", { class: "sub", text: sub }) : null);
}

function renderRun(d) {
  const frag = el("div", { class: "grid" });
  const stepIndex = ["ready", "partially_collected", "upgraded", "planned", "recovered", "late_process_refused"].indexOf(d.step);
  const status = d.resume ? d.resume.status : d.step;

  frag.append(el("div", { class: "grid cols-3" },
    el("div", { class: "card stack" },
      el("div", { class: "row" }, el("h2", { text: "Source" }), el("span", { class: "spacer" }),
        statusBadge(d.step)),
      el("dl", { class: "kv" },
        el("dt", { text: "source id" }), el("dd", { class: "mono", text: d.source.source_id }),
        el("dt", { text: "serving order" }), el("dd", { class: "mono", text: d.source.serving_ordering }),
        el("dt", { text: "records" }), el("dd", { text: String(d.source.size) }),
        el("dt", { text: "versions" }), el("dd", { class: "mono", text: d.source.versions.map((v) => `${v.version}/${v.ordering}`).join("  ·  ") }))),
    el("div", { class: "card stack" },
      el("h2", { text: "Partial scan" }),
      d.partial ? el("dl", { class: "kv" },
        el("dt", { text: "scan" }), el("dd", { class: "mono", text: d.partial.scan_id }),
        el("dt", { text: "status" }), el("dd", {}, statusBadge(d.partial.status)),
        el("dt", { text: "records seen" }), el("dd", { text: String(d.partial.observed) }),
        el("dt", { text: "requests" }), el("dd", { text: String(d.partial.requests) }))
        : el("p", { class: "muted small", text: "Not started." }),
      d.cursor ? el("div", { class: "note small" },
        el("strong", { text: `Cursor at offset ${d.cursor.offset}` }),
        ` recorded under `, el("code", { text: d.cursor.ordering }),
        `. Boundary ids `, el("code", { text: d.cursor.boundary_ids.join(", ") }),
        `; total at stop ${d.cursor.total_at_stop}.`) : null),
    el("div", { class: "card stack" },
      el("h2", { text: "Naive resume attempt" }),
      d.resume ? el("div", { class: "stack" },
        el("div", { class: "row" }, statusBadge(d.resume.status), el("span", { class: "secondary small", text: d.resume.stop_reason || "" })),
        el("dl", { class: "kv" },
          el("dt", { text: "probe verdict" }), el("dd", { class: "mono", text: d.resume.probe.verdict }),
          el("dt", { text: "detected order" }), el("dd", { class: "mono", text: d.resume.probe.detected_ordering || "—" })),
        d.resume.assessment ? el("ul", { class: "reasons" },
          d.resume.assessment.reasons.map((r) => el("li", { text: r }))) : null)
        : el("p", { class: "muted small", text: "Run the upgrade to attempt a naive resume." }))));

  // Actions
  const actions = el("div", { class: "card row" });
  const canPartial = d.step === "ready";
  const canUpgrade = d.step === "partially_collected";
  const canPlan = ["upgraded", "planned", "late_process_refused",
    "same_version_checked"].includes(d.step);
  const canApprove = !d.approved && d.step === "planned";
  const send = (path, body) => async (e) => {
    const btn = e.currentTarget; btn.disabled = true;
    try {
      state.data = await api("POST", `/api/session/${state.sessionId}/${path}`, body || {});
      render();
    } catch (err) { alert(err.message); btn.disabled = false; }
  };
  actions.append(
    el("button", { class: "btn", disabled: !canPartial, onclick: send("run_partial") }, "1 · Collect half"),
    el("button", { class: "btn", disabled: !canUpgrade, onclick: send("upgrade") }, "2 · Deploy upgrade"),
    el("button", { class: "btn", disabled: !canPlan, onclick: send("plan", { strategy: "overlap_backfill" }) }, "Rehearse overlap plan"),
    el("button", { class: "btn", disabled: !canPlan, onclick: send("plan", { strategy: "rebaseline" }) }, "Rehearse rebaseline"),
    el("button", { class: "btn danger", disabled: d.scenario_id !== "late_process" || d.late_process !== null || d.step === "recovered", onclick: send("late_process") }, "Late process resume"),
    el("button", { class: "btn danger", disabled: d.scenario_id !== "repeated_revisions" || d.same_version !== null || d.step === "recovered" || d.step === "partially_collected" || d.step === "ready", onclick: send("same_version_resume") }, "Same-version revision resume"),
    el("span", { class: "spacer" }),
    el("button", { class: "btn primary", disabled: !canApprove, onclick: () => approveDialog(d) }, d.approved ? "Approved" : "3 · Approve a plan…"));
  frag.append(actions);

  if (Object.keys(d.plans).length) {
    const planCards = Object.entries(d.plans).map(([key, p]) => {
      const residual = p.residual_ids.length
        ? el("span", { class: "pill-row" }, p.residual_ids.map((i) =>
            el("span", { class: "idchip residual", text: `#${i}` })))
        : el("span", { class: "muted", text: "none identified" });
      return el("div", { class: "stack" },
        el("div", { class: "row" },
          el("h3", { text: key === "rebaseline" ? "Rebaseline" : "Overlap backfill" }),
          el("span", { class: "spacer" }),
          p.approved ? el("span", { class: "badge good" }, "approved") : null),
        el("dl", { class: "kv" },
          el("dt", { text: "from offset" }), el("dd", { class: "mono", text: String(p.from_offset) }),
          el("dt", { text: "plan pages" }), el("dd", { text: `${p.planned_pages} (+${p.probe_requests} probe)` }),
          el("dt", { text: "naive continuation" }), el("dd", { text: `${p.baseline_pages} pages` }),
          el("dt", { text: "extra requests" }), el("dd", {}, el("strong", { text: `${p.extra_requests >= 0 ? "+" : ""}${p.extra_requests}` })),
          el("dt", { text: "re-read" }), el("dd", { text: `${p.rechecked_ids.length} already-seen records` }),
          el("dt", { text: "residual" }), el("dd", {}, residual)),
        el("div", { class: "small secondary" },
          el("p", { text: "Cannot promise:" }),
          el("ul", { class: "reasons" }, p.may_miss.map((m) =>
            el("li", { text: `${m.window} — ${m.reason} (${m.bounded_by})` })))));
    });
    frag.append(el("div", { class: "section" },
      el("div", { class: "section-head" },
        el("h2", { text: "Recovery plans — measured, not estimated" }),
        el("span", { class: "hint", text: "Rehearsal pages are fetched from the live source and never journalled." })),
      el("div", { class: "grid cols-2" },
        el("div", { class: "card" }, planChart(d.plans)),
        el("div", { class: "card stack" }, planCards))));
  }

  if (d.same_version) {
    const a = d.same_version.assessment;
    frag.append(el("div", { class: "section" },
      el("div", { class: "section-head" },
        el("h2", { text: "Same-version resume (revisions only)" }),
        el("span", { class: "hint", text: "No ordering change. The sort key itself moved." })),
      el("div", { class: "card stack" },
        el("div", { class: "row" }, statusBadge(d.same_version.status),
          el("span", { class: "small secondary", text: d.same_version.stop_reason || "" })),
        el("dl", { class: "kv" },
          el("dt", { text: "detected order" }), el("dd", { class: "mono", text: d.same_version.probe.detected_ordering || "—" }),
          el("dt", { text: "safe to resume" }), el("dd", {}, el("strong", { text: String(a ? a.safe_to_resume : "—") })),
          el("dt", { text: "drifted ids" }), el("dd", { class: "mono", text: a && a.revisions_drifted.length ? a.revisions_drifted.map((i) => "#" + i).join(", ") : "—" })),
        a ? el("ul", { class: "reasons" }, a.reasons.map((r) => el("li", { text: r }))) : null)));
  }

  if (d.late_process) {
    frag.append(el("div", { class: "section" },
      el("div", { class: "section-head" }, el("h2", { text: "Late process (separate OS process)" })),
      el("div", { class: "card stack" },
        el("div", { class: "row" }, statusBadge(d.late_process.status),
          el("span", { class: "mono small secondary", text: `pid ${d.late_process.pid}` }),
          el("span", { class: "small secondary", text: d.late_process.reason || "" })),
        el("dl", { class: "kv" },
          el("dt", { text: "detected order" }), el("dd", { class: "mono", text: d.late_process.detected_ordering || "—" }),
          el("dt", { text: "safe to resume" }), el("dd", {}, el("strong", { text: String(d.late_process.safe) }))),
        el("ul", { class: "reasons" }, (d.late_process.reasons || []).map((r) => el("li", { text: r }))))));
  }

  if (d.verification) {
    const v = d.verification;
    frag.append(el("div", { class: "section" },
      el("div", { class: "section-head" },
        el("h2", { text: "Verification after approval" }),
        el("span", { class: "hint", text: "Per id and per offset run — never the record total." })),
      el("div", { class: "card stack" },
        el("div", { class: "row" },
          v.closure === "closed"
            ? el("span", { class: "badge good" }, "closed")
            : el("span", { class: "badge warn" }, v.closure.replace("_", " ")),
          el("span", { class: "small secondary", text: `${v.counts.verified} verified · ${v.counts.stale} stale · ${v.counts.unobserved} unobserved` }),
          el("span", { class: "spacer" }),
          el("span", { class: "small secondary", text: d.ingest ? `${d.ingest.inserted} inserted, ${d.ingest.updated} updated, ${d.ingest.skipped} skipped` : "" })),
        coverageBar(v),
        el("ul", { class: "reasons" }, v.rationale.map((r) => el("li", { text: r }))))));
  }

  if (d.events.length) {
    frag.append(el("div", { class: "section" },
      el("div", { class: "section-head" }, el("h2", { text: "What happened" })),
      el("div", { class: "card" }, el("ul", { class: "events" }, d.events.map((e) =>
        el("li", {}, el("span", { class: "step", text: e.step }), el("span", { text: e.detail })))))));
  }
  return frag;
}

function renderSource(d) {
  const rows = d.source.manifest;
  const v = d.verification;
  const stateById = {};
  if (v) for (const r of v.records) stateById[r.record_id] = r.state;
  const tbody = el("tbody");
  for (const r of rows) {
    const st = stateById[r.id];
    const tr = el("tr", { tabindex: "0", class: state.selectedRecord === r.id ? "selected" : "" },
      el("td", { class: "mono", text: `#${r.id}` }),
      el("td", { class: "num", text: String(r.revision) }),
      el("td", { class: "num", text: String(r.updated_at) }),
      el("td", { class: "num", text: String(r.position_v1) }),
      el("td", { class: "num", text: String(r.position_v2) }),
      el("td", {}, r.moved ? el("span", { class: "badge warn", text: `moved ${r.position_v1}→${r.position_v2}` }) : el("span", { class: "muted", text: "same" })),
      el("td", {}, r.in_partial_scan ? el("span", { class: "badge", text: "in partial" }) : el("span", { class: "muted", text: "—" })),
      el("td", {}, st ? el("span", { class: `badge ${st === "verified" ? "good" : st === "unobserved" ? "bad" : "warn"}`, text: st }) : el("span", { class: "muted", text: "—" })));
    tr.addEventListener("click", () => { state.selectedRecord = r.id; render(); });
    tr.addEventListener("keydown", (e) => { if (e.key === "Enter") { state.selectedRecord = r.id; render(); } });
    tbody.append(tr);
  }
  const table = el("table", {},
    el("thead", {}, el("tr", {},
      el("th", { text: "id" }), el("th", { class: "num", text: "rev" }), el("th", { class: "num", text: "updated" }),
      el("th", { class: "num", text: "pos v1" }), el("th", { class: "num", text: "pos v2" }),
      el("th", { text: "position" }), el("th", { text: "scan-a" }), el("th", { text: "verified" }))),
    tbody);
  const detail = el("div", { class: "card stack" });
  const rid = state.selectedRecord;
  if (rid && d.provenance[String(rid)]) {
    const prov = d.provenance[String(rid)];
    detail.append(el("h3", { text: `Record #${rid} — provenance` }));
    if (!prov.length) detail.append(el("p", { class: "muted small", text: "No observations." }));
    for (const p of prov) {
      detail.append(el("dl", { class: "kv small" },
        el("dt", { text: "scan" }), el("dd", { class: "mono", text: `${p.scan_id} / ${p.run_id}` }),
        el("dt", { text: "version" }), el("dd", { class: "mono", text: `${p.version} · ${p.ordering} (served ${p.served_ordering})` }),
        el("dt", { text: "revision" }), el("dd", { text: `${p.revision} at updated_at ${p.updated_at}` }),
        el("dt", { text: "request" }), el("dd", { class: "mono", text: `seq ${p.request_seq}` }),
        el("dt", { text: "payload" }), el("dd", { class: "mono", text: p.payload_fingerprint })));
    }
  } else if (rid) {
    detail.append(el("h3", { text: `Record #${rid}` }),
      el("p", { class: "muted small", text: "This record has no ingested provenance yet — it was never observed by an accepted scan, or ingest has not run." }));
  } else {
    detail.append(el("p", { class: "muted small", text: "Select a record to see which scan, request and ordering produced it." }));
  }
  return el("div", { class: "grid cols-2" },
    el("div", { class: "card" },
      el("div", { class: "section-head" }, el("div", {},
        el("h2", { text: "Source manifest" }),
        el("p", { class: "hint", text: "Position under each version. 'moved' means the record's offset changed when the ordering changed." }))),
      el("div", { class: "scroll" }, table)),
    detail);
}

function renderVerify(d) {
  const v = d.verification;
  if (!v) return el("div", { class: "card" }, el("p", { class: "muted", text: "Approve a plan to run verification and ingest." }));
  const closureBadge = v.closure === "closed" ? el("span", { class: "badge good" }, "closed")
    : v.closure === "bounded_open" ? el("span", { class: "badge warn" }, "bounded open")
      : el("span", { class: "badge bad" }, "open");
  return el("div", { class: "grid" },
    el("div", { class: "card stack" },
      el("div", { class: "row" }, el("h2", { text: "Verification" }), closureBadge,
        el("span", { class: "spacer" }),
        el("span", { class: "small secondary", text: `${v.observed_total} of ${v.upstream_total} upstream records observed` })),
      coverageBar(v),
      el("ul", { class: "reasons" }, v.rationale.map((r) => el("li", { text: r }))),
      el("div", { class: "note small" },
        el("strong", { text: "The total is not the test. " }),
        "A record can be revised so the count stays identical while its old position is gone. Closure is decided per id and per contiguous offset run.")),
    el("div", { class: "grid cols-2" },
      el("div", { class: "card" }, el("h3", { text: "Contiguous request runs" }),
        v.contiguous_ranges.length ? el("table", {},
          el("thead", {}, el("tr", {}, el("th", { text: "scan" }), el("th", { text: "requests" }), el("th", { class: "num", text: "pages" }), el("th", { class: "num", text: "records" }))),
          el("tbody", {}, v.contiguous_ranges.map((r) => el("tr", {},
            el("td", { class: "mono", text: r.scan_id }),
            el("td", { class: "mono", text: `${r.first_seq}–${r.last_seq}` }),
            el("td", { class: "num", text: String(r.pages) }),
            el("td", { class: "num", text: String(r.ids.length) }))))) : el("p", { class: "muted small", text: "none" })),
      el("div", { class: "card" }, el("h3", { text: "Gaps and residuals" }),
        el("p", { class: "small secondary", text: `Unobserved ids: ${v.records.filter((r) => r.state === "unobserved").map((r) => `#${r.record_id}`).join(", ") || "none"}` }),
        el("p", { class: "small secondary", text: `Stale ids: ${v.records.filter((r) => r.state === "stale").map((r) => `#${r.record_id}`).join(", ") || "none"}` }),
        el("p", { class: "small secondary", text: `Ingest: ${d.ingest ? `${d.ingest.inserted} inserted, ${d.ingest.updated} updated, ${d.ingest.skipped} skipped` : "not run"}` }))),
    el("div", { class: "card" }, el("h3", { text: "Request log (every page, every attempt)" }),
      el("div", { class: "scroll" }, el("table", {},
        el("thead", {}, el("tr", {}, el("th", { class: "num", text: "#" }), el("th", { text: "scan" }), el("th", { class: "num", text: "offset" }), el("th", { text: "served" }), el("th", { text: "ids" }), el("th", { text: "kind" }))),
        el("tbody", {}, d.requests.map((r) => el("tr", {},
          el("td", { class: "num mono", text: String(r.seq) }),
          el("td", { class: "mono", text: r.scan_id }),
          el("td", { class: "num mono", text: String(r.offset) }),
          el("td", { class: "mono", text: r.served_ordering }),
          el("td", { class: "mono", text: r.ids.join(", ") }),
          el("td", {}, r.rehearsal ? el("span", { class: "badge warn", text: "rehearsal" }) : el("span", { class: "muted", text: "live" })))))))));
}

function scanTable(d) {
  const head = el("tr", {},
    el("th", { text: "scan" }), el("th", { text: "version" }),
    el("th", { text: "ordering" }), el("th", { text: "status" }),
    el("th", { text: "stop reason" }), el("th", { class: "num", text: "requests" }),
    el("th", { text: "evidence" }));
  const body = el("tbody");
  for (const s of d.scans) {
    const isRehearsal = s.scan_id.startsWith("rehearsal:");
    const evidence = isRehearsal
      ? el("span", { class: "badge warn", text: "rehearsal (not evidence)" })
      : s.accepted === false
        ? el("span", { class: "badge bad", text: "rejected" })
        : el("span", { class: "badge good", text: "accepted" });
    body.append(el("tr", {},
      el("td", { class: "mono", text: s.scan_id }),
      el("td", { class: "mono", text: s.version || "—" }),
      el("td", { class: "mono", text: s.ordering || "—" }),
      el("td", {}, statusBadge(s.status)),
      el("td", { class: "secondary small", text: s.stop_reason || "—" }),
      el("td", { class: "num", text: String((s.request_seqs || []).length) }),
      el("td", {}, evidence)));
  }
  return el("div", { class: "card" },
    el("h2", { text: "Scans" }),
    el("table", {}, el("thead", {}, head), body));
}

function render() {
  const d = state.data;
  const app = $("#app");
  app.replaceChildren();
  if (!d) { app.append(el("div", { class: "card" }, "Loading…")); return; }

  const nextStep = {
    ready: "Collect half the source",
    partially_collected: "Deploy the ordering upgrade",
    upgraded: "Inspect the refused resume, then choose a recovery",
    planned: "Compare the rehearsed plans and approve one",
    recovered: "Recovery approved — review provenance",
    late_process_refused: "The late process was refused — plan a recovery",
  }[d.step] || d.step;

  const tabs = el("div", { class: "tabs", role: "tablist" },
    [["run", "Run"], ["source", "Source list"], ["verify", "Verification"], ["scans", "Scans"]].map(([key, label]) =>
      el("button", { class: "tab", role: "tab", "aria-selected": String(state.tab === key), onclick: () => { state.tab = key; render(); } }, label)));

  const banner = el("div", { class: "card row", style: "margin-top:14px" },
    el("strong", { text: d.title }),
    el("span", { class: "secondary", text: `— ${d.blurb}` }),
    el("span", { class: "spacer" }),
    el("span", { class: "badge", text: nextStep }));

  app.append(banner, tabs);
  if (state.tab === "run") app.append(renderRun(d));
  else if (state.tab === "source") app.append(renderSource(d));
  else if (state.tab === "verify") app.append(renderVerify(d));
  else app.append(scanTable(d));
}

/* ------------------------------------------------------------- approvals */
function approveDialog(d) {
  const plans = Object.entries(d.plans);
  if (!plans.length) { alert("Rehearse a plan first."); return; }
  const dialog = el("div", { class: "card stack", role: "dialog", "aria-modal": "true",
    "aria-labelledby": "approve-title", style: "width:min(640px,92vw);max-height:82vh;overflow:auto" });
  const backdrop = el("div", { style: "position:fixed;inset:0;z-index:100;display:flex;align-items:center;justify-content:center;background:rgba(0,0,0,.34)" });
  backdrop.append(dialog);
  const closing = () => { backdrop.remove(); document.removeEventListener("keydown", onKey); };
  const onKey = (e) => { if (e.key === "Escape") closing(); };
  document.addEventListener("keydown", onKey);
  backdrop.addEventListener("click", (e) => { if (e.target === backdrop) closing(); });
  const choice = el("select", {}, plans.map(([k]) => el("option", { value: k }, k === "rebaseline" ? "Rebaseline (full pass)" : "Overlap backfill")));
  const body = el("div", { class: "stack" });
  const update = () => {
    const p = d.plans[choice.value];
    body.replaceChildren(
      el("dl", { class: "kv" },
        el("dt", { text: "requests" }), el("dd", { text: `${p.planned_pages} pages + ${p.probe_requests} probe, vs ${p.baseline_pages} naive (${p.extra_requests >= 0 ? "+" : ""}${p.extra_requests})` }),
        el("dt", { text: "re-read" }), el("dd", { text: `${p.rechecked_ids.length} already-seen records (${p.rechecked_ids.map((i) => "#" + i).join(" ") || "none"})` }),
        el("dt", { text: "residual" }), el("dd", { text: p.residual_ids.map((i) => "#" + i).join(", ") || "none identified" })),
      el("div", { class: "note small" },
        el("strong", { text: "This plan still cannot promise: " }),
        el("ul", { class: "reasons" }, p.may_miss.map((m) => el("li", { text: `${m.window} (${m.bounded_by})` })))),
      el("p", { class: "small secondary", text: "Approval executes the plan against the live source and then verifies by id. Rehearsal requests are excluded from verification." }));
  };
  choice.addEventListener("change", update);
  update();
  const status = el("p", { class: "small secondary" });
  dialog.append(
    el("div", { class: "row" }, el("h2", { id: "approve-title", text: "Approve a recovery plan" }), el("span", { class: "spacer" }),
      el("button", { class: "btn", onclick: closing }, "Cancel")),
    el("div", { class: "row" }, el("label", { class: "small secondary" }, "Strategy ", choice)),
    body, status,
    el("div", { class: "row" }, el("span", { class: "spacer" }),
      el("button", { class: "btn primary", onclick: async () => {
        status.textContent = "Executing…";
        try {
          state.data = await api("POST", `/api/session/${state.sessionId}/approve`, { strategy: choice.value });
          closing(); render();
        } catch (err) { status.textContent = err.message; }
      } }, "Approve and execute")));
  document.body.append(backdrop);
  dialog.querySelector("select").focus();
}

/* ------------------------------------------------------------------ boot */
async function boot() {
  const meta = await api("GET", "/api/scenarios");
  const sel = $("#scenario");
  for (const s of meta.scenarios) sel.append(el("option", { value: s.id, text: s.title }));
  const start = (id) => api("POST", "/api/session", { scenario: id }).then((r) => {
    state.sessionId = r.session_id; state.data = r.state; state.tab = "run"; state.selectedRecord = null; render();
  });
  sel.addEventListener("change", () => start(sel.value).catch((e) => alert(e.message)));
  $("#theme-toggle").addEventListener("click", () => {
    const root = document.documentElement;
    const now = root.getAttribute("data-theme");
    root.setAttribute("data-theme", now === "dark" ? "light" : "dark");
  });
  await start(meta.scenarios[0].id);
}
boot().catch((e) => { $("#app").textContent = `Failed to load: ${e.message}`; });
