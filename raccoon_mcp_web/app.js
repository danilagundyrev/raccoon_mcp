import {
  escapeHTML as esc,
  humanize,
  isLive,
  effectiveStatus,
  scopedData,
  metrics,
  eventLabel,
} from "./model.js";

const $ = (selector) => document.querySelector(selector);
const icons = {
  overview:
    '<rect x="3" y="3" width="7" height="7" rx="1.5"/><rect x="14" y="3" width="7" height="7" rx="1.5"/><rect x="3" y="14" width="7" height="7" rx="1.5"/><rect x="14" y="14" width="7" height="7" rx="1.5"/>',
  agents:
    '<path d="m12 3 8 4.5v9L12 21l-8-4.5v-9L12 3Z"/><path d="m4 7.5 8 4.5 8-4.5M12 12v9M8 5.2l8 4.6"/>',
  missions:
    '<rect x="3" y="4" width="18" height="16" rx="2"/><path d="M9 4v16M15 4v16M5.5 8h1M11.5 8h1M17.5 8h1M5.5 12h1M11.5 12h1"/>',
  messages:
    '<path d="M21 11.5a8.5 8.5 0 0 1-8.5 8.5H4l-1 2v-9.5A8.5 8.5 0 0 1 11.5 4H13M15 3h6v6M21 3l-9 9"/>',
  artifacts:
    '<path d="M14 3H6a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V9l-6-6Z"/><path d="M14 3v6h6M8 13h8M8 17h5"/>',
  events: '<path d="M3 12h4l3-8 4 16 3-8h4"/>',
  search: '<circle cx="10.5" cy="10.5" r="6.5"/><path d="m16 16 5 5"/>',
  arrow: '<path d="M4 12h16m-6-6 6 6-6 6"/>',
  refresh:
    '<path d="M20 7v5h-5M4 17v-5h5"/><path d="M6 7a7 7 0 0 1 12-1l2 6M4 12l2 6a7 7 0 0 0 12-1"/>',
  motion: '<path d="M5 4v16l14-8L5 4Z"/>',
  pause: '<path d="M8 5v14M16 5v14"/>',
  globe:
    '<circle cx="12" cy="12" r="9"/><ellipse cx="12" cy="12" rx="4" ry="9"/><path d="M3 12h18"/>',
  check: '<path d="m5 12 4 4L19 6"/>',
  alert: '<path d="m12 3 10 18H2L12 3ZM12 9v5m0 3v.1"/>',
  network:
    '<rect x="9" y="3" width="6" height="5" rx="1"/><rect x="2" y="16" width="6" height="5" rx="1"/><rect x="16" y="16" width="6" height="5" rx="1"/><path d="M12 8v4M5 16v-4h14v4"/>',
  terminal: '<path d="m5 7 5 5-5 5m8 0h6"/>',
};
const icon = (name, className = "") =>
  `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true" class="${className}">${icons[name] || icons.agents}</svg>`;
const navigation = [
  ["overview", "Mission control", ""],
  ["agents", "Agent network", "instances"],
  ["missions", "Workboard", "work_items"],
  ["messages", "Transmissions", "messages"],
  ["artifacts", "Artifacts & reports", "artifacts"],
  ["events", "Event log", "events"],
];
const viewDescriptions = {
  overview: [
    "Your agents. One orbit.",
    "A live view of every agent, mission and moving part.",
  ],
  agents: [
    "Agent network",
    "Orchestrators and workers. Every connection, every heartbeat.",
  ],
  missions: [
    "The workboard",
    "From the first idea to independent verification. Follow the work.",
  ],
  messages: [
    "Transmissions",
    "The conversation between your repositories, in one place.",
  ],
  artifacts: [
    "Artifacts & reports",
    "Implementation reports, audit verdicts and shared knowledge.",
  ],
  events: [
    "Event log",
    "Every state change. A chronological record of your coordination universe.",
  ],
};
let snapshot = null;
let state = scopedData();
let project = "";
let view = navigation.some((item) => item[0] === location.hash.slice(1))
  ? location.hash.slice(1)
  : "overview";
let query = "";
let filter = "all";
let featureFilter = "";
let demo = false;
let demoData = null;
let connection = "connecting";
let lastSync = null;
let serverOffset = 0;
let timer;
let request;
let lastSignature = "";
let inspectorKey = null;
let inspectorSignature = "";
let inspectorRequest;
let toastTimer;
let motionOff = false;
let activeProjectIndex = 0;
try {
  motionOff = localStorage.getItem("raccoon-motion") === "off";
} catch {
  /* Storage can be disabled. */
}
const now = () => Date.now() + serverOffset;
const age = (timestamp) => {
  if (!timestamp) return "—";
  const seconds = Math.max(
    0,
    Math.floor((now() - Date.parse(timestamp)) / 1000),
  );
  if (!Number.isFinite(seconds)) return "—";
  if (seconds < 60) return `${seconds}s ago`;
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m ago`;
  if (seconds < 86400) return `${Math.floor(seconds / 3600)}h ago`;
  return `${Math.floor(seconds / 86400)}d ago`;
};
const relative = (timestamp) =>
  `<time datetime="${esc(timestamp)}" title="${esc(timestamp)}" data-time="${esc(timestamp)}">${age(timestamp)}</time>`;
const shortId = (id) => (id ? String(id).slice(0, 8) : "—");
const repo = (id) =>
  state.repositories.find((item) => item.repository_id === id);
const repoName = (id) =>
  repo(id)?.display_name || repo(id)?.repository_key || shortId(id);
const projectName = (id) =>
  state.projects.find((item) => item.project_id === id)?.display_name ||
  shortId(id);
const badge = (status) =>
  `<span class="badge ${esc(status)}">${esc(humanize(status))}</span>`;
const matches = (item) =>
  !query ||
  JSON.stringify(item).toLowerCase().includes(query.toLowerCase()) ||
  [
    item.repository_id,
    item.owner_repository_id,
    item.sender_repository_id,
    item.producer_repository_id,
  ].some((id) => repoName(id).toLowerCase().includes(query.toLowerCase()));
const openAttr = (type, id) => `data-open="${type}" data-id="${esc(id)}"`;
const empty = (title, body, small = false, name = "network") =>
  `<div class="empty-state ${small ? "small" : ""}"><div class="empty-icon">${icon(name)}</div><h2>${esc(title)}</h2><p>${esc(body)}</p></div>`;
const panelHeader = (title, name, right = "") =>
  `<div class="panel-header"><div class="panel-title">${icon(name)}${title}</div>${right}</div>`;
const link = (label, target) =>
  `<a class="text-button" href="#${target}">${label}${icon("arrow")}</a>`;

function projectChoices() {
  const search = $("#project-search").value.trim().toLowerCase();
  return [
    {
      project_id: "",
      display_name: "All projects",
      project_key: "Entire coordination universe",
    },
    ...state.projects,
  ].filter((item) =>
    `${item.display_name} ${item.project_key}`.toLowerCase().includes(search),
  );
}
function updateProjectPicker() {
  $("#project-label").textContent = project
    ? projectName(project)
    : "All projects";
  const choices = projectChoices();
  activeProjectIndex = Math.max(
    0,
    Math.min(activeProjectIndex, choices.length - 1),
  );
  const options =
    choices
      .map(
        (item, index) =>
          `<button type="button" role="option" id="project-option-${index}" class="project-option ${index === activeProjectIndex ? "is-focused" : ""}" aria-selected="${item.project_id === project}" tabindex="-1" data-workspace="${esc(item.project_id)}"><span class="project-option-icon">${icon(item.project_id ? "agents" : "globe")}</span><span class="project-option-text"><strong>${esc(item.display_name)}</strong><small>${esc(item.project_key)}</small></span><span class="project-option-check">${item.project_id === project ? icon("check") : ""}</span></button>`,
      )
      .join("") ||
    '<p class="project-options-empty" role="status">No matching projects</p>';
  if ($("#project-options").innerHTML !== options)
    $("#project-options").innerHTML = options;
  if (choices.length)
    $("#project-search").setAttribute(
      "aria-activedescendant",
      `project-option-${activeProjectIndex}`,
    );
  else $("#project-search").removeAttribute("aria-activedescendant");
}
function closeProjectPicker(restoreFocus = false) {
  $("#project-menu").hidden = true;
  $("#project").setAttribute("aria-expanded", "false");
  if (restoreFocus) $("#project").focus();
}
function openProjectPicker(last = false) {
  $("#project-search").value = "";
  const choices = projectChoices();
  activeProjectIndex = last
    ? choices.length - 1
    : Math.max(
        0,
        choices.findIndex((item) => item.project_id === project),
      );
  $("#project-menu").hidden = false;
  $("#project").setAttribute("aria-expanded", "true");
  updateProjectPicker();
  $("#project-search").focus();
  $(`#project-option-${activeProjectIndex}`)?.scrollIntoView({
    block: "nearest",
  });
}

function updateChrome() {
  const counts = metrics(state, now());
  $("#navigation").innerHTML = navigation
    .map(
      ([key, label, collection], index) =>
        `${index === 4 ? '<div class="nav-break">OBSERVABILITY</div>' : ""}<a class="nav-item ${view === key ? "active" : ""}" href="#${key}" ${view === key ? 'aria-current="page"' : ""}>${icon(key)}<span>${label}</span>${collection ? `<span class="nav-count">${collection === "instances" ? counts.online + counts.workers : state[collection].length}</span>` : ""}</a>`,
    )
    .join("");
  $("#breadcrumb").textContent = navigation.find((item) => item[0] === view)[1];
  document.title = `Raccoon / ${navigation.find((item) => item[0] === view)[1]}`;
  updateProjectPicker();
  $("#demo-toggle").innerHTML = demo
    ? "◈ &nbsp; Return to live server <span>↗</span>"
    : "◈ &nbsp; Explore demo universe <span>↗</span>";
  $("#server-version").textContent = `v${snapshot?.version || "0.1.0"}`;
  const offline = connection === "offline" && !demo;
  const connecting = connection === "connecting" && !demo;
  for (const selector of ["#sidebar-light", "#footer-light"])
    $(selector).className =
      `status-light ${offline ? "offline" : connecting ? "connecting" : ""}`;
  $("#sidebar-status").textContent = demo
    ? "Demo universe"
    : offline
      ? "Connection lost"
      : connecting
        ? "Establishing uplink"
        : "Server operational";
  $("#server-address").textContent = demo
    ? "isolated · browser only"
    : location.host;
  $("#footer-status").textContent = demo
    ? "DEMO · Simulated coordination state"
    : offline
      ? "Server unreachable · reconnecting automatically"
      : "Observer connected · read-only";
  const notice = $("#connection-notice");
  notice.hidden = !demo && !offline;
  notice.className = `notice ${demo ? "demo" : ""}`;
  notice.innerHTML = demo
    ? '<span>DEMO UNIVERSE · Illustrative data only. Your MCP state is untouched.</span><button data-action="exit-demo">Back to live server ↗</button>'
    : `<span>${snapshot ? "Uplink interrupted. Showing the last received state" : "No live snapshot received. Waiting for the server"}; retrying every 2 seconds.</span><button data-action="refresh">Retry now</button>`;
  $("#sync-time").textContent = lastSync
    ? `SYNCED ${age(lastSync)} · 2s INTERVAL`
    : "WAITING FOR SNAPSHOT";
}

function pageIntro() {
  const [title, description] = viewDescriptions[view];
  return `<div class="page-intro"><div><div class="eyebrow"><span class="eyebrow-line"></span>${demo ? "SIMULATED UNIVERSE" : "RACCOON OPERATIONS / " + (project ? esc(projectName(project)).toUpperCase() : "ALL SYSTEMS")}</div><h1>${title}</h1><p>${description}</p></div><div class="intro-actions"><span class="live-badge"><i class="status-light ${connection === "offline" && !demo ? "offline" : ""}"></i>${demo ? "DEMO MODE" : connection === "offline" ? "RECONNECTING" : "LIVE TELEMETRY"}</span><button class="outline-button" data-action="refresh">${icon("refresh")} Sync now</button></div></div>`;
}

function spark(values) {
  const max = Math.max(1, ...values);
  return `<svg class="metric-spark" viewBox="0 0 76 25" aria-hidden="true">${values.map((value, index) => `<rect x="${index * 6.4}" y="${25 - (value / max) * 24}" width="3" height="${Math.max(1, (value / max) * 24)}" rx="1"/>`).join("")}</svg>`;
}
function activityBuckets(items, field = "created_at") {
  const buckets = Array(12).fill(0);
  for (const item of items) {
    const hours = (now() - Date.parse(item[field])) / 3600000;
    if (hours >= 0 && hours < 12) buckets[11 - Math.floor(hours)]++;
  }
  return buckets;
}
function metricCards() {
  const counts = metrics(state, now());
  const cards = [
    [
      "Connected agents",
      counts.online + counts.workers,
      state.instances.length + state.workers.length,
      "agents",
      `${counts.online} orchestrators · ${counts.workers} workers`,
      state.instances,
      "started_at",
    ],
    [
      "Work in motion",
      counts.running,
      state.work_items.length,
      "events",
      `${state.work_items.filter((item) => item.status === "ready").length} ready for pickup`,
      state.work_items,
      "created_at",
    ],
    [
      "Completed work",
      counts.completed,
      state.work_items.length,
      "check",
      `${counts.verified} independently verified`,
      state.work_items.filter((item) => item.status === "completed"),
      "finished_at",
    ],
    [
      "Open requests",
      counts.requests,
      null,
      "messages",
      `${counts.attention} items need attention`,
      state.requests,
      "created_at",
    ],
  ];
  return `<div class="metrics-grid">${cards.map(([label, number, total, name, foot, items, field]) => `<div class="metric"><div class="metric-top"><span>${label}</span><span class="metric-icon">${icon(name)}</span></div><div class="metric-number">${number}${total !== null ? `<span class="total">/ ${total}</span>` : ""}</div><div class="metric-foot"><span class="accent">${name === "messages" && counts.attention ? "!" : "↗"}</span>${foot}</div>${spark(activityBuckets(items, field))}</div>`).join("")}</div>`;
}

function topology() {
  const repositories = state.repositories.filter(matches).slice(0, 8);
  const positions = repositories.map((repository, index) => {
    const angle =
      repositories.length === 1
        ? -Math.PI / 2
        : (Math.PI * 2 * index) / repositories.length - Math.PI / 2 - 0.35;
    return {
      repository,
      x: 400 + Math.cos(angle) * 252,
      y: 205 + Math.sin(angle) * 126,
    };
  });
  const nodes = positions
    .map(({ repository, x, y }, index) => {
      const instances = state.instances.filter(
        (item) => item.repository_id === repository.repository_id,
      );
      const workers = state.workers.filter(
        (item) =>
          item.repository_id === repository.repository_id &&
          isLive(item, now()),
      );
      const online = instances.filter((item) => isLive(item, now())).length;
      const count = online + workers.length;
      const path = `M400 205 Q${400 + (x - 400) * 0.1} ${y} ${x} ${y}`;
      const satellites = [
        ...instances.filter((item) => isLive(item, now())),
        ...workers,
      ]
        .slice(0, 5)
        .map((actor, actorIndex) => {
          const sx = x + (actorIndex - Math.min(count, 5) / 2 + 0.5) * 16;
          const sy = y - 44;
          const type = actor.instance_id ? "instances" : "workers";
          return `<path d="M${x} ${y - 22}L${sx} ${sy}" stroke="#687c6666" fill="none"/><g class="node-button" role="button" tabindex="0" aria-label="Inspect ${esc(actor.role || "orchestrator")} ${shortId(actor.instance_id || actor.worker_run_id)}" ${openAttr(type, actor.instance_id || actor.worker_run_id)}><circle class="satellite-pulse" cx="${sx}" cy="${sy}" r="4" style="animation-delay:${actorIndex * 0.6}s"/><circle class="satellite live" cx="${sx}" cy="${sy}" r="3"/><title>${esc(actor.task_summary || "Orchestrator")} · ${esc(humanize(actor.role || "orchestrator"))}</title></g>`;
        })
        .join("");
      return `<path class="link ${count ? "live" : ""}" d="${path}"/>${count ? `<path class="signal" d="${path}" style="animation-delay:-${index * 0.7}s"/>` : ""}${satellites}<g class="node-button ${count ? "online" : ""}" role="button" tabindex="0" aria-label="Inspect repository ${esc(repository.display_name)}" ${openAttr("repositories", repository.repository_id)}><circle class="node-disc" cx="${x}" cy="${y}" r="22"/><path d="m${x - 9} ${y - 5} 4-3h5l3 3h7v12h-19z" fill="none" stroke="${count ? "#b5cf80" : "#8e9da6"}" stroke-width="1.2"/><text class="node-label" x="${x}" y="${y + 37}" text-anchor="middle">${esc(repository.display_name.length > 22 ? repository.display_name.slice(0, 21) + "…" : repository.display_name)}</text><text class="node-sub" x="${x}" y="${y + 50}" text-anchor="middle">${count ? `${count} AGENT${count === 1 ? "" : "S"} CONNECTED` : "STANDBY"}</text></g>`;
    })
    .join("");
  return `<section class="panel">${panelHeader("Coordination topology", "network", '<span class="panel-kicker">LIVE NETWORK</span>')}<div class="graph-wrap"><div class="graph-caption"><span class="status-light"></span> ${repositories.length ? "REPOSITORY CONSTELLATION" : "AWAITING FIRST CONNECTION"}</div><div class="graph-corner">COORD SYS / RCN-01</div><svg class="topology" viewBox="0 0 800 410" role="group" aria-label="Interactive coordination topology. Select a repository or agent to inspect it."><circle class="orbit" cx="400" cy="205" r="162"/><ellipse class="orbit orbit-rotating" cx="400" cy="205" rx="285" ry="130"/><ellipse class="orbit" cx="400" cy="205" rx="320" ry="155"/><path d="M370 205h60m-30-30v60" stroke="#544638" stroke-width=".5"/>${nodes}<circle class="hub-glow" cx="400" cy="205" r="66"/><circle class="orbit orbit-inner" cx="400" cy="205" r="48"/><circle class="hub-circle" cx="400" cy="205" r="35"/><circle class="hub-ring" cx="400" cy="205" r="42"/><image href="/dashboard/assets/favicon.svg" x="380" y="185" width="40" height="40"/><text x="400" y="270" text-anchor="middle" class="hub-label">RACCOON HUB</text><text x="400" y="285" text-anchor="middle" class="node-sub">${demo ? "SIMULATED NODE" : connection === "offline" ? "UPLINK INTERRUPTED" : "COORDINATION ONLINE"}</text></svg></div><div class="graph-legend"><div class="legend-items"><span class="legend-item"><i class="legend-dot green"></i>Repository</span><span class="legend-item"><i class="legend-dot"></i>Live agent</span><span class="legend-item"><i class="legend-dot gray"></i>Signal route</span></div><a href="#agents">${state.repositories.filter(matches).length > 8 ? "8 shown · View all" : "Inspect network"} ↗</a></div></section>`;
}

function eventTarget(event) {
  const map = {
    work_item: ["work_items", "work_item_id"],
    worker_run: ["workers", "worker_run_id"],
    feature: ["features", "feature_id"],
    request: ["requests", "request_id"],
    message: ["messages", "message_id"],
  };
  const [collection, key] = map[event.entity_type] || ["events", "event_id"];
  return {
    collection,
    item: state[collection].find(
      (item) => String(item[key]) === String(event.entity_id),
    ),
  };
}
function eventRow(event) {
  const { collection, item } = eventTarget(event);
  const color =
    /failed/.test(event.kind) ||
    ["failed", "blocked"].includes(event.entity_status)
      ? "red"
      : /completed|answered/.test(event.kind)
        ? ""
        : /message|request/.test(event.kind)
          ? "blue"
          : "orange";
  const name = /message|request/.test(event.kind)
    ? "messages"
    : /completed|answered/.test(event.kind)
      ? "check"
      : /failed/.test(event.kind)
        ? "alert"
        : "events";
  return `<button class="event" ${openAttr(item ? collection : "events", item ? event.entity_id : String(event.event_id))}><span class="event-icon ${color}">${icon(name)}</span><span class="event-body"><span class="event-title">${esc(eventLabel(event.kind))}<span class="event-time">${relative(event.created_at)}</span></span><span class="event-summary">${esc(item?.title || item?.task_summary || item?.question_summary || item?.summary || shortId(event.entity_id))}</span><span class="event-meta">${esc(repoName(event.owner_repository_id || item?.repository_id || item?.sender_repository_id))}${event.entity_status ? ` <span>·</span> ${esc(humanize(event.entity_status))}` : ""}${event.recipient_count > 1 ? ` · ${event.recipient_count} recipients` : ""}</span></span></button>`;
}
function matchesEvent(event) {
  const target = eventTarget(event);
  return matches(event) || (target.item && matches(target.item));
}
function feed() {
  const events = state.events.filter(matchesEvent).slice(0, 5);
  return `<section class="panel feed-panel">${panelHeader("Live activity", "events", '<span class="panel-kicker">EVENT STREAM</span>')}<div class="feed-scroll">${events.length ? events.map(eventRow).join("") : empty("Quiet on the wire", "New coordination events will appear here as agents work.", true, "events")}</div><a class="feed-footer" href="#events">Open event log &nbsp; ↗</a></section>`;
}
function operationRow(item) {
  return `<button class="operation" ${openAttr("work_items", item.work_item_id)}><span class="operation-mark ${item.kind === "audit" ? "audit" : ""}">${icon(item.kind === "audit" ? "check" : "terminal")}</span><span class="operation-info"><span class="operation-title">${esc(item.title)}</span><span class="operation-sub"><span>${esc(repoName(item.owner_repository_id))}</span><span>·</span><span>${esc(humanize(item.kind))}</span></span></span>${badge(item.status)}</button>`;
}
function projectCards() {
  const projects = state.projects.filter(
    (item) => (!project || item.project_id === project) && matches(item),
  );
  return projects.length
    ? `<div class="section-header"><h2>Project orbits <span class="mono">/ ${projects.length}</span></h2><span class="panel-kicker">DURABLE WORKSPACES</span></div><div class="project-grid">${projects
        .map((item) => {
          const items = state.work_items.filter(
            (work) => work.project_id === item.project_id,
          );
          const completed = items.filter(
            (work) => work.status === "completed",
          ).length;
          const progress = items.length ? (completed / items.length) * 100 : 0;
          return `<button class="project-card" data-project="${esc(item.project_id)}"><div class="project-top"><span><span class="project-symbol">◈</span>${esc(item.display_name)}</span><span class="mono">↗</span></div><div class="project-sub"><span>${state.repositories.filter((repository) => repository.project_id === item.project_id).length} REPOSITORIES</span><span>${state.features.filter((feature) => feature.project_id === item.project_id && feature.status === "active").length} ACTIVE MISSIONS</span></div><div class="progress-track"><i style="width:${progress}%"></i></div><div class="progress-caption"><span>Work completed</span><span>${completed} / ${items.length}</span></div></button>`;
        })
        .join("")}</div>`
    : "";
}
function overview() {
  const operations = state.work_items
    .filter(
      (item) =>
        !["completed", "cancelled"].includes(item.status) && matches(item),
    )
    .slice(0, 5);
  return `${metricCards()}<div class="overview-grid">${topology()}${feed()}</div><div class="bottom-grid"><section class="panel">${panelHeader("Active operations", "terminal", link("Workboard", "missions"))}<div class="operations">${operations.length ? operations.map(operationRow).join("") : empty("No work in flight", "When agents create work cards, their progress will appear here.", true, "missions")}</div></section><section class="panel">${panelHeader("Repository uplinks", "globe", `<span class="panel-kicker">${state.repositories.length} REGISTERED</span>`)}<div class="repo-list">${
    state.repositories
      .filter(matches)
      .slice(0, 5)
      .map((repository) => {
        const online = state.instances.filter(
          (item) =>
            item.repository_id === repository.repository_id &&
            isLive(item, now()),
        ).length;
        return `<button class="repo-row" ${openAttr("repositories", repository.repository_id)}><span class="repo-avatar">${esc(repository.repository_key.slice(0, 2).toUpperCase())}</span><span class="repo-name">${esc(repository.display_name)}<small>${esc(projectName(repository.project_id))}</small></span><span class="repo-count">${online} live</span><i class="status-light ${online ? "" : "offline"}"></i></button>`;
      })
      .join("") ||
    empty(
      "No uplinks yet",
      "Repositories appear when an orchestrator connects.",
      true,
      "globe",
    )
  }</div></section></div>${projectCards()}${!state.projects.length && !query ? `<div class="empty-state"><h2>Your command center is ready.</h2><p>Connect agents to <code>/mcp</code> and their universe will come to life. Or take a look around with an isolated demo.</p><button class="primary-button" data-action="demo">Explore demo universe ${icon("arrow")}</button></div>` : ""}`;
}

function filters(options) {
  return `<div class="filter-group" role="group" aria-label="Filter by status or type">${options.map(([value, label]) => `<button class="filter-chip ${filter === value ? "active" : ""}" data-filter="${value}" aria-pressed="${filter === value}">${label}</button>`).join("")}</div>`;
}
function agentCard(actor, worker) {
  const role = worker ? actor.role : "orchestrator";
  const live = isLive(actor, now());
  const started = Date.parse(
    actor.activated_at || actor.started_at || actor.created_at,
  );
  const lease = Date.parse(actor.lease_expires_at);
  const remaining = Math.min(
    100,
    Math.max(0, ((lease - now()) / Math.max(1, lease - started)) * 100),
  );
  const status = effectiveStatus(actor, now());
  return `<button class="agent-card" ${openAttr(worker ? "workers" : "instances", worker ? actor.worker_run_id : actor.instance_id)}><div class="agent-card-top"><span class="agent-avatar ${role}">${icon(role === "auditor" ? "check" : role === "butler" ? "messages" : "agents")}</span>${badge(status)}</div><h3>${esc(humanize(role))} <span class="mono">/ ${shortId(actor.worker_run_id || actor.instance_id)}</span></h3><p class="agent-task">${esc(actor.task_summary || `Coordinating ${repoName(actor.repository_id)}`)}</p><div class="agent-details"><span>${esc(repoName(actor.repository_id))}</span><span>HB ${relative(actor.last_heartbeat_at)}</span><svg class="heartbeat-line ${live ? "live" : ""}" viewBox="0 0 60 20" aria-hidden="true"><path d="M0 10h14l4-7 5 14 5-10 4 3h28"/></svg></div><div class="lease-track"><i style="width:${live ? remaining : 0}%"></i></div></button>`;
}
function agents() {
  const actors = [
    ...state.instances.map((item) => ({ ...item, worker: false })),
    ...state.workers.map((item) => ({ ...item, worker: true })),
  ]
    .filter(
      (item) =>
        matches(item) &&
        (filter === "all" ||
          (filter === "live" && isLive(item, now())) ||
          (item.role || "orchestrator") === filter),
    )
    .sort((a, b) => Number(isLive(b, now())) - Number(isLive(a, now())));
  return `${metricCards()}<div class="toolbar">${filters([
    ["all", "All agents"],
    ["live", "Connected"],
    ["orchestrator", "Orchestrators"],
    ["executor", "Executors"],
    ["auditor", "Auditors"],
    ["butler", "Butlers"],
  ])}<span class="view-note">${actors.length} ACTORS · LEASE-AWARE PRESENCE</span></div>${actors.length ? `<div class="agent-grid">${actors.map((item) => agentCard(item, item.worker)).join("")}</div>` : empty("No agents in this orbit", "Try another filter or connect an orchestrator to this project.")}`;
}
function workCard(item) {
  const dependencies = item.dependency_ids || [];
  const unmet = dependencies.filter(
    (id) =>
      !state.work_items.some(
        (work) => work.work_item_id === id && work.status === "completed",
      ),
  ).length;
  return `<button class="task-card" ${openAttr("work_items", item.work_item_id)}><div class="task-type"><span>${esc(item.kind)}</span><span>P${item.priority}</span></div><h3>${esc(item.title)}</h3>${item.summary ? `<p>${esc(item.summary)}</p>` : ""}${badge(item.verification_status || item.status)}${dependencies.length ? `<div class="task-deps">⌁ ${dependencies.length} dependencies${unmet ? ` · ${unmet} unresolved or outside snapshot` : " · all complete"}</div>` : ""}<div class="task-footer"><span>${esc(repoName(item.owner_repository_id))}</span><span>REV ${item.revision}</span></div></button>`;
}
function missions() {
  const features = state.features.filter(matches);
  const cards = state.work_items.filter(
    (item) =>
      matches(item) &&
      (!featureFilter || item.feature_id === featureFilter) &&
      (filter === "all" || item.kind === filter),
  );
  const columns = [
    ["Queued", ["planned", "ready"]],
    ["In motion", ["claimed", "in_progress"]],
    ["Needs attention", ["blocked", "failed"]],
    ["Finished", ["completed", "cancelled"]],
  ];
  return `<div class="mission-strip">${features
    .map((feature) => {
      const items = state.work_items.filter(
        (item) => item.feature_id === feature.feature_id,
      );
      const completed = items.filter(
        (item) => item.status === "completed",
      ).length;
      return `<button class="mission-card" ${openAttr("features", feature.feature_id)}>${badge(feature.status)}<h3>${esc(feature.title)}</h3><div class="progress-track"><i style="width:${items.length ? (completed / items.length) * 100 : 0}%"></i></div><div class="progress-caption"><span>${esc(repoName(feature.coordinator_repository_id))}</span><span>${completed}/${items.length} complete</span></div></button>`;
    })
    .join("")}</div><div class="toolbar">${filters([
    ["all", "All work"],
    ["implementation", "Implementation"],
    ["audit", "Audits"],
    ["remediation", "Remediation"],
  ])}<select class="filter-select" id="feature-filter" aria-label="Filter by mission"><option value="">All missions</option>${state.features.map((item) => `<option value="${esc(item.feature_id)}" ${featureFilter === item.feature_id ? "selected" : ""}>${esc(item.title)}</option>`).join("")}</select></div><div class="kanban">${columns
    .map(([label, statuses]) => {
      const items = cards.filter((item) => statuses.includes(item.status));
      return `<section class="kanban-column"><h2 class="column-header"><span><i></i>${label}</span><span class="column-count">${items.length}</span></h2>${items.map(workCard).join("") || '<div class="empty-column">CLEAR SECTOR</div>'}</section>`;
    })
    .join("")}</div>${requestTable()}`;
}

function requestTable() {
  const requests = state.requests.filter(matches);
  return `<section class="panel request-block">${panelHeader("Cross-repository requests", "messages", `<span class="panel-kicker">${requests.length} REQUESTS</span>`)}${requests.length ? `<div class="table-wrap"><table class="data-table"><thead><tr><th>QUESTION</th><th>ROUTE</th><th>STATUS</th><th>UPDATED</th></tr></thead><tbody>${requests.map((item) => `<tr><td class="table-summary"><button class="row-button" ${openAttr("requests", item.request_id)}>${esc(item.question_summary)}</button><small>${esc(humanize(item.handler_requirement))}</small></td><td><span class="mono">${esc(repoName(item.requester_repository_id))} → ${esc(repoName(item.target_repository_id))}</span></td><td>${badge(item.status)}</td><td class="mono">${relative(item.updated_at)}</td></tr>`).join("")}</tbody></table></div>` : empty("No open conversations", "Questions between repositories and their answers will appear here.", true, "messages")}</section>`;
}
function messages() {
  const items = state.messages.filter(
    (item) =>
      matches(item) &&
      (filter === "all" ||
        item.kind === filter ||
        (filter === "pending" && item.pending_deliveries > 0)),
  );
  return `<div class="toolbar">${filters([
    ["all", "All transmissions"],
    ["report", "Reports"],
    ["announcement", "Announcements"],
    ["command", "Commands"],
    ["pending", "Awaiting acknowledgement"],
  ])}<span class="view-note">${items.length} ENVELOPES</span></div><section class="panel">${panelHeader("Repository transmissions", "messages", '<span class="panel-kicker">IMMUTABLE MESSAGE HISTORY</span>')}${items.length ? `<div class="table-wrap"><table class="data-table"><thead><tr><th>TRANSMISSION</th><th>FROM → TO</th><th>DELIVERY</th><th>SENT</th></tr></thead><tbody>${items.map((item) => `<tr><td class="table-summary"><button class="row-button" ${openAttr("messages", item.message_id)}>${esc(item.summary)}</button><small>${esc(humanize(item.kind))} · Thread ${shortId(item.thread_id)}${item.artifact_id ? " · Artifact attached" : ""}</small></td><td class="mono">${esc(repoName(item.sender_repository_id))}<br>→ ${esc((item.target_repository_ids || []).map(repoName).join(", ") || "—")}</td><td>${badge(item.pending_deliveries ? "pending" : "completed")}</td><td class="mono">${relative(item.created_at)}</td></tr>`).join("")}</tbody></table></div>` : empty("The channel is quiet", "Reports, announcements and commands sent by your agents appear here.", false, "messages")}</section>${requestTable()}`;
}
function artifacts() {
  const items = state.artifacts.filter(
    (item) => matches(item) && (filter === "all" || item.kind === filter),
  );
  return `<div class="toolbar">${filters([
    ["all", "All artifacts"],
    ["implementation_report", "Implementation"],
    ["audit_report", "Audits"],
    ["api_contract", "API contracts"],
    ["answer", "Answers"],
    ["note", "Notes"],
  ])}<span class="view-note">${items.length} ARTIFACTS</span></div>${items.length ? `<div class="report-grid">${items.map((item) => `<button class="report-card" ${openAttr("artifacts", item.artifact_id)}><div class="report-card-top"><span class="report-icon">${icon("artifacts")}</span><span>${esc(item.content_type)}</span></div><span class="eyebrow">${esc(humanize(item.kind))}</span><h3>${esc(item.title)}</h3><div class="report-card-bottom"><span>${esc(repoName(item.producer_repository_id))}</span><span>${relative(item.created_at)}</span><span>↗</span></div></button>`).join("")}</div>` : empty("Knowledge is on its way", "Full implementation reports and audit verdicts will be available here.", false, "artifacts")}`;
}
function events() {
  const items = state.events.filter(
    (item) =>
      matchesEvent(item) && (filter === "all" || item.entity_type === filter),
  );
  return `<div class="toolbar">${filters([
    ["all", "All events"],
    ["worker_run", "Workers"],
    ["work_item", "Work cards"],
    ["feature", "Missions"],
    ["message", "Messages"],
    ["request", "Requests"],
  ])}<span class="view-note">${items.length} CHANGES · NEWEST FIRST</span></div><section class="panel">${panelHeader("Coordination timeline", "events", '<span class="panel-kicker">FAN-OUT COPIES MERGED</span>')}<div class="event-log">${items.map(eventRow).join("") || empty("Nothing has moved yet", "This timeline fills automatically as agents coordinate work.", false, "events")}</div></section>`;
}
function render(force = false) {
  if (!snapshot) return;
  state = scopedData(snapshot, project);
  updateChrome();
  if (inspectorKey && $("#inspector").open) {
    const [type, id] = inspectorKey.split(":");
    const item = entity(type, id);
    const signature = JSON.stringify(item);
    if (item && type !== "artifacts" && signature !== inspectorSignature) {
      const scroll = $("#inspector").scrollTop;
      $("#inspector-content").innerHTML = detailContents(type, item);
      $("#inspector").scrollTop = scroll;
      inspectorSignature = signature;
    }
  }
  // Ignore the snapshot timestamp; unchanged state keeps SVG and CSS animations running.
  const signature = JSON.stringify([
    view,
    project,
    query,
    filter,
    featureFilter,
    connection,
    demo,
    [...state.instances, ...state.workers].map((actor) => isLive(actor, now())),
    ...[
      "projects",
      "repositories",
      "instances",
      "features",
      "work_items",
      "workers",
      "requests",
      "messages",
      "artifacts",
      "events",
    ].map((key) => state[key]),
  ]);
  if (!force && signature === lastSignature) return;
  lastSignature = signature;
  const clipped = Object.entries(snapshot.totals || {}).filter(
    ([, total]) => total > snapshot.limit,
  );
  const limitNotice = clipped.length
    ? `<div class="limit-notice">Showing up to ${snapshot.limit} records per collection. ${clipped.map(([key, total]) => `${esc(humanize(key))}: ${total}`).join(" · ")}. Select a project to narrow the snapshot. Counters reflect loaded records.</div>`
    : "";
  const views = { overview, agents, missions, messages, artifacts, events };
  $("#main").innerHTML =
    `${pageIntro()}${limitNotice}<div class="view-enter">${views[view]()}</div>`;
}

async function sync() {
  clearTimeout(timer);
  if (demo) return;
  request?.abort();
  const controller = new AbortController();
  request = controller;
  const timeout = setTimeout(() => controller.abort(), 8000);
  try {
    const response = await fetch(
      `/api/dashboard/snapshot${project ? `?project_id=${encodeURIComponent(project)}` : ""}`,
      { signal: controller.signal, cache: "no-store" },
    );
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    const received = await response.json();
    if (controller !== request || demo) return;
    snapshot = received;
    serverOffset = Date.parse(received.server_time) - Date.now();
    connection = "online";
    lastSync = received.server_time;
    if (
      project &&
      !received.projects.some((item) => item.project_id === project)
    )
      project = "";
    render();
  } catch (error) {
    if (controller !== request || demo) return;
    connection = "offline";
    updateChrome();
    if (snapshot) render();
    else
      $("#main").innerHTML =
        `${pageIntro()}${empty("Waiting for the server", "The coordination server is unreachable. This page reconnects automatically. Start the updated Raccoon binary, then try Sync now.", false, "network")}`;
  } finally {
    clearTimeout(timeout);
    if (controller === request && !demo && !document.hidden)
      timer = setTimeout(sync, 2000);
  }
}

const ids = {
  projects: "project_id",
  repositories: "repository_id",
  instances: "instance_id",
  features: "feature_id",
  work_items: "work_item_id",
  workers: "worker_run_id",
  requests: "request_id",
  messages: "message_id",
  artifacts: "artifact_id",
  events: "event_id",
};
function entity(type, id) {
  return state[type]?.find((item) => String(item[ids[type]]) === String(id));
}
function detailLink(type, id, subtitle = "") {
  if (!id) return "";
  const item = entity(type, id);
  const title =
    item?.title ||
    item?.task_summary ||
    item?.question_summary ||
    item?.summary ||
    item?.display_name ||
    `${humanize(type)} / ${shortId(id)}`;
  return `<button class="detail-link" ${openAttr(type, id)}>${icon(type === "work_items" ? "missions" : type)}<span>${esc(title)}${subtitle ? `<small>${esc(subtitle)}</small>` : ""}</span><span class="arrow">↗</span></button>`;
}
function detailFields(item) {
  const skip = new Set([
    "title",
    "summary",
    "task_summary",
    "question_summary",
    "content",
    "capabilities",
    "context_refs",
    "dependency_ids",
    "audit_scope_work_item_ids",
    "target_repository_ids",
    "audit_outcomes",
  ]);
  return `<div class="detail-grid">${Object.entries(item)
    .filter(
      ([key, value]) =>
        !skip.has(key) &&
        value !== null &&
        !key.endsWith("_id") &&
        key !== "event_id",
    )
    .map(
      ([key, value]) =>
        `<div class="detail-field"><label>${esc(humanize(key))}</label><span>${key === "status" || key === "verification_status" ? badge(value) : key.endsWith("_at") ? `${esc(new Date(value).toLocaleString())}` : esc(typeof value === "object" ? JSON.stringify(value) : value)}</span></div>`,
    )
    .join("")}</div>`;
}
function detailContents(type, item) {
  let html = `<p class="inspector-summary">${esc(item.summary || item.task_summary || item.question_summary || (type === "repositories" ? item.canonical_path : ""))}</p>`;
  if (type === "instances" || type === "workers")
    html += `<div style="margin-bottom:20px">${badge(effectiveStatus(item, now()))} <span class="mono">Lease ${isLive(item, now()) ? "valid" : "inactive"} at server time</span></div>`;
  html += detailFields(item);
  if (
    item.repository_id ||
    item.owner_repository_id ||
    item.producer_repository_id ||
    item.sender_repository_id
  )
    html += `<h3 class="detail-heading">REPOSITORY</h3>${detailLink("repositories", item.repository_id || item.owner_repository_id || item.producer_repository_id || item.sender_repository_id)}`;
  if (item.feature_id && type !== "features")
    html += `<h3 class="detail-heading">MISSION</h3>${detailLink("features", item.feature_id)}`;
  if (item.work_item_id && type !== "work_items")
    html += `<h3 class="detail-heading">WORK ITEM</h3>${detailLink("work_items", item.work_item_id)}`;
  if (type === "repositories") {
    html += `<h3 class="detail-heading">AGENTS IN THIS REPOSITORY</h3>${state.instances
      .filter((actor) => actor.repository_id === item.repository_id)
      .map((actor) =>
        detailLink(
          "instances",
          actor.instance_id,
          `${effectiveStatus(actor, now())} · orchestrator`,
        ),
      )
      .join("")}${state.workers
      .filter((actor) => actor.repository_id === item.repository_id)
      .map((actor) =>
        detailLink(
          "workers",
          actor.worker_run_id,
          `${effectiveStatus(actor, now())} · ${actor.role}`,
        ),
      )
      .join("")}`;
  }
  if (type === "features")
    html += `<h3 class="detail-heading">WORK CARDS</h3>${
      state.work_items
        .filter((work) => work.feature_id === item.feature_id)
        .map((work) =>
          detailLink(
            "work_items",
            work.work_item_id,
            `${humanize(work.status)} · ${repoName(work.owner_repository_id)}`,
          ),
        )
        .join("") || '<p class="mono">No work cards in this snapshot.</p>'
    }`;
  if (type === "instances")
    html += `<h3 class="detail-heading">CHILD WORKERS</h3>${
      state.workers
        .filter((worker) => worker.parent_instance_id === item.instance_id)
        .map((worker) =>
          detailLink(
            "workers",
            worker.worker_run_id,
            `${worker.role} · ${effectiveStatus(worker, now())}`,
          ),
        )
        .join("") ||
      '<p class="mono">No tracked child workers in this snapshot.</p>'
    }`;
  if (type === "work_items") {
    html += `<h3 class="detail-heading">EXECUTION LINEAGE</h3>${
      state.workers
        .filter((worker) => worker.work_item_id === item.work_item_id)
        .map((worker) =>
          detailLink(
            "workers",
            worker.worker_run_id,
            `${worker.role} · ${effectiveStatus(worker, now())}`,
          ),
        )
        .join("") || '<p class="mono">No worker attempts in this snapshot.</p>'
    }`;
    if (item.dependency_ids?.length)
      html += `<h3 class="detail-heading">DEPENDENCIES</h3>${item.dependency_ids.map((id) => detailLink("work_items", id)).join("")}`;
    if (item.audit_scope_work_item_ids?.length)
      html += `<h3 class="detail-heading">AUDIT SCOPE</h3>${item.audit_scope_work_item_ids.map((id) => detailLink("work_items", id)).join("")}`;
    html += `<h3 class="detail-heading">LINKED REPORTS</h3>${detailLink("artifacts", item.implementation_artifact_id, "Implementation report")}${detailLink("artifacts", item.latest_audit_artifact_id, "Latest audit")}`;
    if (item.parent_work_item_id)
      html += detailLink(
        "work_items",
        item.parent_work_item_id,
        "Parent work card",
      );
  }
  if (type === "messages") {
    html += `<h3 class="detail-heading">RECIPIENTS</h3>${(item.target_repository_ids || []).map((id) => detailLink("repositories", id)).join("")}`;
    if (item.artifact_id)
      html += `<h3 class="detail-heading">ATTACHED ARTIFACT</h3>${detailLink("artifacts", item.artifact_id)}`;
    const thread = state.messages.filter(
      (message) =>
        message.thread_id === item.thread_id &&
        message.message_id !== item.message_id,
    );
    if (thread.length)
      html += `<h3 class="detail-heading">THREAD</h3>${thread.map((message) => detailLink("messages", message.message_id)).join("")}`;
  }
  if (type === "requests")
    html += `<h3 class="detail-heading">REQUEST ROUTE</h3>${detailLink("repositories", item.requester_repository_id, "Requester")}${detailLink("repositories", item.target_repository_id, "Target")}<h3 class="detail-heading">QUESTION & ANSWER</h3>${detailLink("artifacts", item.question_artifact_id, "Full question")}${detailLink("messages", item.answer_message_id, "Answer")}${detailLink("workers", item.claimed_by_worker_run_id, "Handling worker")}`;
  if (type === "workers" && item.parent_instance_id)
    html += `<h3 class="detail-heading">PARENT ORCHESTRATOR</h3>${detailLink("instances", item.parent_instance_id)}`;
  if (type === "events") {
    const target = eventTarget(item);
    if (target.item)
      html += `<h3 class="detail-heading">CHANGED OBJECT</h3>${detailLink(target.collection, item.entity_id)}`;
  }
  if (item.addresses_audit_run_id)
    html += `<h3 class="detail-heading">REMEDIATION FOR</h3>${detailLink("workers", item.addresses_audit_run_id)}`;
  if (
    item.context_refs?.length ||
    (item.capabilities && Object.keys(item.capabilities).length)
  )
    html += `<h3 class="detail-heading">${item.capabilities ? "CAPABILITIES" : "CONTEXT REFERENCES"}</h3><pre class="content-body json-body">${esc(JSON.stringify(item.capabilities || item.context_refs, null, 2))}</pre>`;
  if (type === "artifacts")
    html +=
      '<h3 class="detail-heading">ARTIFACT CONTENT</h3><div id="artifact-body" class="mono">Loading full report…</div>';
  return (
    html +
    `<div class="detail-id"><code>${esc(item[ids[type]])}</code><button class="text-button" data-copy="${esc(item[ids[type]])}">Copy ID</button></div>`
  );
}
async function inspect(type, id) {
  inspectorRequest?.abort();
  const item = entity(type, id);
  const dialog = $("#inspector");
  inspectorKey = `${type}:${id}`;
  inspectorSignature = JSON.stringify(item);
  $("#inspector-type").textContent =
    `${humanize(type === "instances" ? "orchestrator" : type === "workers" ? "worker run" : type === "work_items" ? "work card" : type)} / ${shortId(id)}`;
  $("#inspector-title").textContent =
    item?.title ||
    item?.display_name ||
    item?.task_summary ||
    item?.question_summary ||
    item?.summary ||
    (type === "events" && item
      ? eventLabel(item.kind)
      : `${humanize(type)} / ${shortId(id)}`);
  $("#inspector-content").innerHTML = item
    ? detailContents(type, item)
    : type === "artifacts"
      ? '<div id="artifact-body" class="mono">Loading report…</div>'
      : empty(
          "Outside the current snapshot",
          "This record may be older than the snapshot limit. Try selecting its project to narrow the view.",
          true,
        );
  if (!dialog.open) dialog.showModal();
  dialog.scrollTop = 0;
  if (type !== "artifacts") return;
  const key = inspectorKey;
  const controller = new AbortController();
  inspectorRequest = controller;
  try {
    let artifact;
    if (demo) artifact = demoData.artifactBodies[id];
    else {
      const response = await fetch(
        `/api/dashboard/artifacts/${encodeURIComponent(id)}`,
        { signal: controller.signal, cache: "no-store" },
      );
      if (!response.ok) throw new Error("Cannot load report");
      artifact = await response.json();
    }
    if (key !== inspectorKey || !dialog.open) return;
    if (!artifact) throw new Error("Artifact not found");
    $("#inspector-title").textContent = artifact.title;
    $("#artifact-body").innerHTML =
      `${(artifact.audit_outcomes || []).map((outcome) => `<div class="outcome ${esc(outcome.verdict)}">${badge(outcome.verdict)}<p>${esc(outcome.summary)}</p>${detailLink("work_items", outcome.scoped_work_item_id)}</div>`).join("")}<div class="inspector-tools"><button class="outline-button" data-action="download-artifact">Download report ↓</button></div><pre class="content-body" id="report-content"></pre>`;
    $("#report-content").textContent = artifact.content;
    $("#artifact-body")._artifact = artifact;
  } catch (error) {
    if (controller.signal.aborted || key !== inspectorKey) return;
    $("#artifact-body").innerHTML =
      '<p class="inspector-summary">Report unavailable. The server may be offline or the artifact was removed.</p><button class="outline-button" data-action="retry-artifact">Retry</button>';
  }
}

function toast(message) {
  clearTimeout(toastTimer);
  $("#toast").textContent = message;
  $("#toast").hidden = false;
  toastTimer = setTimeout(() => {
    $("#toast").hidden = true;
  }, 3000);
}
async function toggleDemo() {
  closeProjectPicker();
  request?.abort();
  request = null;
  clearTimeout(timer);
  $("#inspector").close();
  demo = !demo;
  project = "";
  filter = "all";
  featureFilter = "";
  query = "";
  $("#search").value = "";
  window.scrollTo({ top: 0 });
  if (demo) {
    const { createDemo } = await import("./demo.js");
    demoData = createDemo();
    snapshot = demoData.snapshot;
    serverOffset = 0;
    lastSync = new Date().toISOString();
    render(true);
  } else {
    snapshot = null;
    lastSync = null;
    state = scopedData();
    connection = "connecting";
    $("#main").innerHTML =
      '<div class="loading-screen"><div class="loading-orbit"></div><h1>Returning to live orbit</h1></div>';
    updateChrome();
    await sync();
  }
}
function chooseProject(id) {
  closeProjectPicker();
  project = id;
  updateProjectPicker();
  featureFilter = "";
  $("#inspector").close();
  if (demo) render(true);
  else {
    // Clear old project data immediately. An aborted response cannot replace the new scope.
    request?.abort();
    request = null;
    $("#main").innerHTML =
      '<div class="loading-screen"><div class="loading-orbit"></div><h1>Switching orbit</h1></div>';
    sync();
  }
}
document.addEventListener("click", async (event) => {
  const target = event.target.closest(
    "[data-open],[data-filter],[data-project],[data-action],[data-copy]",
  );
  if (!target) return;
  if (target.dataset.open) {
    inspect(target.dataset.open, target.dataset.id);
    return;
  }
  if (target.dataset.filter) {
    filter = target.dataset.filter;
    render(true);
    return;
  }
  if (target.dataset.project) {
    chooseProject(target.dataset.project);
    return;
  }
  if (target.dataset.copy) {
    try {
      await navigator.clipboard.writeText(target.dataset.copy);
      toast("ID copied to clipboard");
    } catch {
      toast("Clipboard unavailable. Select the ID to copy it.");
    }
    return;
  }
  if (target.dataset.action === "demo" || target.dataset.action === "exit-demo")
    toggleDemo();
  if (target.dataset.action === "refresh") {
    if (demo)
      toast("Demo is illustrative. Switch to the live server for telemetry.");
    else {
      await sync();
      if (connection === "online") toast("Coordination state synchronized");
    }
  }
  if (target.dataset.action === "retry-artifact") {
    const [type, id] = inspectorKey.split(":");
    inspect(type, id);
  }
  if (target.dataset.action === "download-artifact") {
    const artifact = $("#artifact-body")._artifact;
    const url = URL.createObjectURL(
      new Blob([artifact.content], { type: "text/plain;charset=utf-8" }),
    );
    const anchor = document.createElement("a");
    anchor.href = url;
    anchor.download = `${artifact.title.replace(/[^a-z0-9а-яё_-]/gi, "_").slice(0, 100)}.${artifact.content_type.includes("markdown") ? "md" : "txt"}`;
    anchor.click();
    setTimeout(() => URL.revokeObjectURL(url), 1000);
  }
});
document.addEventListener("keydown", (event) => {
  const node = event.target;
  if (
    event.key === "/" &&
    !["INPUT", "TEXTAREA", "SELECT"].includes(node.tagName) &&
    !$("#inspector").open
  ) {
    event.preventDefault();
    $("#search").focus();
  }
  if (
    (event.key === "Enter" || event.key === " ") &&
    node.matches(".node-button")
  ) {
    event.preventDefault();
    inspect(node.dataset.open, node.dataset.id);
  }
  if (event.key === "Escape") {
    $("#sidebar").classList.remove("open");
    $("#menu-toggle").setAttribute("aria-expanded", "false");
  }
});
$("#project").addEventListener("click", () => {
  if ($("#project-menu").hidden) openProjectPicker();
  else closeProjectPicker();
});
$("#project").addEventListener("keydown", (event) => {
  if (["ArrowDown", "ArrowUp"].includes(event.key)) {
    event.preventDefault();
    openProjectPicker(event.key === "ArrowUp");
  }
});
$("#project-search").addEventListener("input", () => {
  activeProjectIndex = 0;
  updateProjectPicker();
});
$("#project-search").addEventListener("keydown", (event) => {
  const choices = projectChoices();
  if (
    ["ArrowDown", "ArrowUp", "Home", "End", "Enter", "Escape"].includes(
      event.key,
    )
  ) {
    event.preventDefault();
    event.stopPropagation();
  } else return;
  if (event.key === "Escape") return closeProjectPicker(true);
  if (event.key === "Enter") {
    const choice = choices[activeProjectIndex];
    if (choice) {
      chooseProject(choice.project_id);
      $("#project").focus();
    }
    return;
  }
  if (!choices.length) return;
  if (event.key === "Home") activeProjectIndex = 0;
  else if (event.key === "End") activeProjectIndex = choices.length - 1;
  else
    activeProjectIndex =
      (activeProjectIndex +
        (event.key === "ArrowDown" ? 1 : -1) +
        choices.length) %
      choices.length;
  updateProjectPicker();
  $(`#project-option-${activeProjectIndex}`)?.scrollIntoView({
    block: "nearest",
  });
});
$("#project-options").addEventListener("click", (event) => {
  const option = event.target.closest("[data-workspace]");
  if (option) {
    chooseProject(option.dataset.workspace);
    $("#project").focus();
  }
});
document.addEventListener("pointerdown", (event) => {
  if (!$("#project-picker").contains(event.target)) closeProjectPicker();
});
$("#project-picker").addEventListener("focusout", (event) => {
  if (!$("#project-picker").contains(event.relatedTarget)) closeProjectPicker();
});
$("#search").addEventListener("input", (event) => {
  query = event.target.value;
  render(true);
});
document.addEventListener("change", (event) => {
  if (event.target.id === "feature-filter") {
    featureFilter = event.target.value;
    render(true);
  }
});
window.addEventListener("hashchange", () => {
  const next = location.hash.slice(1);
  if (next === "main") return;
  view = navigation.some((item) => item[0] === next) ? next : "overview";
  filter = "all";
  query = "";
  featureFilter = "";
  $("#search").value = "";
  $("#sidebar").classList.remove("open");
  $("#menu-toggle").setAttribute("aria-expanded", "false");
  $("#inspector").close();
  render(true);
  window.scrollTo({ top: 0 });
});
$("#close-inspector").addEventListener("click", () => $("#inspector").close());
$("#inspector").addEventListener("close", () => {
  inspectorKey = null;
  inspectorRequest?.abort();
});
$("#inspector").addEventListener("click", (event) => {
  if (
    event.target === $("#inspector") &&
    event.clientX < $("#inspector").getBoundingClientRect().left
  )
    $("#inspector").close();
});
$("#demo-toggle").addEventListener("click", toggleDemo);
$("#menu-toggle").addEventListener("click", () => {
  const open = $("#sidebar").classList.toggle("open");
  $("#menu-toggle").setAttribute("aria-expanded", String(open));
});
function updateMotion() {
  document.body.classList.toggle("reduce-motion", motionOff);
  $("#motion-toggle").innerHTML = icon(motionOff ? "motion" : "pause");
  $("#motion-toggle").setAttribute("aria-pressed", String(motionOff));
  $("#motion-toggle").title = motionOff
    ? "Enable animations"
    : "Pause animations";
}
$("#motion-toggle").addEventListener("click", () => {
  motionOff = !motionOff;
  try {
    localStorage.setItem("raccoon-motion", motionOff ? "off" : "on");
  } catch {
    /* Optional preference. */
  }
  updateMotion();
});
document.addEventListener("visibilitychange", () => {
  if (document.hidden) {
    clearTimeout(timer);
    request?.abort();
    request = null;
  } else if (!demo) sync();
});
window.addEventListener("online", () => {
  if (!demo) sync();
});
setInterval(() => {
  $("#clock").textContent =
    `${new Date().toLocaleTimeString("en-GB")} / LOCAL TIME`;
  for (const element of document.querySelectorAll("[data-time]"))
    element.textContent = age(element.dataset.time);
  if (lastSync)
    $("#sync-time").textContent = `SYNCED ${age(lastSync)} · 2s INTERVAL`;
}, 1000);
$("#search-icon").innerHTML = icon("search");
updateMotion();
updateChrome();
sync();
