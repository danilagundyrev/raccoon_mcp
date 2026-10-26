export const collections = [
  "repositories",
  "instances",
  "features",
  "work_items",
  "workers",
  "requests",
  "messages",
  "artifacts",
  "events",
];
export const escapeHTML = (value) =>
  String(value ?? "").replace(
    /[&<>"']/g,
    (char) =>
      ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[
        char
      ],
  );
export const humanize = (value) =>
  String(value ?? "")
    .replaceAll("_", " ")
    .replace(/^./, (char) => char.toUpperCase());
export const isLive = (actor, now = Date.now()) =>
  ["online", "active"].includes(actor.status) &&
  Date.parse(actor.lease_expires_at) > now;
export function scopedData(snapshot = {}, projectId = "") {
  const scoped = { ...snapshot, projects: snapshot.projects || [] };
  for (const collection of collections)
    scoped[collection] = (snapshot[collection] || []).filter(
      (item) => !projectId || item.project_id === projectId,
    );
  return scoped;
}
export function metrics(state, now = Date.now()) {
  return {
    online: state.instances.filter((actor) => isLive(actor, now)).length,
    workers: state.workers.filter((actor) => isLive(actor, now)).length,
    running: state.work_items.filter((item) =>
      ["claimed", "in_progress"].includes(item.status),
    ).length,
    completed: state.work_items.filter((item) => item.status === "completed")
      .length,
    verified: state.work_items.filter(
      (item) => item.verification_status === "verified",
    ).length,
    attention:
      state.work_items.filter(
        (item) =>
          ["blocked", "failed"].includes(item.status) ||
          item.verification_status === "verification_blocked",
      ).length +
      state.requests.filter((item) => item.status === "failed").length,
    requests: state.requests.filter((item) =>
      ["pending", "claimed"].includes(item.status),
    ).length,
  };
}
const labels = {
  message_available: "Message delivered",
  worker_run_created: "Worker assigned",
  worker_run_active: "Worker activated",
  worker_run_completed: "Worker completed",
  worker_run_failed: "Worker failed",
  request_available: "Request opened",
  request_claimed: "Request claimed",
  request_answered: "Request answered",
  request_failed: "Request failed",
  feature_created: "Mission created",
  feature_updated: "Mission updated",
  feature_completed: "Mission completed",
  feature_cancelled: "Mission cancelled",
  work_item_created: "Work item created",
  work_item_updated: "Work item updated",
  audit_depth_increased: "Audit depth increased",
};
export const eventLabel = (kind) => labels[kind] || humanize(kind);
export function effectiveStatus(actor, now = Date.now()) {
  if (
    ["online", "active", "created"].includes(actor.status) &&
    !(Date.parse(actor.lease_expires_at) > now)
  )
    return "expired";
  return actor.status;
}
