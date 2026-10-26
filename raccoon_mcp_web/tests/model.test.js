import test from "node:test";
import assert from "node:assert/strict";
import {
  isLive,
  effectiveStatus,
  scopedData,
  metrics,
  eventLabel,
  escapeHTML,
} from "../model.js";

test("a created worker is assigned but not yet connected", () => {
  const now = Date.parse("2026-10-26T10:00:00Z");
  const worker = {
    status: "created",
    lease_expires_at: "2026-10-26T10:01:00Z",
  };
  assert.equal(isLive(worker, now), false);
  assert.equal(effectiveStatus(worker, now), "created");
  assert.equal(effectiveStatus(worker, now + 60001), "expired");
});

test("live actors require an unexpired lease and a live status", () => {
  const now = Date.parse("2026-10-26T10:00:00Z");
  assert.equal(
    isLive({ status: "online", lease_expires_at: "2026-10-26T10:01:00Z" }, now),
    true,
  );
  assert.equal(
    isLive({ status: "active", lease_expires_at: "2026-10-26T09:59:00Z" }, now),
    false,
  );
  assert.equal(
    isLive(
      { status: "completed", lease_expires_at: "2026-10-26T11:00:00Z" },
      now,
    ),
    false,
  );
  assert.equal(
    isLive({ status: "online", lease_expires_at: "invalid" }, now),
    false,
  );
});

test("project filtering includes related workers and keeps projects discoverable", () => {
  const snapshot = {
    projects: [{ project_id: "a" }, { project_id: "b" }],
    repositories: [{ project_id: "a" }, { project_id: "b" }],
    workers: [{ project_id: "b" }],
  };
  assert.equal(scopedData(snapshot, "a").repositories.length, 1);
  assert.equal(scopedData(snapshot, "a").workers.length, 0);
  assert.equal(scopedData(snapshot, "a").projects.length, 2);
  assert.equal(scopedData(snapshot, "").repositories.length, 2);
});

test("completed work awaiting audit is not counted as verified", () => {
  const counts = metrics(
    scopedData({
      work_items: [
        { status: "completed", verification_status: "pending_audit" },
        { status: "completed", verification_status: "verified" },
        { status: "blocked" },
        { status: "cancelled" },
      ],
    }),
  );
  assert.equal(counts.completed, 2);
  assert.equal(counts.verified, 1);
  assert.equal(counts.attention, 1);
});

test("server strings are escaped and event labels tolerate future kinds", () => {
  assert.equal(
    escapeHTML('<img src=x onerror="x">'),
    "&lt;img src=x onerror=&quot;x&quot;&gt;",
  );
  assert.equal(eventLabel("worker_run_active"), "Worker activated");
  assert.equal(eventLabel("future_event"), "Future event");
});
