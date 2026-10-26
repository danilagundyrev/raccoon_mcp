// Browser-only fixtures. Never sent to an MCP tool or persisted to the server.
export function createDemo() {
  const ago = (minutes) => new Date(Date.now() - minutes * 60000).toISOString();
  const future = (minutes) =>
    new Date(Date.now() + minutes * 60000).toISOString();
  const projects = [
    {
      project_id: "demo-orbit",
      project_key: "orbit",
      display_name: "Orbit Commerce",
      created_at: ago(720),
      updated_at: ago(2),
    },
    {
      project_id: "demo-atlas",
      project_key: "atlas",
      display_name: "Atlas Intelligence",
      created_at: ago(540),
      updated_at: ago(8),
    },
  ];
  const repositories = [
    ["api", "Core API", "demo-orbit"],
    ["web", "Web Client", "demo-orbit"],
    ["mobile", "Mobile App", "demo-orbit"],
    ["infra", "Infrastructure", "demo-orbit"],
    ["engine", "Intelligence Engine", "demo-atlas"],
  ].map(([key, name, project_id]) => ({
    repository_id: `repo-${key}`,
    repository_key: key,
    display_name: name,
    project_id,
    canonical_path: `~/workspace/${key}`,
    created_at: ago(500),
    updated_at: ago(2),
  }));
  const instances = repositories.map((repository, index) => ({
    instance_id: `agent-${index}`,
    repository_id: repository.repository_id,
    project_id: repository.project_id,
    kind: "orchestrator",
    status: index === 3 ? "disconnected" : "online",
    started_at: ago(95),
    last_heartbeat_at: ago(index === 3 ? 48 : 0.12 * (index + 1)),
    lease_expires_at: index === 3 ? ago(18) : future(24),
    disconnected_at: index === 3 ? ago(48) : null,
    capabilities: { spawning: true, independent_audits: true },
  }));
  const features = [
    {
      feature_id: "feature-checkout",
      project_id: "demo-orbit",
      title: "Build the next checkout",
      summary:
        "A unified checkout experience across web and mobile. Shared payment contract, resilient retries and independent audits.",
      coordinator_repository_id: "repo-api",
      status: "active",
      revision: 4,
      created_at: ago(120),
      updated_at: ago(4),
    },
    {
      feature_id: "feature-auth",
      project_id: "demo-orbit",
      title: "Passwordless authentication",
      summary:
        "Secure sign-in without the friction. A coordinated rollout of passkey authentication.",
      coordinator_repository_id: "repo-api",
      status: "active",
      revision: 3,
      created_at: ago(180),
      updated_at: ago(16),
    },
    {
      feature_id: "feature-search",
      project_id: "demo-atlas",
      title: "Semantic discovery pipeline",
      summary:
        "Connect indexing, retrieval and evaluation into one observable pipeline.",
      coordinator_repository_id: "repo-engine",
      status: "active",
      revision: 2,
      created_at: ago(90),
      updated_at: ago(8),
    },
  ];
  const workSpecs = [
    [
      "payment",
      "Define payment intent contract",
      "api",
      "implementation",
      "completed",
      "feature-checkout",
      "verified",
    ],
    [
      "checkout",
      "Wire up the checkout experience",
      "web",
      "implementation",
      "in_progress",
      "feature-checkout",
    ],
    [
      "wallet",
      "Implement native wallet flow",
      "mobile",
      "implementation",
      "in_progress",
      "feature-checkout",
    ],
    [
      "webhook",
      "Reconcile payment webhooks",
      "api",
      "implementation",
      "ready",
      "feature-checkout",
    ],
    [
      "payment-audit",
      "Audit the payment boundary",
      "api",
      "audit",
      "completed",
      "feature-checkout",
    ],
    [
      "passkey",
      "Add passkey challenge endpoints",
      "api",
      "implementation",
      "completed",
      "feature-auth",
      "pending_audit",
    ],
    [
      "auth-audit",
      "Verify passkey replay protection",
      "api",
      "audit",
      "in_progress",
      "feature-auth",
    ],
    [
      "auth-web",
      "Design the sign-in ceremony",
      "web",
      "implementation",
      "blocked",
      "feature-auth",
    ],
    [
      "auth-mobile",
      "Connect device credentials",
      "mobile",
      "implementation",
      "planned",
      "feature-auth",
    ],
    [
      "embedding",
      "Implement embedding batch pipeline",
      "engine",
      "implementation",
      "in_progress",
      "feature-search",
    ],
    [
      "retrieval",
      "Measure retrieval quality",
      "engine",
      "implementation",
      "planned",
      "feature-search",
    ],
    [
      "index",
      "Provision vector index",
      "engine",
      "implementation",
      "completed",
      "feature-search",
      "verified",
    ],
  ];
  const summaries = {
    payment:
      "Publish the shared request and response contract. Cover idempotency keys, currency precision and retry semantics.",
    checkout:
      "Build the multi-step checkout flow against the accepted payment contract. Keep loading, retry and confirmation states consistent.",
    wallet:
      "Connect the native wallet sheet and synchronize payment state with the shared API.",
    webhook:
      "Handle webhook replays and out-of-order delivery without duplicating transactions.",
    "payment-audit":
      "Independent verification of payment contracts, failure modes and idempotency behavior.",
    passkey:
      "Implement enrollment and authentication challenges with scoped expiry and replay prevention.",
    "auth-audit":
      "Validate challenge expiry, origin binding and one-time consumption independently.",
    "auth-web":
      "Waiting for the repository answer about credential recovery behavior.",
    "auth-mobile":
      "Integrate platform credentials after the recovery policy is agreed.",
    embedding:
      "Batch document embeddings with backpressure and resumable checkpoints.",
    retrieval:
      "Evaluate recall against the fixed query corpus after embeddings are available.",
    index:
      "Create a versioned vector index with predictable collection lifecycle.",
  };
  const work_items = workSpecs.map(
    (
      [key, title, repository, kind, status, feature_id, verification_status],
      index,
    ) => ({
      work_item_id: `work-${key}`,
      feature_id,
      project_id: repository === "engine" ? "demo-atlas" : "demo-orbit",
      owner_repository_id: `repo-${repository}`,
      title,
      summary: summaries[key],
      kind,
      status,
      revision: status === "completed" ? 5 : 2,
      priority: index < 3 ? 1 : 2,
      max_audit_iterations: 3,
      verification_status: verification_status || null,
      dependency_ids:
        key === "checkout" || key === "wallet"
          ? ["work-payment"]
          : key === "retrieval"
            ? ["work-embedding"]
            : [],
      audit_scope_work_item_ids:
        key === "payment-audit"
          ? ["work-payment"]
          : key === "auth-audit"
            ? ["work-passkey"]
            : [],
      implementation_artifact_id:
        key === "payment"
          ? "artifact-payment"
          : key === "passkey"
            ? "artifact-passkey"
            : null,
      latest_audit_artifact_id: key === "payment" ? "artifact-audit" : null,
      assignee_instance_id:
        status === "in_progress"
          ? instances.find(
              (actor) => actor.repository_id === `repo-${repository}`,
            )?.instance_id
          : null,
      created_at: ago(110 - index * 3),
      updated_at: ago(index * 1.1 + 0.3),
      finished_at: status === "completed" ? ago(24 - index) : null,
      claim_expires_at: status === "in_progress" ? future(44) : null,
    }),
  );
  const workers = [
    "checkout",
    "wallet",
    "auth-audit",
    "embedding",
    "payment",
    "payment-audit",
    "passkey",
  ].map((key, index) => {
    const work = work_items.find((item) => item.work_item_id === `work-${key}`);
    return {
      worker_run_id: `worker-${key}`,
      project_id: work.project_id,
      repository_id: work.owner_repository_id,
      parent_instance_id: instances.find(
        (actor) => actor.repository_id === work.owner_repository_id,
      ).instance_id,
      role: work.kind === "audit" ? "auditor" : "executor",
      status: index < 4 ? "active" : "completed",
      task_summary: work.title,
      work_item_id: work.work_item_id,
      request_id: null,
      context_refs: [{ feature: work.feature_id, source: "accepted plan" }],
      result_summary:
        index < 4 ? null : "Implementation and verification report published.",
      error_summary: null,
      created_at: ago(40 + index * 3),
      activated_at: ago(39 + index * 3),
      last_heartbeat_at: ago(index < 4 ? 0.1 + index * 0.07 : 20),
      lease_expires_at: future(40),
      finished_at: index < 4 ? null : ago(20),
    };
  });
  const artifacts = [
    [
      "payment",
      "Payment intent · implementation report",
      "implementation_report",
      "api",
      "feature-checkout",
      "work-payment",
    ],
    [
      "audit",
      "Payment boundary · independent audit",
      "audit_report",
      "api",
      "feature-checkout",
      "work-payment-audit",
    ],
    [
      "contract",
      "Checkout API contract / v2",
      "api_contract",
      "api",
      "feature-checkout",
      "work-payment",
    ],
    [
      "passkey",
      "Passkey challenge · implementation report",
      "implementation_report",
      "api",
      "feature-auth",
      "work-passkey",
    ],
    [
      "note",
      "Retrieval evaluation protocol",
      "note",
      "engine",
      "feature-search",
      null,
    ],
  ].map(([key, title, kind, repository, feature_id, work_item_id], index) => ({
    artifact_id: `artifact-${key}`,
    title,
    kind,
    producer_repository_id: `repo-${repository}`,
    project_id: repository === "engine" ? "demo-atlas" : "demo-orbit",
    content_type: "text/markdown",
    feature_id,
    work_item_id,
    created_at: ago(25 - index * 3),
  }));
  const bodies = {
    payment:
      "# Payment intent implementation\n\nStatus: completed · independently verified\n\n## Changes\n\nImplemented the shared payment intent contract with durable idempotency keys, explicit currency units, and consistent retry behavior.\n\nThe web and mobile repositories can now consume the same accepted contract.\n\n## Verification\n\n• Duplicate requests reuse the same intent.\n• Invalid currency precision is rejected.\n• Provider failures return a retryable error without committing partial state.\n\n## Handoff\n\nWeb Client and Mobile App own the remaining checkout integration.",
    audit:
      "# Independent payment audit\n\nVerdict: PASSED\n\n## Scope\n\nPayment intent API, idempotency boundary, provider error recovery.\n\n## Findings\n\nNo actionable findings. The contract and implementation agree on key reuse, expiry and failure semantics.\n\n## Evidence\n\n• Verified duplicate request behavior.\n• Exercised provider timeout and retry.\n• Checked invalid monetary amounts and empty identifiers.\n\nWork item may be marked verified.",
    contract:
      '# Checkout API / v2\n\nPOST /payment-intents\n\nRequest\n\n{\n  "amount_minor": 4990,\n  "currency": "EUR",\n  "idempotency_key": "checkout-session-key"\n}\n\nResponse\n\n{\n  "intent_id": "pi_example",\n  "status": "requires_confirmation",\n  "expires_in_seconds": 900\n}\n\nAll amounts use integer minor units. Repeating the same key returns the original intent.',
    passkey:
      "# Passkey challenge endpoints\n\nStatus: implementation completed · awaiting independent audit\n\n## Implemented\n\nEnrollment and sign-in challenges with origin binding, single consumption and bounded expiry.\n\n## Verification\n\nUnit and integration checks cover replay rejection, expired challenge handling and invalid origins.\n\n## Next owner\n\nIndependent auditor: verify security-sensitive boundaries before rollout.",
    note: "# Retrieval evaluation protocol\n\nUse the frozen query corpus for each index version.\n\nTrack recall at 10, median latency and empty-result rate. Record embedding model and collection version with every report.\n\nThe evaluation starts after the embedding batch pipeline completes.",
  };
  const artifactBodies = Object.fromEntries(
    artifacts.map((item) => [
      item.artifact_id,
      {
        ...item,
        content: bodies[item.artifact_id.replace("artifact-", "")],
        audit_outcomes:
          item.kind === "audit_report"
            ? [
                {
                  verdict: "passed",
                  summary:
                    "Payment boundaries satisfy the accepted contract and retry requirements.",
                  scoped_work_item_id: "work-payment",
                },
              ]
            : [],
      },
    ]),
  );
  const messages = [
    [
      "contract",
      "Payment contract v2 is ready for integration",
      "api",
      ["web", "mobile"],
      "announcement",
      "artifact-contract",
      0,
    ],
    [
      "audit",
      "Payment boundary audit passed with no findings",
      "api",
      ["web"],
      "report",
      "artifact-audit",
      0,
    ],
    [
      "passkey",
      "Passkey endpoints implemented; audit is running",
      "api",
      ["web", "mobile"],
      "report",
      "artifact-passkey",
      1,
    ],
  ].map(
    (
      [key, summary, sender, targets, kind, artifact_id, pending_deliveries],
      index,
    ) => ({
      message_id: `message-${key}`,
      project_id: "demo-orbit",
      sender_repository_id: `repo-${sender}`,
      target_repository_ids: targets.map((target) => `repo-${target}`),
      summary,
      kind,
      artifact_id,
      pending_deliveries,
      thread_id: `thread-${key}`,
      feature_id: index === 2 ? "feature-auth" : "feature-checkout",
      created_at: ago(18 - index * 5),
    }),
  );
  const requests = [
    {
      request_id: "request-recovery",
      project_id: "demo-orbit",
      requester_repository_id: "repo-web",
      target_repository_id: "repo-api",
      feature_id: "feature-auth",
      work_item_id: "work-auth-web",
      question_summary: "What is the agreed credential recovery behavior?",
      status: "claimed",
      handler_requirement: "butler_required",
      answer_requirement: "artifact_required",
      created_at: ago(14),
      updated_at: ago(3),
      context_refs: [],
    },
    {
      request_id: "request-wallet",
      project_id: "demo-orbit",
      requester_repository_id: "repo-mobile",
      target_repository_id: "repo-api",
      feature_id: "feature-checkout",
      work_item_id: "work-wallet",
      question_summary: "Confirm the wallet cancellation response contract",
      status: "pending",
      handler_requirement: "orchestrator_allowed",
      answer_requirement: "summary_allowed",
      created_at: ago(6),
      updated_at: ago(6),
      context_refs: [],
    },
  ];
  const events = [
    [
      "worker_run_active",
      "worker_run",
      "worker-embedding",
      "active",
      0.2,
      "repo-engine",
    ],
    [
      "work_item_updated",
      "work_item",
      "work-checkout",
      "in_progress",
      0.9,
      "repo-web",
    ],
    [
      "worker_run_active",
      "worker_run",
      "worker-auth-audit",
      "active",
      1.5,
      "repo-api",
    ],
    [
      "request_claimed",
      "request",
      "request-recovery",
      "claimed",
      3,
      "repo-api",
    ],
    [
      "work_item_updated",
      "work_item",
      "work-wallet",
      "in_progress",
      4,
      "repo-mobile",
    ],
    [
      "request_available",
      "request",
      "request-wallet",
      "pending",
      6,
      "repo-api",
    ],
    ["message_available", "message", "message-passkey", null, 8, "repo-api"],
    [
      "worker_run_completed",
      "worker_run",
      "worker-passkey",
      "completed",
      10,
      "repo-api",
    ],
    [
      "work_item_updated",
      "work_item",
      "work-payment",
      "completed",
      13,
      "repo-api",
    ],
    ["message_available", "message", "message-audit", null, 15, "repo-api"],
    [
      "worker_run_completed",
      "worker_run",
      "worker-payment-audit",
      "completed",
      17,
      "repo-api",
    ],
    ["message_available", "message", "message-contract", null, 18, "repo-api"],
    [
      "worker_run_completed",
      "worker_run",
      "worker-payment",
      "completed",
      21,
      "repo-api",
    ],
    [
      "feature_created",
      "feature",
      "feature-checkout",
      "active",
      35,
      "repo-api",
    ],
  ].map(
    (
      [kind, entity_type, entity_id, entity_status, minutes, repository],
      index,
    ) => ({
      event_id: 100 - index,
      project_id: repository === "repo-engine" ? "demo-atlas" : "demo-orbit",
      kind,
      entity_type,
      entity_id,
      entity_status,
      owner_repository_id: repository,
      recipient_count: repository === "repo-engine" ? 1 : 3,
      created_at: ago(minutes),
    }),
  );
  const snapshot = {
    projects,
    repositories,
    instances,
    features,
    work_items,
    workers,
    requests,
    messages,
    artifacts,
    events,
    server_time: new Date().toISOString(),
    version: "0.1.0",
    limit: 1000,
    totals: {},
  };
  return { snapshot, artifactBodies };
}
