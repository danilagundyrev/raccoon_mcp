# Raccoon Mission Control

A local, read-only command center for the agent coordination server in `src`. Assets are embedded with Rust's `include_str!`, so the dashboard ships in the same binary as the MCP server and works offline.

From the repository root:

```powershell
cargo run --release -- --database .raccoon-mcp/hub.sqlite3
```

Open **http://127.0.0.1:8765/dashboard**. Use the same host and port as your MCP endpoint. Rebuild/restart after changing embedded frontend assets.

## Files

| File | Responsibility |
| --- | --- |
| `index.html`, `style.css` | Responsive command-center shell, animations and accessibility. |
| `app.js` | Views, polling, project/search filters, inspectors and downloads. |
| `model.js` | Lease-aware presence, metrics, filtering and escaping. |
| `demo.js` | Explicitly opt-in, browser-only illustrative coordination data. |
| `../src/server/dashboard.rs` | Static assets and read-only HTTP routes. |
| `../src/store/sqlite/dashboard.rs` | Transactionally consistent observer snapshots and report reads. |

No runtime npm dependencies or build step. `package.json` only configures native JavaScript modules and a test command.

## Checks

From the repository root:

```powershell
cargo test
node --test raccoon_mcp_web/tests/model.test.js
cargo build
python raccoon_mcp_web/tests/smoke.py
```

The smoke check launches the compiled debug binary on an available local port with a temporary database. It exercises real MCP connections, work claims, worker activation/completion, report publication, artifact retrieval, project isolation, event fan-out and observer origin validation. The child process and its database are cleaned up afterward. Python is only needed for this optional check.

For browser QA against an **isolated** server, `python raccoon_mcp_web/tests/smoke.py --url http://127.0.0.1:8877` creates fixture coordination records on that server. Do not point this command at your normal database.

## Observer behavior

- Polls every two seconds, with an eight-second request timeout, cancellation on scope changes and automatic reconnection.
- Background tabs stop polling. Unchanged snapshots preserve running graph animations.
- Reads related records in one SQLite transaction. It never acquires an actor identity or renews leases.
- A lease must be valid at server time for an actor to count as connected. Expired actors remain inspectable.
- Up to 1,000 rows per collection; active entities are prioritized. The UI discloses truncation and counts only the loaded data. Events are ordered newest first and equivalent fan-out notifications are collapsed.
- Artifact bodies are loaded only on demand. Agent-supplied strings and report bodies are rendered as text, never executed as HTML. Downloads retain the full original body.
- All assets are local. The read API rejects disallowed origins and cross-site browser requests and sends `Cache-Control: no-store`.
- Demo data is held only in browser memory. Switching back always fetches the real server.

MCP agents do not gain any new tools or change their protocol. The learning MCP is unrelated to this dashboard.
