use super::*;
use serde_json::{Value, json};
use sqlx::{Column, TypeInfo, ValueRef};

const SNAPSHOT_LIMIT: i64 = 1000;

impl SqliteStore {
    /// A human observer reads all repositories without acquiring an actor or renewing a lease.
    /// One read transaction keeps related cards, workers and events in the same SQLite snapshot.
    pub async fn dashboard_snapshot(&self, project_id: Option<&str>) -> Result<Value, StoreError> {
        let mut transaction = self.pool.begin().await?;
        let mut snapshot = json!({"server_time": utc_now(), "limit": SNAPSHOT_LIMIT, "totals": {}});
        let collections = [
            ("projects", "SELECT * FROM projects", "updated_at DESC"),
            ("repositories", "SELECT * FROM repositories", "updated_at DESC"),
            ("instances", "SELECT a.*, r.project_id FROM agent_instances a JOIN repositories r USING(repository_id)", "(status = 'online') DESC, last_heartbeat_at DESC"),
            ("features", "SELECT * FROM features", "(status = 'active') DESC, updated_at DESC"),
            ("work_items", "SELECT w.*, f.project_id,
                (SELECT json_group_array(dependency_work_item_id) FROM work_item_dependencies d WHERE d.work_item_id = w.work_item_id) AS dependency_ids_json,
                (SELECT json_group_array(scoped_work_item_id) FROM work_item_audit_scope s WHERE s.audit_work_item_id = w.work_item_id) AS audit_scope_work_item_ids_json
                FROM work_items w JOIN features f USING(feature_id)", "(status NOT IN ('completed', 'cancelled')) DESC, updated_at DESC"),
            ("workers", "SELECT w.*, r.project_id FROM worker_runs w JOIN repositories r USING(repository_id)", "(status IN ('created', 'active')) DESC, created_at DESC"),
            ("requests", "SELECT * FROM requests", "(status IN ('pending', 'claimed')) DESC, updated_at DESC"),
            ("messages", "SELECT m.*,
                (SELECT json_group_array(repository_id) FROM message_targets t WHERE t.message_id = m.message_id) AS target_repository_ids_json,
                (SELECT count(*) FROM deliveries d WHERE d.message_id = m.message_id AND d.status = 'pending') AS pending_deliveries
                FROM messages m", "created_at DESC"),
            ("artifacts", "SELECT a.artifact_id, a.project_id, a.producer_repository_id, a.producer_instance_id, a.producer_worker_run_id, a.kind, a.title, a.content_type, a.created_at,
                (SELECT feature_id FROM messages m WHERE m.artifact_id = a.artifact_id LIMIT 1) AS feature_id,
                (SELECT work_item_id FROM messages m WHERE m.artifact_id = a.artifact_id LIMIT 1) AS work_item_id
                FROM artifacts a", "created_at DESC"),
            // Events are fan-out notifications. Collapse copies of the same change for the human feed.
            ("events", "SELECT max(event_id) AS event_id, project_id, kind, entity_type, entity_id, feature_id, owner_repository_id, entity_status, entity_revision, work_item_id, request_id, created_at, count(*) AS recipient_count
                FROM events GROUP BY project_id, kind, entity_type, entity_id, feature_id, owner_repository_id, entity_status, entity_revision, work_item_id, request_id, created_at", "event_id DESC"),
        ];
        for (name, select, order) in collections {
            let scope = if name == "projects" { None } else { project_id };
            // All SQL identifiers and expressions above are compile-time constants. Only values are bound.
            let query = format!(
                "SELECT *, count(*) OVER() AS _total FROM ({select}) WHERE (? IS NULL OR project_id = ?) ORDER BY {order} LIMIT ?"
            );
            let rows = sqlx::query(&query)
                .bind(scope)
                .bind(scope)
                .bind(SNAPSHOT_LIMIT)
                .fetch_all(&mut *transaction)
                .await?;
            snapshot["totals"][name] = json!(
                rows.first()
                    .map(|row| row.get::<i64, _>("_total"))
                    .unwrap_or(0)
            );
            snapshot[name] =
                Value::Array(rows.iter().map(dashboard_row).collect::<Result<_, _>>()?);
        }
        transaction.commit().await?;
        Ok(snapshot)
    }

    pub async fn dashboard_artifact(&self, id: &str) -> Result<Option<Value>, StoreError> {
        let mut transaction = self.pool.begin().await?;
        let row = sqlx::query("SELECT * FROM artifacts WHERE artifact_id = ?")
            .bind(id)
            .fetch_optional(&mut *transaction)
            .await?;
        let Some(row) = row else {
            return Ok(None);
        };
        let mut artifact = dashboard_row(&row)?;
        let outcomes = sqlx::query("SELECT * FROM audit_outcomes WHERE audit_artifact_id = ?")
            .bind(id)
            .fetch_all(&mut *transaction)
            .await?;
        artifact["audit_outcomes"] = Value::Array(
            outcomes
                .iter()
                .map(dashboard_row)
                .collect::<Result<_, _>>()?,
        );
        transaction.commit().await?;
        Ok(Some(artifact))
    }
}

fn dashboard_row(row: &SqliteRow) -> Result<Value, StoreError> {
    let mut fields = serde_json::Map::new();
    for column in row.columns() {
        let name = column.name();
        if name == "_total" {
            continue;
        }
        let raw = row.try_get_raw(column.ordinal())?;
        let value = if raw.is_null() {
            Value::Null
        } else if raw.type_info().name() == "INTEGER" {
            json!(row.try_get::<i64, _>(column.ordinal())?)
        } else {
            let text: String = row.try_get(column.ordinal())?;
            if name.ends_with("_json") {
                serde_json::from_str(&text)?
            } else {
                json!(text)
            }
        };
        fields.insert(name.strip_suffix("_json").unwrap_or(name).to_owned(), value);
    }
    Ok(Value::Object(fields))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    async fn test_store(lease: u16) -> (SqliteStore, std::path::PathBuf) {
        let path = std::env::temp_dir().join(format!(
            "raccoon-dashboard-{}.sqlite3",
            uuid::Uuid::new_v4()
        ));
        let store = SqliteStore::open(&path, 2, lease, 3600, 600, 3600)
            .await
            .unwrap();
        (store, path)
    }

    fn connect_command(project: &str, repo: &str, path: &str) -> ConnectOrchestrator {
        ConnectOrchestrator {
            project_key: project.into(),
            project_display_name: None,
            repository_key: repo.into(),
            repository_display_name: None,
            canonical_path: path.into(),
            instance_id: None,
            capabilities: json!({"spawning": true}),
        }
    }

    #[tokio::test]
    async fn snapshot_discloses_truncation_instead_of_silently_counting_a_partial_history() {
        let (store, path) = test_store(90).await;
        let mut transaction = store.pool.begin().await.unwrap();
        for index in 0..1003 {
            sqlx::query("INSERT INTO projects (project_id, project_key, display_name, created_at, updated_at) VALUES (?, ?, 'Fixture', ?, ?)")
                .bind(ProjectId::new().to_string()).bind(format!("project-{index}"))
                .bind(utc_now()).bind(utc_now()).execute(&mut *transaction).await.unwrap();
        }
        transaction.commit().await.unwrap();
        let snapshot = store.dashboard_snapshot(None).await.unwrap();
        assert_eq!(snapshot["projects"].as_array().unwrap().len(), 1000);
        assert_eq!(snapshot["totals"]["projects"], 1003);
        assert_eq!(snapshot["limit"], 1000);
        store.pool.close().await;
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn snapshot_is_project_scoped_and_does_not_renew_leases() {
        let (store, path) = test_store(90).await;
        let first = store
            .connect_orchestrator(connect_command("first", "api", "C:/first"))
            .await
            .unwrap();
        store
            .connect_orchestrator(connect_command("second", "app", "C:/second"))
            .await
            .unwrap();
        sqlx::query("UPDATE agent_instances SET lease_expires_at = '2000-01-01T00:00:00Z' WHERE instance_id = ?")
            .bind(first.instance.instance_id.to_string()).execute(&store.pool).await.unwrap();
        let snapshot = store
            .dashboard_snapshot(Some(&first.project.project_id.to_string()))
            .await
            .unwrap();
        assert_eq!(snapshot["projects"].as_array().unwrap().len(), 2);
        assert_eq!(snapshot["repositories"].as_array().unwrap().len(), 1);
        assert_eq!(snapshot["instances"].as_array().unwrap().len(), 1);
        assert_eq!(
            snapshot["instances"][0]["lease_expires_at"],
            "2000-01-01T00:00:00Z"
        );
        assert_eq!(snapshot["instances"][0]["capabilities"]["spawning"], true);
        assert!(snapshot["work_items"].as_array().unwrap().is_empty());
        store.pool.close().await;
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn snapshot_reads_new_commits_and_excludes_artifact_bodies() {
        let (store, path) = test_store(90).await;
        assert!(
            store.dashboard_snapshot(None).await.unwrap()["projects"]
                .as_array()
                .unwrap()
                .is_empty()
        );
        let connected = store
            .connect_orchestrator(connect_command("live", "api", "C:/live"))
            .await
            .unwrap();
        let id = ArtifactId::new().to_string();
        sqlx::query("INSERT INTO artifacts (artifact_id, project_id, producer_repository_id, kind, title, content_type, content, created_at) VALUES (?, ?, ?, 'note', 'Report', 'text/markdown', '<script>untrusted</script>', ?)")
            .bind(&id).bind(connected.project.project_id.to_string()).bind(connected.repository.repository_id.to_string()).bind(utc_now())
            .execute(&store.pool).await.unwrap();
        let snapshot = store.dashboard_snapshot(None).await.unwrap();
        assert_eq!(snapshot["projects"].as_array().unwrap().len(), 1);
        assert_eq!(snapshot["artifacts"][0]["title"], "Report");
        assert!(snapshot["artifacts"][0].get("content").is_none());
        let artifact = store.dashboard_artifact(&id).await.unwrap().unwrap();
        assert_eq!(artifact["content"], "<script>untrusted</script>");
        assert!(
            store
                .dashboard_artifact(&ArtifactId::new().to_string())
                .await
                .unwrap()
                .is_none()
        );
        store.pool.close().await;
        let _ = std::fs::remove_file(path);
    }
}
