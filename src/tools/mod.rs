use dataxlr8_mcp_core::mcp::{empty_schema, error_result, get_i64, get_str, json_result, make_schema};
use dataxlr8_mcp_core::Database;
use rmcp::model::*;
use rmcp::service::{RequestContext, RoleServer};
use rmcp::ServerHandler;
use serde::{Deserialize, Serialize};
use tracing::{error, info, warn};

// ============================================================================
// Constants
// ============================================================================

const DEFAULT_LIMIT: i64 = 20;
const MAX_LIMIT: i64 = 500;
const DEFAULT_OFFSET: i64 = 0;
const DEFAULT_DAYS: i64 = 30;
const MAX_DAYS: i64 = 365;

const VALID_PERIODS: &[&str] = &["today", "week", "month", "all"];
const VALID_METRICS: &[&str] = &["contacts", "deals", "activities", "emails"];
const VALID_GRANULARITIES: &[&str] = &["daily", "weekly", "monthly"];

// ============================================================================
// Input helpers
// ============================================================================

/// Extract a trimmed string from args. Returns None if key missing or empty after trim.
fn get_trimmed_str(args: &serde_json::Value, key: &str) -> Option<String> {
    get_str(args, key).map(|s| s.trim().to_string()).filter(|s| !s.is_empty())
}

/// Extract and clamp a limit value.
fn get_limit(args: &serde_json::Value) -> i64 {
    get_i64(args, "limit")
        .unwrap_or(DEFAULT_LIMIT)
        .max(1)
        .min(MAX_LIMIT)
}

/// Extract and clamp an offset value.
fn get_offset(args: &serde_json::Value) -> i64 {
    get_i64(args, "offset").unwrap_or(DEFAULT_OFFSET).max(0)
}

/// Validate a period value, returning a default if invalid.
fn validate_period(args: &serde_json::Value, default: &str) -> Result<String, CallToolResult> {
    let period = get_trimmed_str(args, "period").unwrap_or_else(|| default.to_string());
    if !VALID_PERIODS.contains(&period.as_str()) {
        return Err(error_result(&format!(
            "Invalid period '{}'. Must be one of: {}",
            period,
            VALID_PERIODS.join(", ")
        )));
    }
    Ok(period)
}

// ============================================================================
// Data types
// ============================================================================

#[derive(Debug, Serialize, Deserialize, sqlx::FromRow)]
pub struct SavedDashboard {
    pub id: uuid::Uuid,
    pub name: String,
    pub config: serde_json::Value,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Serialize, Deserialize, sqlx::FromRow)]
pub struct KpiCache {
    pub id: uuid::Uuid,
    pub metric_name: String,
    pub value: f64,
    pub period: String,
    pub cached_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Serialize, Deserialize, sqlx::FromRow)]
struct CountRow {
    count: i64,
}

#[derive(Debug, Serialize, Deserialize, sqlx::FromRow)]
struct SumRow {
    total: Option<f64>,
}

#[derive(Debug, Serialize, Deserialize, sqlx::FromRow)]
struct StageRow {
    stage: String,
    deal_count: i64,
    total_value: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, sqlx::FromRow)]
struct ActivityRow {
    source: String,
    activity_type: String,
    subject: Option<String>,
    occurred_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Serialize, Deserialize, sqlx::FromRow)]
struct TeamRow {
    owner_id: Option<uuid::Uuid>,
    metric: String,
    count: i64,
}

#[derive(Debug, Serialize, Deserialize, sqlx::FromRow)]
struct TrendRow {
    period: String,
    count: i64,
}

#[derive(Debug, Serialize, Deserialize, sqlx::FromRow)]
struct SchemaCheck {
    schema_name: String,
}

// ============================================================================
// Tool definitions
// ============================================================================

fn build_tools() -> Vec<Tool> {
    vec![
        Tool {
            name: "kpi_snapshot".into(),
            title: None,
            description: Some(
                "Query key metrics across crm, email, enrichment schemas. Returns contacts added, deals won, emails sent, enrichments run for a given period (today, week, month, all)."
                    .into(),
            ),
            input_schema: make_schema(
                serde_json::json!({
                    "period": { "type": "string", "enum": ["today", "week", "month", "all"], "description": "Time period for metrics (default: all)" }
                }),
                vec![],
            ),
            output_schema: None,
            annotations: None,
            execution: None,
            icons: None,
            meta: None,
        },
        Tool {
            name: "revenue_forecast".into(),
            title: None,
            description: Some(
                "Forecast revenue based on deal pipeline values and stage probabilities. Shows weighted pipeline value and expected revenue."
                    .into(),
            ),
            input_schema: empty_schema(),
            output_schema: None,
            annotations: None,
            execution: None,
            icons: None,
            meta: None,
        },
        Tool {
            name: "activity_feed".into(),
            title: None,
            description: Some(
                "Latest actions across all MCPs (emails sent, deals moved, contacts added) in chronological order."
                    .into(),
            ),
            input_schema: make_schema(
                serde_json::json!({
                    "limit": { "type": "integer", "description": "Max results (default 20, max 500)" },
                    "offset": { "type": "integer", "description": "Number of results to skip (default 0)" }
                }),
                vec![],
            ),
            output_schema: None,
            annotations: None,
            execution: None,
            icons: None,
            meta: None,
        },
        Tool {
            name: "team_performance".into(),
            title: None,
            description: Some(
                "Per-agent/user metrics: deals closed, emails sent, contacts enriched. Grouped by owner_id."
                    .into(),
            ),
            input_schema: make_schema(
                serde_json::json!({
                    "period": { "type": "string", "enum": ["today", "week", "month", "all"], "description": "Time period (default: month)" },
                    "limit": { "type": "integer", "description": "Max results per metric (default 20, max 500)" },
                    "offset": { "type": "integer", "description": "Number of results to skip per metric (default 0)" }
                }),
                vec![],
            ),
            output_schema: None,
            annotations: None,
            execution: None,
            icons: None,
            meta: None,
        },
        Tool {
            name: "trend_chart".into(),
            title: None,
            description: Some(
                "Daily/weekly/monthly trends for any metric. Returns time-series data for charting."
                    .into(),
            ),
            input_schema: make_schema(
                serde_json::json!({
                    "metric": { "type": "string", "enum": ["contacts", "deals", "activities", "emails"], "description": "Which metric to trend" },
                    "granularity": { "type": "string", "enum": ["daily", "weekly", "monthly"], "description": "Time granularity (default: daily)" },
                    "days": { "type": "integer", "description": "How many days back to look (default: 30, max: 365)" },
                    "limit": { "type": "integer", "description": "Max data points to return (default 20, max 500)" },
                    "offset": { "type": "integer", "description": "Number of data points to skip (default 0)" }
                }),
                vec!["metric"],
            ),
            output_schema: None,
            annotations: None,
            execution: None,
            icons: None,
            meta: None,
        },
        Tool {
            name: "save_dashboard".into(),
            title: None,
            description: Some(
                "Save a set of KPI queries as a named dashboard configuration."
                    .into(),
            ),
            input_schema: make_schema(
                serde_json::json!({
                    "name": { "type": "string", "description": "Dashboard name (unique)" },
                    "config": { "type": "object", "description": "Dashboard config JSON (widgets, metrics, layout)" }
                }),
                vec!["name", "config"],
            ),
            output_schema: None,
            annotations: None,
            execution: None,
            icons: None,
            meta: None,
        },
        Tool {
            name: "load_dashboard".into(),
            title: None,
            description: Some(
                "Retrieve a saved dashboard config by name."
                    .into(),
            ),
            input_schema: make_schema(
                serde_json::json!({
                    "name": { "type": "string", "description": "Dashboard name to load" }
                }),
                vec!["name"],
            ),
            output_schema: None,
            annotations: None,
            execution: None,
            icons: None,
            meta: None,
        },
        Tool {
            name: "health_check".into(),
            title: None,
            description: Some(
                "Verify all dataxlr8 schemas exist and are accessible. Returns status for each schema."
                    .into(),
            ),
            input_schema: empty_schema(),
            output_schema: None,
            annotations: None,
            execution: None,
            icons: None,
            meta: None,
        },
    ]
}

// ============================================================================
// MCP Server
// ============================================================================

const KNOWN_SCHEMAS: &[&str] = &[
    "crm",
    "email",
    "enrichment",
    "dashboard",
    "commissions",
    "contacts",
    "pipeline",
    "scoring",
    "templates",
    "notifications",
    "analytics",
    "audit",
    "scheduler",
    "webhooks",
    "features",
    "reporting",
];

const STAGE_PROBABILITIES: &[(&str, f64)] = &[
    ("lead", 0.10),
    ("qualified", 0.25),
    ("proposal", 0.50),
    ("negotiation", 0.75),
    ("closed_won", 1.00),
    ("closed_lost", 0.00),
];

#[derive(Clone)]
pub struct DashboardMcpServer {
    db: Database,
}

impl DashboardMcpServer {
    pub fn new(db: Database) -> Self {
        Self { db }
    }

    fn period_filter(period: &str) -> &'static str {
        match period {
            "today" => "AND created_at >= CURRENT_DATE",
            "week" => "AND created_at >= CURRENT_DATE - INTERVAL '7 days'",
            "month" => "AND created_at >= CURRENT_DATE - INTERVAL '30 days'",
            _ => "",
        }
    }

    fn period_filter_col(period: &str, col: &str) -> String {
        match period {
            "today" => format!("AND {col} >= CURRENT_DATE"),
            "week" => format!("AND {col} >= CURRENT_DATE - INTERVAL '7 days'"),
            "month" => format!("AND {col} >= CURRENT_DATE - INTERVAL '30 days'"),
            _ => String::new(),
        }
    }

    // ---- Tool handlers ----

    async fn handle_kpi_snapshot(&self, args: &serde_json::Value) -> CallToolResult {
        let period = match validate_period(args, "all") {
            Ok(p) => p,
            Err(e) => return e,
        };
        let time_filter = Self::period_filter(&period);

        let mut kpis = serde_json::Map::new();
        kpis.insert("period".into(), serde_json::Value::String(period.clone()));

        // Contacts added
        let sql = format!("SELECT COUNT(*) as count FROM crm.contacts WHERE true {time_filter}");
        match sqlx::query_as::<_, CountRow>(&sql)
            .fetch_one(self.db.pool())
            .await
        {
            Ok(row) => { kpis.insert("contacts_added".into(), serde_json::json!(row.count)); }
            Err(e) => {
                warn!(error = %e, "Failed to query crm.contacts for KPI snapshot");
                kpis.insert("contacts_added".into(), serde_json::json!("schema_unavailable"));
            }
        }

        // Deals won
        let sql = format!(
            "SELECT COUNT(*) as count FROM crm.deals WHERE stage = 'closed_won' {time_filter}"
        );
        match sqlx::query_as::<_, CountRow>(&sql)
            .fetch_one(self.db.pool())
            .await
        {
            Ok(row) => { kpis.insert("deals_won".into(), serde_json::json!(row.count)); }
            Err(e) => {
                warn!(error = %e, "Failed to query crm.deals (won) for KPI snapshot");
                kpis.insert("deals_won".into(), serde_json::json!("schema_unavailable"));
            }
        }

        // Total deals
        let sql = format!("SELECT COUNT(*) as count FROM crm.deals WHERE true {time_filter}");
        match sqlx::query_as::<_, CountRow>(&sql)
            .fetch_one(self.db.pool())
            .await
        {
            Ok(row) => { kpis.insert("total_deals".into(), serde_json::json!(row.count)); }
            Err(e) => {
                warn!(error = %e, "Failed to query crm.deals (total) for KPI snapshot");
                kpis.insert("total_deals".into(), serde_json::json!("schema_unavailable"));
            }
        }

        // Pipeline value
        match sqlx::query_as::<_, SumRow>(
            "SELECT COALESCE(SUM(value), 0) as total FROM crm.deals WHERE stage NOT IN ('closed_won', 'closed_lost')",
        )
        .fetch_one(self.db.pool())
        .await
        {
            Ok(row) => { kpis.insert("pipeline_value".into(), serde_json::json!(row.total.unwrap_or(0.0))); }
            Err(e) => {
                warn!(error = %e, "Failed to query pipeline value");
                kpis.insert("pipeline_value".into(), serde_json::json!("schema_unavailable"));
            }
        }

        // Emails sent (from email schema if exists)
        let time_filter_sent = Self::period_filter_col(&period, "sent_at");
        let sql = format!("SELECT COUNT(*) as count FROM email.sent_emails WHERE true {time_filter_sent}");
        match sqlx::query_as::<_, CountRow>(&sql)
            .fetch_one(self.db.pool())
            .await
        {
            Ok(row) => { kpis.insert("emails_sent".into(), serde_json::json!(row.count)); }
            Err(e) => {
                warn!(error = %e, "Failed to query email.sent_emails");
                kpis.insert("emails_sent".into(), serde_json::json!("schema_unavailable"));
            }
        }

        // Enrichments run (from enrichment schema if exists)
        let sql = format!("SELECT COUNT(*) as count FROM enrichment.enrichment_runs WHERE true {time_filter}");
        match sqlx::query_as::<_, CountRow>(&sql)
            .fetch_one(self.db.pool())
            .await
        {
            Ok(row) => { kpis.insert("enrichments_run".into(), serde_json::json!(row.count)); }
            Err(e) => {
                warn!(error = %e, "Failed to query enrichment.enrichment_runs");
                kpis.insert("enrichments_run".into(), serde_json::json!("schema_unavailable"));
            }
        }

        // Cache the results
        for (key, val) in &kpis {
            if let Some(n) = val.as_i64() {
                if let Err(e) = sqlx::query(
                    "INSERT INTO dashboard.kpi_cache (metric_name, value, period) VALUES ($1, $2, $3)",
                )
                .bind(key)
                .bind(n as f64)
                .bind(&period)
                .execute(self.db.pool())
                .await
                {
                    warn!(error = %e, metric = key, "Failed to cache KPI metric");
                }
            }
        }

        info!(period = period.as_str(), "KPI snapshot generated");
        json_result(&kpis)
    }

    async fn handle_revenue_forecast(&self) -> CallToolResult {
        let mut forecast = serde_json::Map::new();
        let mut stages = Vec::new();
        let mut total_weighted = 0.0f64;
        let mut total_pipeline = 0.0f64;

        match sqlx::query_as::<_, StageRow>(
            r#"SELECT stage, COUNT(*) as deal_count, SUM(value)::TEXT as total_value
               FROM crm.deals
               WHERE stage NOT IN ('closed_won', 'closed_lost')
               GROUP BY stage ORDER BY stage"#,
        )
        .fetch_all(self.db.pool())
        .await
        {
            Ok(rows) => {
                for row in &rows {
                    let prob = STAGE_PROBABILITIES
                        .iter()
                        .find(|(s, _)| *s == row.stage)
                        .map(|(_, p)| *p)
                        .unwrap_or(0.0);

                    let value: f64 = row
                        .total_value
                        .as_deref()
                        .and_then(|v| v.parse().ok())
                        .unwrap_or(0.0);

                    let weighted = value * prob;
                    total_weighted += weighted;
                    total_pipeline += value;

                    stages.push(serde_json::json!({
                        "stage": row.stage,
                        "deals": row.deal_count,
                        "value": value,
                        "probability": prob,
                        "weighted_value": weighted,
                    }));
                }
            }
            Err(e) => {
                error!(error = %e, "Failed to query deal pipeline for revenue forecast");
                return error_result(&format!("Failed to query pipeline: {e}"));
            }
        }

        // Closed won revenue
        let won_revenue: f64 = match sqlx::query_as::<_, SumRow>(
            "SELECT COALESCE(SUM(value), 0) as total FROM crm.deals WHERE stage = 'closed_won'",
        )
        .fetch_one(self.db.pool())
        .await
        {
            Ok(row) => row.total.unwrap_or(0.0),
            Err(e) => {
                warn!(error = %e, "Failed to query closed_won revenue, defaulting to 0");
                0.0
            }
        };

        forecast.insert("stages".into(), serde_json::json!(stages));
        forecast.insert("total_pipeline_value".into(), serde_json::json!(total_pipeline));
        forecast.insert("weighted_forecast".into(), serde_json::json!(total_weighted));
        forecast.insert("closed_won_revenue".into(), serde_json::json!(won_revenue));
        forecast.insert(
            "total_expected_revenue".into(),
            serde_json::json!(won_revenue + total_weighted),
        );

        info!("Revenue forecast generated");
        json_result(&forecast)
    }

    async fn handle_activity_feed(&self, args: &serde_json::Value) -> CallToolResult {
        let limit = get_limit(args);
        let offset = get_offset(args);
        let mut activities: Vec<serde_json::Value> = Vec::new();

        // CRM activities
        match sqlx::query_as::<_, ActivityRow>(
            r#"SELECT 'crm' as source, activity_type, subject, occurred_at
               FROM crm.activities
               ORDER BY occurred_at DESC LIMIT $1 OFFSET $2"#,
        )
        .bind(limit)
        .bind(offset)
        .fetch_all(self.db.pool())
        .await
        {
            Ok(rows) => {
                for r in rows {
                    activities.push(serde_json::json!({
                        "source": r.source,
                        "type": r.activity_type,
                        "subject": r.subject,
                        "at": r.occurred_at,
                    }));
                }
            }
            Err(e) => {
                warn!(error = %e, "Failed to query crm.activities for activity feed");
            }
        }

        // Recent contacts
        #[derive(sqlx::FromRow)]
        struct RecentContact {
            email: Option<String>,
            created_at: chrono::DateTime<chrono::Utc>,
        }
        match sqlx::query_as::<_, RecentContact>(
            "SELECT email, created_at FROM crm.contacts ORDER BY created_at DESC LIMIT $1 OFFSET $2",
        )
        .bind(limit)
        .bind(offset)
        .fetch_all(self.db.pool())
        .await
        {
            Ok(rows) => {
                for r in rows {
                    activities.push(serde_json::json!({
                        "source": "crm",
                        "type": "contact_added",
                        "subject": r.email,
                        "at": r.created_at,
                    }));
                }
            }
            Err(e) => {
                warn!(error = %e, "Failed to query crm.contacts for activity feed");
            }
        }

        // Recent deals
        #[derive(sqlx::FromRow)]
        struct RecentDeal {
            title: String,
            stage: String,
            updated_at: chrono::DateTime<chrono::Utc>,
        }
        match sqlx::query_as::<_, RecentDeal>(
            "SELECT title, stage, updated_at FROM crm.deals ORDER BY updated_at DESC LIMIT $1 OFFSET $2",
        )
        .bind(limit)
        .bind(offset)
        .fetch_all(self.db.pool())
        .await
        {
            Ok(rows) => {
                for r in rows {
                    activities.push(serde_json::json!({
                        "source": "crm",
                        "type": format!("deal_{}", r.stage),
                        "subject": r.title,
                        "at": r.updated_at,
                    }));
                }
            }
            Err(e) => {
                warn!(error = %e, "Failed to query crm.deals for activity feed");
            }
        }

        // Sort all by timestamp descending
        activities.sort_by(|a, b| {
            let at_a = a.get("at").and_then(|v| v.as_str()).unwrap_or("");
            let at_b = b.get("at").and_then(|v| v.as_str()).unwrap_or("");
            at_b.cmp(at_a)
        });

        // Truncate to limit
        activities.truncate(limit as usize);

        info!(count = activities.len(), limit = limit, offset = offset, "Activity feed generated");
        json_result(&serde_json::json!({
            "activities": activities,
            "limit": limit,
            "offset": offset,
            "count": activities.len(),
        }))
    }

    async fn handle_team_performance(&self, args: &serde_json::Value) -> CallToolResult {
        let period = match validate_period(args, "month") {
            Ok(p) => p,
            Err(e) => return e,
        };
        let limit = get_limit(args);
        let offset = get_offset(args);
        let time_filter = Self::period_filter(&period);

        let mut team: Vec<serde_json::Value> = Vec::new();

        // Deals closed per owner
        let sql = format!(
            r#"SELECT owner_id, 'deals_closed' as metric, COUNT(*) as count
               FROM crm.deals
               WHERE stage = 'closed_won' AND owner_id IS NOT NULL {time_filter}
               GROUP BY owner_id
               ORDER BY count DESC
               LIMIT $1 OFFSET $2"#
        );
        match sqlx::query_as::<_, TeamRow>(&sql)
            .bind(limit)
            .bind(offset)
            .fetch_all(self.db.pool())
            .await
        {
            Ok(rows) => {
                for r in rows {
                    team.push(serde_json::json!({
                        "owner_id": r.owner_id,
                        "metric": r.metric,
                        "count": r.count,
                    }));
                }
            }
            Err(e) => {
                warn!(error = %e, "Failed to query deals closed per owner");
            }
        }

        // Contacts owned per owner
        let sql = format!(
            r#"SELECT owner_id, 'contacts_owned' as metric, COUNT(*) as count
               FROM crm.contacts
               WHERE owner_id IS NOT NULL {time_filter}
               GROUP BY owner_id
               ORDER BY count DESC
               LIMIT $1 OFFSET $2"#
        );
        match sqlx::query_as::<_, TeamRow>(&sql)
            .bind(limit)
            .bind(offset)
            .fetch_all(self.db.pool())
            .await
        {
            Ok(rows) => {
                for r in rows {
                    team.push(serde_json::json!({
                        "owner_id": r.owner_id,
                        "metric": r.metric,
                        "count": r.count,
                    }));
                }
            }
            Err(e) => {
                warn!(error = %e, "Failed to query contacts owned per owner");
            }
        }

        // Activities per owner (via deals)
        let sql = format!(
            r#"SELECT d.owner_id, 'activities_logged' as metric, COUNT(a.*) as count
               FROM crm.activities a
               JOIN crm.deals d ON d.id = a.deal_id
               WHERE d.owner_id IS NOT NULL {time_filter_a}
               GROUP BY d.owner_id
               ORDER BY count DESC
               LIMIT $1 OFFSET $2"#,
            time_filter_a = Self::period_filter_col(&period, "a.occurred_at")
        );
        match sqlx::query_as::<_, TeamRow>(&sql)
            .bind(limit)
            .bind(offset)
            .fetch_all(self.db.pool())
            .await
        {
            Ok(rows) => {
                for r in rows {
                    team.push(serde_json::json!({
                        "owner_id": r.owner_id,
                        "metric": r.metric,
                        "count": r.count,
                    }));
                }
            }
            Err(e) => {
                warn!(error = %e, "Failed to query activities per owner");
            }
        }

        info!(period = period.as_str(), limit = limit, offset = offset, "Team performance generated");
        json_result(&serde_json::json!({
            "period": period,
            "metrics": team,
            "limit": limit,
            "offset": offset,
        }))
    }

    async fn handle_trend_chart(&self, args: &serde_json::Value) -> CallToolResult {
        let metric = match get_trimmed_str(args, "metric") {
            Some(m) => m,
            None => return error_result("Missing required parameter: metric"),
        };

        if !VALID_METRICS.contains(&metric.as_str()) {
            return error_result(&format!(
                "Invalid metric '{}'. Must be one of: {}",
                metric,
                VALID_METRICS.join(", ")
            ));
        }

        let granularity = get_trimmed_str(args, "granularity").unwrap_or_else(|| "daily".into());
        if !VALID_GRANULARITIES.contains(&granularity.as_str()) {
            return error_result(&format!(
                "Invalid granularity '{}'. Must be one of: {}",
                granularity,
                VALID_GRANULARITIES.join(", ")
            ));
        }

        let days = get_i64(args, "days").unwrap_or(DEFAULT_DAYS).max(1).min(MAX_DAYS);
        let limit = get_limit(args);
        let offset = get_offset(args);

        let trunc = match granularity.as_str() {
            "weekly" => "week",
            "monthly" => "month",
            _ => "day",
        };

        let (table, col) = match metric.as_str() {
            "contacts" => ("crm.contacts", "created_at"),
            "deals" => ("crm.deals", "created_at"),
            "activities" => ("crm.activities", "occurred_at"),
            "emails" => ("email.sent_emails", "sent_at"),
            // Already validated above, this branch is unreachable
            _ => return error_result(&format!("Unknown metric: {metric}")),
        };

        let sql = format!(
            r#"SELECT DATE_TRUNC('{trunc}', {col})::TEXT as period, COUNT(*) as count
               FROM {table}
               WHERE {col} >= CURRENT_DATE - INTERVAL '{days} days'
               GROUP BY 1 ORDER BY 1
               LIMIT $1 OFFSET $2"#
        );

        match sqlx::query_as::<_, TrendRow>(&sql)
            .bind(limit)
            .bind(offset)
            .fetch_all(self.db.pool())
            .await
        {
            Ok(rows) => {
                let data: Vec<serde_json::Value> = rows
                    .iter()
                    .map(|r| {
                        serde_json::json!({
                            "period": r.period,
                            "count": r.count,
                        })
                    })
                    .collect();
                info!(metric = metric.as_str(), points = data.len(), "Trend chart generated");
                json_result(&serde_json::json!({
                    "metric": metric,
                    "granularity": granularity,
                    "days": days,
                    "data": data,
                    "limit": limit,
                    "offset": offset,
                    "count": data.len(),
                }))
            }
            Err(e) => {
                error!(error = %e, metric = metric.as_str(), "Trend query failed");
                error_result(&format!("Trend query failed (schema may not exist): {e}"))
            }
        }
    }

    async fn handle_save_dashboard(&self, args: &serde_json::Value) -> CallToolResult {
        let name = match get_trimmed_str(args, "name") {
            Some(n) => n,
            None => return error_result("Missing required parameter: name (must be a non-empty string)"),
        };

        if name.len() > 255 {
            return error_result("Dashboard name must be 255 characters or fewer");
        }

        let config = match args.get("config") {
            Some(c) if c.is_object() => c.clone(),
            Some(_) => return error_result("Parameter 'config' must be a JSON object"),
            None => return error_result("Missing required parameter: config"),
        };

        match sqlx::query_as::<_, SavedDashboard>(
            r#"INSERT INTO dashboard.saved_dashboards (name, config)
               VALUES ($1, $2)
               ON CONFLICT (name) DO UPDATE SET config = EXCLUDED.config, created_at = now()
               RETURNING *"#,
        )
        .bind(&name)
        .bind(&config)
        .fetch_one(self.db.pool())
        .await
        {
            Ok(dash) => {
                info!(name = name.as_str(), "Dashboard saved");
                json_result(&dash)
            }
            Err(e) => {
                error!(error = %e, name = name.as_str(), "Failed to save dashboard");
                error_result(&format!("Failed to save dashboard: {e}"))
            }
        }
    }

    async fn handle_load_dashboard(&self, args: &serde_json::Value) -> CallToolResult {
        let name = match get_trimmed_str(args, "name") {
            Some(n) => n,
            None => return error_result("Missing required parameter: name (must be a non-empty string)"),
        };

        match sqlx::query_as::<_, SavedDashboard>(
            "SELECT * FROM dashboard.saved_dashboards WHERE name = $1",
        )
        .bind(&name)
        .fetch_optional(self.db.pool())
        .await
        {
            Ok(Some(dash)) => {
                info!(name = name.as_str(), "Dashboard loaded");
                json_result(&dash)
            }
            Ok(None) => {
                // List available dashboards
                let names: Vec<String> = match sqlx::query_scalar(
                    "SELECT name FROM dashboard.saved_dashboards ORDER BY name",
                )
                .fetch_all(self.db.pool())
                .await
                {
                    Ok(n) => n,
                    Err(e) => {
                        warn!(error = %e, "Failed to list available dashboards");
                        vec![]
                    }
                };
                warn!(name = name.as_str(), "Dashboard not found");
                error_result(&format!(
                    "Dashboard '{}' not found. Available: {}",
                    name,
                    if names.is_empty() {
                        "(none)".to_string()
                    } else {
                        names.join(", ")
                    }
                ))
            }
            Err(e) => {
                error!(error = %e, name = name.as_str(), "Failed to load dashboard");
                error_result(&format!("Failed to load dashboard: {e}"))
            }
        }
    }

    async fn handle_health_check(&self) -> CallToolResult {
        let mut results = serde_json::Map::new();
        let mut healthy = 0;
        let mut unhealthy = 0;

        for schema in KNOWN_SCHEMAS {
            let sql = format!(
                "SELECT schema_name FROM information_schema.schemata WHERE schema_name = '{schema}'"
            );
            match sqlx::query_as::<_, SchemaCheck>(&sql)
                .fetch_optional(self.db.pool())
                .await
            {
                Ok(Some(_)) => {
                    results.insert(schema.to_string(), serde_json::json!("ok"));
                    healthy += 1;
                }
                Ok(None) => {
                    results.insert(schema.to_string(), serde_json::json!("missing"));
                    unhealthy += 1;
                }
                Err(e) => {
                    error!(error = %e, schema = schema, "Health check failed for schema");
                    results.insert(
                        schema.to_string(),
                        serde_json::json!(format!("error: {e}")),
                    );
                    unhealthy += 1;
                }
            }
        }

        // DB connectivity
        match self.db.health_check().await {
            Ok(()) => {
                results.insert("database".into(), serde_json::json!("connected"));
            }
            Err(e) => {
                error!(error = %e, "Database health check failed");
                results.insert("database".into(), serde_json::json!(format!("error: {e}")));
            }
        }

        info!(healthy = healthy, unhealthy = unhealthy, "Health check complete");
        json_result(&serde_json::json!({
            "healthy": healthy,
            "unhealthy": unhealthy,
            "total": KNOWN_SCHEMAS.len(),
            "schemas": results,
        }))
    }
}

// ============================================================================
// ServerHandler trait implementation
// ============================================================================

impl ServerHandler for DashboardMcpServer {
    fn get_info(&self) -> ServerInfo {
        ServerInfo {
            protocol_version: ProtocolVersion::V_2024_11_05,
            capabilities: ServerCapabilities::builder().enable_tools().build(),
            server_info: Implementation::from_build_env(),
            instructions: Some(
                "DataXLR8 Dashboard MCP — executive-level views across all dataxlr8 schemas. KPIs, revenue forecast, activity feed, team performance, trends."
                    .into(),
            ),
        }
    }

    fn list_tools(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> impl std::future::Future<Output = Result<ListToolsResult, rmcp::ErrorData>> + Send + '_ {
        async {
            Ok(ListToolsResult {
                tools: build_tools(),
                next_cursor: None,
                meta: None,
            })
        }
    }

    fn call_tool(
        &self,
        request: CallToolRequestParams,
        _context: RequestContext<RoleServer>,
    ) -> impl std::future::Future<Output = Result<CallToolResult, rmcp::ErrorData>> + Send + '_ {
        async move {
            let args =
                serde_json::to_value(&request.arguments).unwrap_or(serde_json::Value::Null);
            let name_str: &str = request.name.as_ref();

            info!(tool = name_str, "Tool invoked");

            let result = match name_str {
                "kpi_snapshot" => self.handle_kpi_snapshot(&args).await,
                "revenue_forecast" => self.handle_revenue_forecast().await,
                "activity_feed" => self.handle_activity_feed(&args).await,
                "team_performance" => self.handle_team_performance(&args).await,
                "trend_chart" => self.handle_trend_chart(&args).await,
                "save_dashboard" => self.handle_save_dashboard(&args).await,
                "load_dashboard" => self.handle_load_dashboard(&args).await,
                "health_check" => self.handle_health_check().await,
                other => {
                    warn!(tool = other, "Unknown tool invoked");
                    error_result(&format!("Unknown tool: {}", request.name))
                }
            };

            Ok(result)
        }
    }
}
