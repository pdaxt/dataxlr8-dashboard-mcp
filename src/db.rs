use anyhow::Result;
use sqlx::PgPool;

pub async fn setup_schema(pool: &PgPool) -> Result<()> {
    sqlx::raw_sql(
        r#"
        CREATE SCHEMA IF NOT EXISTS dashboard;

        CREATE TABLE IF NOT EXISTS dashboard.saved_dashboards (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            name TEXT NOT NULL UNIQUE,
            config JSONB NOT NULL DEFAULT '{}'::jsonb,
            created_at TIMESTAMPTZ DEFAULT now()
        );

        CREATE TABLE IF NOT EXISTS dashboard.kpi_cache (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            metric_name TEXT NOT NULL,
            value DOUBLE PRECISION NOT NULL DEFAULT 0,
            period TEXT NOT NULL DEFAULT 'all',
            cached_at TIMESTAMPTZ DEFAULT now()
        );

        CREATE INDEX IF NOT EXISTS idx_saved_dashboards_name ON dashboard.saved_dashboards(name);
        CREATE INDEX IF NOT EXISTS idx_kpi_cache_metric ON dashboard.kpi_cache(metric_name);
        CREATE INDEX IF NOT EXISTS idx_kpi_cache_period ON dashboard.kpi_cache(period);
        "#,
    )
    .execute(pool)
    .await?;

    Ok(())
}
