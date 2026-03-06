# dataxlr8-dashboard-mcp

Provides KPI dashboards and analytics for the DataXLR8 CRM and recruitment platform. Query key metrics, forecast revenue, track team performance, and save custom dashboard configurations.

## Tools

| Tool | Description |
|------|-------------|
| kpi_snapshot | Query key metrics across crm, email, enrichment schemas. Returns contacts added, deals won, emails sent, enrichments run for a given period (today, week, month, all). |
| revenue_forecast | Forecast revenue based on deal pipeline values and stage probabilities. Shows weighted pipeline value and expected revenue. |
| activity_feed | Latest actions across all MCPs (emails sent, deals moved, contacts added) in chronological order. |
| team_performance | Per-agent/user metrics: deals closed, emails sent, contacts enriched. Grouped by owner_id. |
| trend_chart | Daily/weekly/monthly trends for any metric. Returns time-series data for charting. |
| save_dashboard | Save a set of KPI queries as a named dashboard configuration. |
| load_dashboard | Retrieve a saved dashboard config by name. |
| health_check | Verify all dataxlr8 schemas exist and are accessible. Returns status for each schema. |

## Setup

```bash
DATABASE_URL=postgres://dataxlr8:dataxlr8@localhost:5432/dataxlr8 cargo run
```

## Schema

Creates `dashboard` schema in PostgreSQL with tables for saved dashboard configurations and KPI caching.

## Part of

[DataXLR8](https://github.com/pdaxt) - AI-powered recruitment platform
