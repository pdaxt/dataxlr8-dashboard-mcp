use anyhow::Result;
use rmcp::transport::io::stdio;
use rmcp::ServiceExt;
use tracing::info;

mod db;
mod tools;

use tools::DashboardMcpServer;

#[tokio::main]
async fn main() -> Result<()> {
    let config = dataxlr8_mcp_core::Config::from_env("dataxlr8-dashboard-mcp")
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    dataxlr8_mcp_core::logging::init(&config.log_level);

    info!(
        server = config.server_name,
        "Starting DataXLR8 Dashboard MCP server"
    );

    let database = dataxlr8_mcp_core::Database::connect(&config.database_url)
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    db::setup_schema(database.pool()).await?;

    let server = DashboardMcpServer::new(database.clone());

    let transport = stdio();
    let service = server.serve(transport).await?;

    info!("Dashboard MCP server connected via stdio");

    tokio::select! {
        result = service.waiting() => {
            result?;
            info!("MCP service ended");
        }
        _ = tokio::signal::ctrl_c() => {
            info!("Received shutdown signal");
        }
    }

    database.close().await;
    info!("Dashboard MCP server shut down");

    Ok(())
}
