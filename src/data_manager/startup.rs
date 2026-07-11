use crate::data_manager::database::seed_equity_details;
use crate::data_manager::equity_details::parse_embedded_equity_details;
use crate::data_manager::state::State;

pub async fn migrate_equity_details(state: &State) {
    let pool = match state.database.pool() {
        Some(pool) => pool,
        None => {
            tracing::debug!("No database pool; skipping equity_details migration");
            return;
        }
    };

    match parse_embedded_equity_details() {
        Ok(details) => match seed_equity_details(pool, &details).await {
            Ok(count) if count > 0 => {
                tracing::info!("Seeded equity_details ({} rows)", count);
            }
            Ok(_) => {}
            Err(err) => {
                tracing::warn!("equity_details migration failed: {}", err);
            }
        },
        Err(err) => {
            tracing::warn!("Could not parse embedded equity details: {}", err);
        }
    }
}
