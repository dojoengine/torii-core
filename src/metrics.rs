use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use std::sync::OnceLock;

static PROM_HANDLE: OnceLock<PrometheusHandle> = OnceLock::new();

fn env_bool(name: &str, default: bool) -> bool {
    std::env::var(name)
        .ok()
        .and_then(|v| match v.trim().to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => Some(true),
            "0" | "false" | "no" | "off" => Some(false),
            _ => None,
        })
        .unwrap_or(default)
}

/// Initialize Prometheus recorder from environment.
///
/// - `TORII_METRICS_ENABLED`: defaults to `true`
pub fn init_from_env() -> Result<bool, String> {
    if PROM_HANDLE.get().is_some() {
        return Ok(true);
    }
    if !env_bool("TORII_METRICS_ENABLED", true) {
        return Ok(false);
    }

    let handle = PrometheusBuilder::new()
        .install_recorder()
        .map_err(|e| format!("failed to install Prometheus recorder: {e}"))?;

    let _ = PROM_HANDLE.set(handle);
    // Seed always-on series so dashboards can confirm scraping even before ETL traffic.
    ::metrics::gauge!("torii_observability_enabled").set(1.0);
    ::metrics::gauge!("torii_uptime_seconds").set(0.0);
    Ok(true)
}

pub fn render() -> Option<String> {
    PROM_HANDLE.get().map(PrometheusHandle::render)
}

pub fn is_enabled() -> bool {
    PROM_HANDLE.get().is_some()
}

pub fn set_uptime_seconds(seconds: f64) {
    ::metrics::gauge!("torii_uptime_seconds").set(seconds);
}

pub fn set_build_info(version: &str) {
    ::metrics::gauge!("torii_build_info", "version" => version.to_string()).set(1.0);
}
