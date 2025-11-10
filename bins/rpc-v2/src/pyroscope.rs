#[cfg(feature = "pyroscope")]
pub fn init_pyroscope(service_name: &str) {
    use pyroscope::PyroscopeAgent;
    use pyroscope_pprofrs::{pprof_backend, PprofConfig};

    let user = match std::env::var("PYROSCOPE_USER") {
        Ok(s) if !s.is_empty() => s,
        _ => "1107578".to_string(),
    };

    let password = match std::env::var("PYROSCOPE_PASSWORD") {
        Ok(s) if !s.is_empty() => s,
        _ => "...".to_string(),
    };

    let server = match std::env::var("PYROSCOPE_SERVER") {
        Ok(s) if !s.is_empty() => s,
        _ => "https://profiles-prod-001.grafana.net".to_string(),
    };
    let app_name = std::env::var("PYROSCOPE_APP_NAME").unwrap_or_else(|_| service_name.to_string());
    let sample_rate: u32 = std::env::var("PYROSCOPE_SAMPLE_RATE")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(100);

    let backend = pprof_backend(PprofConfig::new().sample_rate(sample_rate).report_thread_name());

    let mut builder = PyroscopeAgent::builder(server, app_name)
        .basic_auth(user, password)
        .backend(backend);
    // Add a basic tag to distinguish services
    builder = builder.tags(vec![("service", service_name)]);

    // Optionally add auth token
    if let Ok(token) = std::env::var("PYROSCOPE_AUTH_TOKEN") {
        if !token.is_empty() {
            builder = builder.auth_token(token);
        }
    }

    match builder.build().and_then(|a| a.start()) {
        Ok(agent) => {
            infinisvm_logger::info!("pyroscope up");
            // Leak the agent to keep it running for process lifetime
            let _ = Box::leak(Box::new(agent));
        }
        Err(e) => {
            eprintln!("Failed to start pyroscope: {e}");
        }
    }
}
