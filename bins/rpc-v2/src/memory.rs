#[cfg(feature = "track_oom")]
pub fn init_jemalloc_profiling() {
    use axum::{http::StatusCode, response::IntoResponse};

    std::thread::Builder::new()
        .name("seqPprof".to_string())
        .spawn(move || {
            let runtime = tokio::runtime::Runtime::new().unwrap();
            runtime.block_on(async {
                pub async fn handle_get_heap() -> Result<impl IntoResponse, (StatusCode, String)> {
                    let mut prof_ctl = jemalloc_pprof::PROF_CTL.as_ref().unwrap().lock().await;
                    require_profiling_activated(&prof_ctl)?;
                    let pprof = prof_ctl
                        .dump_pprof()
                        .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))?;
                    Ok(pprof)
                }

                /// Checks whether jemalloc profiling is activated an
                /// returns an error response if not.
                fn require_profiling_activated(
                    prof_ctl: &jemalloc_pprof::JemallocProfCtl,
                ) -> Result<(), (StatusCode, String)> {
                    if prof_ctl.activated() {
                        Ok(())
                    } else {
                        Err((axum::http::StatusCode::FORBIDDEN, "heap profiling not activated".into()))
                    }
                }

                pub async fn handle_get_heap_flamegraph() -> Result<impl IntoResponse, (StatusCode, String)> {
                    use axum::{body::Body, http::header::CONTENT_TYPE, response::Response};

                    let mut prof_ctl = jemalloc_pprof::PROF_CTL.as_ref().unwrap().lock().await;
                    require_profiling_activated(&prof_ctl)?;
                    let svg = prof_ctl
                        .dump_flamegraph()
                        .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))?;
                    Response::builder()
                        .header(CONTENT_TYPE, "image/svg+xml")
                        .body(Body::from(svg))
                        .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))
                }

                let a = jemalloc_pprof::PROF_CTL.as_ref();
                if a.is_some() {
                    println!("Jemalloc profiling activated");
                } else {
                    println!("Jemalloc profiling not activated");
                }

                let app = axum::Router::new()
                    .route("/debug/pprof/heap", axum::routing::get(handle_get_heap))
                    .route(
                        "/debug/pprof/heap/flamegraph",
                        axum::routing::get(handle_get_heap_flamegraph),
                    );
                let listener = tokio::net::TcpListener::bind("127.0.0.1:3000").await.unwrap();
                axum::serve(listener, app).await.unwrap();
            });
        })
        .unwrap();
}
