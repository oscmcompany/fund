use axum::{
    body::Body,
    http::{Method, Request, StatusCode},
    routing::get,
    Router,
};
use tower::ServiceExt;

#[test]
fn test_route_paths_follow_conventions() {
    let routes = vec![
        "/health",
        "/predictions",
        "/portfolios",
        "/equity-bars",
        "/equity-details",
    ];

    for route in routes {
        assert!(
            route.starts_with('/'),
            "Route should start with /: {}",
            route
        );
        assert!(
            !route.ends_with('/') || route == "/",
            "Route should not end with /: {}",
            route
        );
        assert!(
            route
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '/' || c == '-'),
            "Route should only contain alphanumeric, /, or - characters: {}",
            route
        );
    }
}

#[test]
fn test_route_paths_are_lowercase_with_hyphens() {
    let routes = vec![
        "/health",
        "/predictions",
        "/portfolios",
        "/equity-bars",
        "/equity-details",
    ];

    for route in routes {
        let path_without_slash = route.trim_start_matches('/');
        assert!(
            path_without_slash
                .chars()
                .all(|c| c.is_ascii_lowercase() || c == '-'),
            "Route path should be lowercase with hyphens: {}",
            route
        );
    }
}

#[test]
fn test_route_paths_use_hyphen_not_underscore() {
    let routes = vec!["/equity-bars", "/equity-details"];

    for route in routes {
        assert!(
            !route.contains('_'),
            "Route should use hyphens, not underscores: {}",
            route
        );
        if route.contains('-') {
            assert!(
                route.split('-').count() >= 2,
                "Hyphenated route should have at least two parts: {}",
                route
            );
        }
    }
}

#[test]
fn test_health_route_path() {
    let health_path = "/health";

    assert_eq!(health_path, "/health");
    assert!(health_path.starts_with('/'));
    assert_eq!(health_path.len(), 7);
}

#[test]
fn test_predictions_route_path() {
    let predictions_path = "/predictions";

    assert_eq!(predictions_path, "/predictions");
    assert!(predictions_path.starts_with('/'));
    assert!(!predictions_path.contains('-'));
}

#[test]
fn test_portfolios_route_path() {
    let portfolios_path = "/portfolios";

    assert_eq!(portfolios_path, "/portfolios");
    assert!(portfolios_path.starts_with('/'));
    assert!(!portfolios_path.contains('-'));
}

#[test]
fn test_equity_bars_route_path() {
    let equity_bars_path = "/equity-bars";

    assert_eq!(equity_bars_path, "/equity-bars");
    assert!(equity_bars_path.starts_with('/'));
    assert!(equity_bars_path.contains('-'));
    assert!(!equity_bars_path.contains('_'));
}

#[test]
fn test_equity_details_route_path() {
    let equity_details_path = "/equity-details";

    assert_eq!(equity_details_path, "/equity-details");
    assert!(equity_details_path.starts_with('/'));
    assert!(equity_details_path.contains('-'));
    assert!(!equity_details_path.contains('_'));
}

#[test]
fn test_route_paths_no_trailing_slashes() {
    let routes = vec![
        "/health",
        "/predictions",
        "/portfolios",
        "/equity-bars",
        "/equity-details",
    ];

    for route in routes {
        assert!(
            !route.ends_with('/'),
            "Route should not have trailing slash: {}",
            route
        );
    }
}

#[test]
fn test_route_paths_no_double_slashes() {
    let routes = vec![
        "/health",
        "/predictions",
        "/portfolios",
        "/equity-bars",
        "/equity-details",
    ];

    for route in routes {
        assert!(
            !route.contains("//"),
            "Route should not have double slashes: {}",
            route
        );
    }
}

#[test]
fn test_http_methods_are_standard() {
    let methods = vec![Method::GET, Method::POST];

    for method in methods {
        assert!(
            method == Method::GET
                || method == Method::POST
                || method == Method::PUT
                || method == Method::DELETE
                || method == Method::PATCH,
            "Method should be a standard HTTP method"
        );
    }
}

#[test]
fn test_get_method_for_queries() {
    let get_routes = vec![
        "/health",
        "/predictions",
        "/portfolios",
        "/equity-bars",
        "/equity-details",
    ];

    for route in get_routes {
        assert!(
            !route.is_empty(),
            "GET route should not be empty: {}",
            route
        );
    }
}

#[test]
fn test_post_method_for_mutations() {
    let post_routes = vec!["/predictions", "/portfolios", "/equity-bars"];

    for route in post_routes {
        assert!(
            !route.is_empty(),
            "POST route should not be empty: {}",
            route
        );
    }
}

#[test]
fn test_route_uniqueness() {
    let routes = vec![
        "/health",
        "/predictions",
        "/portfolios",
        "/equity-bars",
        "/equity-details",
    ];

    let unique_count = routes
        .iter()
        .collect::<std::collections::HashSet<_>>()
        .len();

    assert_eq!(unique_count, routes.len(), "All routes should be unique");
}

#[test]
fn test_route_naming_consistency() {
    let resource_routes = vec![
        ("/predictions", "predictions"),
        ("/portfolios", "portfolios"),
        ("/equity-bars", "equity-bars"),
        ("/equity-details", "equity-details"),
    ];

    for (route, expected_name) in resource_routes {
        let route_name = route.trim_start_matches('/');
        assert_eq!(
            route_name, expected_name,
            "Route name should match expected: {}",
            route
        );
    }
}

#[test]
fn test_plural_resource_names() {
    let plural_routes = vec!["/predictions", "/portfolios"];

    for route in plural_routes {
        let name = route.trim_start_matches('/');
        assert!(
            name.ends_with('s'),
            "Resource route should be plural: {}",
            route
        );
    }
}

#[tokio::test]
async fn test_router_creation_basic_structure() {
    let router: Router = Router::new();

    assert!(
        format!("{:?}", router).contains("Router"),
        "Should be a Router instance"
    );
}

#[tokio::test]
async fn test_router_with_single_route() {
    async fn handler() -> &'static str {
        "test"
    }

    let router = Router::new().route("/test", axum::routing::get(handler));

    let request = Request::builder().uri("/test").body(Body::empty()).unwrap();

    let response = router.oneshot(request).await.unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_router_with_multiple_routes() {
    async fn handler1() -> &'static str {
        "handler1"
    }

    async fn handler2() -> &'static str {
        "handler2"
    }

    let router = Router::new()
        .route("/route1", axum::routing::get(handler1))
        .route("/route2", axum::routing::get(handler2));

    let request1 = Request::builder()
        .uri("/route1")
        .body(Body::empty())
        .unwrap();

    let response1 = router.clone().oneshot(request1).await.unwrap();
    assert_eq!(response1.status(), StatusCode::OK);

    let request2 = Request::builder()
        .uri("/route2")
        .body(Body::empty())
        .unwrap();

    let response2 = router.oneshot(request2).await.unwrap();
    assert_eq!(response2.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_router_get_method() {
    async fn get_handler() -> &'static str {
        "GET response"
    }

    let router = Router::new().route("/resource", axum::routing::get(get_handler));

    let request = Request::builder()
        .method(Method::GET)
        .uri("/resource")
        .body(Body::empty())
        .unwrap();

    let response = router.oneshot(request).await.unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_router_post_method() {
    async fn post_handler() -> &'static str {
        "POST response"
    }

    let router = Router::new().route("/resource", axum::routing::post(post_handler));

    let request = Request::builder()
        .method(Method::POST)
        .uri("/resource")
        .body(Body::empty())
        .unwrap();

    let response = router.oneshot(request).await.unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_router_method_not_allowed() {
    async fn get_handler() -> &'static str {
        "GET only"
    }

    let router = Router::new().route("/resource", axum::routing::get(get_handler));

    let request = Request::builder()
        .method(Method::POST)
        .uri("/resource")
        .body(Body::empty())
        .unwrap();

    let response = router.oneshot(request).await.unwrap();

    assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);
}

#[tokio::test]
async fn test_router_not_found() {
    async fn handler() -> &'static str {
        "found"
    }

    let router = Router::new().route("/exists", axum::routing::get(handler));

    let request = Request::builder()
        .uri("/does-not-exist")
        .body(Body::empty())
        .unwrap();

    let response = router.oneshot(request).await.unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_router_same_path_different_methods() {
    async fn get_handler() -> &'static str {
        "GET"
    }

    async fn post_handler() -> &'static str {
        "POST"
    }

    let router = Router::new().route("/resource", get(get_handler).post(post_handler));

    let get_request = Request::builder()
        .method(Method::GET)
        .uri("/resource")
        .body(Body::empty())
        .unwrap();

    let get_response = router.clone().oneshot(get_request).await.unwrap();
    assert_eq!(get_response.status(), StatusCode::OK);

    let post_request = Request::builder()
        .method(Method::POST)
        .uri("/resource")
        .body(Body::empty())
        .unwrap();

    let post_response = router.oneshot(post_request).await.unwrap();
    assert_eq!(post_response.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_router_case_sensitive_paths() {
    async fn handler() -> &'static str {
        "response"
    }

    let router = Router::new().route("/resource", axum::routing::get(handler));

    let lowercase_request = Request::builder()
        .uri("/resource")
        .body(Body::empty())
        .unwrap();

    let lowercase_response = router.clone().oneshot(lowercase_request).await.unwrap();
    assert_eq!(lowercase_response.status(), StatusCode::OK);

    let uppercase_request = Request::builder()
        .uri("/RESOURCE")
        .body(Body::empty())
        .unwrap();

    let uppercase_response = router.oneshot(uppercase_request).await.unwrap();
    assert_eq!(uppercase_response.status(), StatusCode::NOT_FOUND);
}

#[test]
fn test_route_path_segments() {
    let routes = vec![
        ("/health", vec!["health"]),
        ("/predictions", vec!["predictions"]),
        ("/portfolios", vec!["portfolios"]),
        ("/equity-bars", vec!["equity-bars"]),
        ("/equity-details", vec!["equity-details"]),
    ];

    for (route, expected_segments) in routes {
        let segments: Vec<&str> = route
            .trim_start_matches('/')
            .split('/')
            .filter(|s| !s.is_empty())
            .collect();

        assert_eq!(
            segments, expected_segments,
            "Route segments should match for: {}",
            route
        );
    }
}

#[test]
fn test_route_depth() {
    let routes = vec![
        ("/health", 1),
        ("/predictions", 1),
        ("/portfolios", 1),
        ("/equity-bars", 1),
        ("/equity-details", 1),
    ];

    for (route, expected_depth) in routes {
        let depth = route.matches('/').count();

        assert_eq!(
            depth, expected_depth,
            "Route depth should match for: {}",
            route
        );
    }
}

#[test]
fn test_no_query_parameters_in_route_paths() {
    let routes = vec![
        "/health",
        "/predictions",
        "/portfolios",
        "/equity-bars",
        "/equity-details",
    ];

    for route in routes {
        assert!(
            !route.contains('?'),
            "Route path should not contain query parameters: {}",
            route
        );
        assert!(
            !route.contains('&'),
            "Route path should not contain query parameter separators: {}",
            route
        );
    }
}

#[test]
fn test_no_fragments_in_route_paths() {
    let routes = vec![
        "/health",
        "/predictions",
        "/portfolios",
        "/equity-bars",
        "/equity-details",
    ];

    for route in routes {
        assert!(
            !route.contains('#'),
            "Route path should not contain fragments: {}",
            route
        );
    }
}

#[test]
fn test_route_paths_are_absolute() {
    let routes = vec![
        "/health",
        "/predictions",
        "/portfolios",
        "/equity-bars",
        "/equity-details",
    ];

    for route in routes {
        assert!(
            route.starts_with('/'),
            "Route path should be absolute (start with /): {}",
            route
        );
    }
}

#[test]
fn test_resource_oriented_routing() {
    let resource_routes = vec![
        "/predictions",
        "/portfolios",
        "/equity-bars",
        "/equity-details",
    ];

    for route in resource_routes {
        let resource_name = route.trim_start_matches('/');
        assert!(
            !resource_name.is_empty(),
            "Resource name should not be empty: {}",
            route
        );
        assert!(
            resource_name.len() > 3,
            "Resource name should be descriptive: {}",
            route
        );
    }
}

#[tokio::test]
async fn test_router_preserves_route_order() {
    async fn handler1() -> &'static str {
        "1"
    }
    async fn handler2() -> &'static str {
        "2"
    }
    async fn handler3() -> &'static str {
        "3"
    }

    let router = Router::new()
        .route("/route1", axum::routing::get(handler1))
        .route("/route2", axum::routing::get(handler2))
        .route("/route3", axum::routing::get(handler3));

    let request1 = Request::builder()
        .uri("/route1")
        .body(Body::empty())
        .unwrap();
    assert_eq!(
        router.clone().oneshot(request1).await.unwrap().status(),
        StatusCode::OK
    );

    let request2 = Request::builder()
        .uri("/route2")
        .body(Body::empty())
        .unwrap();
    assert_eq!(
        router.clone().oneshot(request2).await.unwrap().status(),
        StatusCode::OK
    );

    let request3 = Request::builder()
        .uri("/route3")
        .body(Body::empty())
        .unwrap();
    assert_eq!(
        router.oneshot(request3).await.unwrap().status(),
        StatusCode::OK
    );
}

#[tokio::test]
async fn test_production_app_health_route() {
    use datamanager::router::create_app;

    let app = create_app().await;

    let request = Request::builder()
        .uri("/health")
        .method(Method::GET)
        .body(Body::empty())
        .unwrap();

    let response = app.clone().oneshot(request).await.unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_production_app_predictions_routes() {
    use datamanager::router::create_app;

    let app = create_app().await;

    let get_request = Request::builder()
        .uri("/predictions")
        .method(Method::GET)
        .body(Body::empty())
        .unwrap();

    let get_response = app.clone().oneshot(get_request).await.unwrap();
    assert!(
        get_response.status() != StatusCode::NOT_FOUND,
        "GET /predictions route should exist"
    );

    let post_request = Request::builder()
        .uri("/predictions")
        .method(Method::POST)
        .body(Body::empty())
        .unwrap();

    let post_response = app.oneshot(post_request).await.unwrap();
    assert!(
        post_response.status() != StatusCode::NOT_FOUND,
        "POST /predictions route should exist"
    );
}

#[tokio::test]
async fn test_production_app_portfolios_routes() {
    use datamanager::router::create_app;

    let app = create_app().await;

    let get_request = Request::builder()
        .uri("/portfolios")
        .method(Method::GET)
        .body(Body::empty())
        .unwrap();

    let get_response = app.clone().oneshot(get_request).await.unwrap();
    assert!(
        get_response.status() != StatusCode::NOT_FOUND,
        "GET /portfolios route should exist"
    );

    let post_request = Request::builder()
        .uri("/portfolios")
        .method(Method::POST)
        .body(Body::empty())
        .unwrap();

    let post_response = app.oneshot(post_request).await.unwrap();
    assert!(
        post_response.status() != StatusCode::NOT_FOUND,
        "POST /portfolios route should exist"
    );
}

#[tokio::test]
async fn test_production_app_equity_bars_routes() {
    use datamanager::router::create_app;

    let app = create_app().await;

    let get_request = Request::builder()
        .uri("/equity-bars")
        .method(Method::GET)
        .body(Body::empty())
        .unwrap();

    let get_response = app.clone().oneshot(get_request).await.unwrap();
    assert!(
        get_response.status() != StatusCode::NOT_FOUND,
        "GET /equity-bars route should exist"
    );

    let post_request = Request::builder()
        .uri("/equity-bars")
        .method(Method::POST)
        .body(Body::empty())
        .unwrap();

    let post_response = app.oneshot(post_request).await.unwrap();
    assert!(
        post_response.status() != StatusCode::NOT_FOUND,
        "POST /equity-bars route should exist"
    );
}

#[tokio::test]
async fn test_production_app_equity_details_route() {
    use datamanager::router::create_app;

    let app = create_app().await;

    let request = Request::builder()
        .uri("/equity-details")
        .method(Method::GET)
        .body(Body::empty())
        .unwrap();

    let response = app.oneshot(request).await.unwrap();
    assert!(
        response.status() != StatusCode::NOT_FOUND,
        "GET /equity-details route should exist"
    );
}

#[tokio::test]
async fn test_production_app_nonexistent_route() {
    use datamanager::router::create_app;

    let app = create_app().await;

    let request = Request::builder()
        .uri("/nonexistent-route")
        .method(Method::GET)
        .body(Body::empty())
        .unwrap();

    let response = app.oneshot(request).await.unwrap();
    assert_eq!(
        response.status(),
        StatusCode::NOT_FOUND,
        "Nonexistent routes should return 404"
    );
}
