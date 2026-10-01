from main import CORS_ALLOWED_HEADERS, CORS_ALLOWED_METHODS, CORS_ALLOWED_ORIGINS


def test_cors_policy_does_not_use_wildcards():
    assert CORS_ALLOWED_ORIGINS
    assert "*" not in CORS_ALLOWED_ORIGINS
    assert "*" not in CORS_ALLOWED_METHODS
    assert "*" not in CORS_ALLOWED_HEADERS


def test_cors_policy_allows_required_browser_headers():
    assert {"Authorization", "Content-Type", "X-Correlation-ID"} <= set(CORS_ALLOWED_HEADERS)
    assert {"GET", "POST", "PUT", "OPTIONS"} <= set(CORS_ALLOWED_METHODS)
