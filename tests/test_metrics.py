"""Tests for the metrics registry and the admin HTTP endpoint."""

import json
import urllib.error
import urllib.request

from imustore.admin import AdminServer
from imustore.metrics import Registry


def test_counter_and_gauge_render_prometheus():
    registry = Registry()
    hits = registry.counter("http_requests_total", "requests")
    hits.inc(command="get")
    hits.inc(command="get")
    hits.inc(command="set")
    connections = registry.gauge("connections", "open connections")
    connections.set(3)

    text = registry.render()
    assert "# TYPE http_requests_total counter" in text
    assert 'http_requests_total{command="get"} 2' in text
    assert 'http_requests_total{command="set"} 1' in text
    assert "connections 3" in text
    assert hits.value(command="get") == 2


def test_histogram_renders_buckets_sum_and_count():
    registry = Registry()
    latency = registry.histogram("latency_seconds", "latency", buckets=(0.01, 0.1, 1.0))
    for value in (0.005, 0.05, 0.5):
        latency.observe(value)

    text = registry.render()
    assert 'latency_seconds_bucket{le="0.01"} 1' in text
    assert 'latency_seconds_bucket{le="0.1"} 2' in text
    assert 'latency_seconds_bucket{le="+Inf"} 3' in text
    assert "latency_seconds_count 3" in text
    assert f"latency_seconds_sum {0.005 + 0.05 + 0.5}" in text  # same float accumulation as the code


def _get(base, path):
    with urllib.request.urlopen(f"{base}{path}", timeout=3) as response:
        return response.status, response.read().decode("utf-8")


def test_admin_server_serves_metrics_health_and_stats():
    registry = Registry()
    registry.counter("widgets_total", "widgets").inc(5)
    admin = AdminServer(registry, stats=lambda: {"keys": 42}, host="127.0.0.1", port=0)
    host, port = admin.start()
    base = f"http://{host}:{port}"
    try:
        status, body = _get(base, "/metrics")
        assert status == 200 and "widgets_total 5" in body

        status, body = _get(base, "/healthz")
        assert status == 200 and body.strip() == "ok"

        status, body = _get(base, "/stats")
        assert status == 200 and json.loads(body) == {"keys": 42}
    finally:
        admin.stop()


def test_admin_stats_error_is_reported_as_500():
    admin = AdminServer(Registry(), stats=lambda: 1 / 0, host="127.0.0.1", port=0)
    host, port = admin.start()
    try:
        code = None
        try:
            _get(f"http://{host}:{port}", "/stats")
        except urllib.error.HTTPError as exc:
            code = exc.code
        assert code == 500  # a broken stats callable returns 500, not a dropped socket
    finally:
        admin.stop()


def test_admin_server_unknown_path_is_404():
    admin = AdminServer(Registry(), host="127.0.0.1", port=0)
    host, port = admin.start()
    try:
        try:
            _get(f"http://{host}:{port}", "/nope")
            raised = False
        except urllib.error.HTTPError as exc:
            raised = exc.code == 404
        assert raised
    finally:
        admin.stop()
