from observability.metrics import Registry, route_label


def test_counter_gauge_histogram_render():
    r = Registry()
    c = r.counter("t_requests_total", "requests", ["code"])
    c.inc(code="200")
    c.inc(2, code="200")
    g = r.gauge("t_free_bytes", "free", ["node"], fn=lambda: [({"node": "n1"}, 5)])
    h = r.histogram("t_latency_seconds", "lat", [], buckets=(0.1, 1))
    h.observe(0.05)
    h.observe(0.5)
    text = r.render()
    assert 't_requests_total{code="200"} 3' in text
    assert 't_free_bytes{node="n1"} 5' in text
    assert 't_latency_seconds_bucket{le="0.1"} 1' in text
    assert 't_latency_seconds_bucket{le="+Inf"} 2' in text
    assert "t_latency_seconds_count 2" in text
    assert "# TYPE t_requests_total counter" in text
    assert g.kind == "gauge"


def test_route_labels_have_low_cardinality():
    assert route_label("/buckets/photos/objects/a/b/c.jpg") == "/buckets/{bucket}/objects/{key}"
    assert route_label("/v1/chunks/" + "a" * 64) == "/v1/chunks/{hash}"
    assert route_label("/uploads/0b8e/parts/3") == "/uploads/{id}/parts/{n}"
    assert route_label("/cluster/status") == "/cluster/status"
    assert route_label("/v1/files/123e4567-e89b-12d3-a456-426614174000") == "/v1/files/{id}"
