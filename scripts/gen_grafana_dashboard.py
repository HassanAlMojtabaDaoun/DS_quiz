"""Generate the Grafana dashboard (monitoring/grafana/dashboards/storage.json).

    python scripts/gen_grafana_dashboard.py
"""
import json
from pathlib import Path

DS = {"type": "prometheus", "uid": "prometheus"}
panels = []


def place(w, h):
    """Simple flow layout on Grafana's 24-column grid."""
    x, y, row_h = place.state
    if x + w > 24:
        x, y, row_h = 0, y + row_h, 0
    pos = {"x": x, "y": y, "w": w, "h": h}
    place.state = (x + w, y, max(row_h, h))
    return pos


place.state = (0, 0, 0)


def stat(title, expr, unit="none", thresholds=None, desc=""):
    steps = thresholds or [{"color": "green", "value": None}]
    panels.append({
        "type": "stat", "title": title, "description": desc, "datasource": DS,
        "gridPos": place(4, 4),
        "targets": [{"refId": "A", "expr": expr, "datasource": DS}],
        "fieldConfig": {"defaults": {"unit": unit, "thresholds": {
            "mode": "absolute", "steps": steps}}, "overrides": []},
        "options": {"reduceOptions": {"calcs": ["lastNotNull"]},
                    "colorMode": "background", "graphMode": "area"},
    })


def series(title, targets, unit="none", w=12, h=8, desc="", stack=False):
    panels.append({
        "type": "timeseries", "title": title, "description": desc, "datasource": DS,
        "gridPos": place(w, h),
        "targets": [{"refId": chr(65 + i), "expr": e, "legendFormat": legend,
                     "datasource": DS} for i, (e, legend) in enumerate(targets)],
        "fieldConfig": {"defaults": {"unit": unit, "custom": {
            "lineWidth": 2, "fillOpacity": 15,
            "stacking": {"mode": "normal" if stack else "none"}}}, "overrides": []},
        "options": {"legend": {"displayMode": "table", "placement": "bottom",
                               "calcs": ["lastNotNull", "max"]},
                    "tooltip": {"mode": "multi"}},
    })


RED = [{"color": "green", "value": None}, {"color": "red", "value": 1}]
stat("Storage nodes up", "sum(ds_node_up)", thresholds=[
    {"color": "red", "value": None}, {"color": "orange", "value": 3},
    {"color": "green", "value": 4}])
stat("Ring members", "ds_ring_members")
stat("Objects", "ds_objects_total")
stat("Degraded objects", "ds_objects_degraded", thresholds=[
    {"color": "green", "value": None}, {"color": "orange", "value": 1}])
stat("Unavailable objects", "ds_objects_unavailable", thresholds=RED)
stat("Queued job batches", "ds_jobs_pending_batches")

series("API throughput (requests/s) per gateway",
       [('sum by (instance) (rate(ds_http_requests_total{service="gateway"}[1m]))',
         "{{instance}}")], "reqps",
       desc="Requests handled by each gateway instance behind the load balancer")
series("Data throughput through the gateways",
       [('sum(rate(ds_http_request_bytes_total{service="gateway"}[1m]))', "upload"),
        ('sum(rate(ds_http_response_bytes_total{service="gateway"}[1m]))', "download")],
       "Bps")
series("Gateway latency",
       [(f'histogram_quantile({q}, sum by (le) (rate(ds_http_request_duration_seconds_bucket'
         f'{{service="gateway"}}[5m])))', f"p{int(q * 100)}") for q in (0.5, 0.95, 0.99)],
       "s")
series("Gateway responses by status",
       [('sum by (status) (rate(ds_http_requests_total{service="gateway"}[1m]))',
         "{{status}}"),
        ('sum(rate(ds_gateway_rate_limited_total[1m]))', "rate limited (429)")],
       "reqps")
series("Disk usage per storage node",
       [("(ds_node_total_bytes - ds_node_free_bytes) / ds_node_total_bytes",
         "{{node}}")], "percentunit")
series("Data stored per storage node",
       [("ds_node_stored_bytes", "{{node}}")], "bytes", stack=True)
series("Degraded and unavailable objects",
       [("ds_objects_degraded", "degraded objects"),
        ("ds_chunks_degraded", "degraded chunks"),
        ("ds_objects_unavailable", "unavailable objects")])
series("Repair rate (bytes/s)",
       [("rate(ds_repair_bytes_written_total[1m])", "written {{scheme}}"),
        ("rate(ds_repair_bytes_read_total[1m])", "read {{scheme}}")], "Bps",
       desc="Repair traffic per redundancy scheme")
series("Chunks repaired per second",
       [("rate(ds_repair_chunks_repaired_total[1m])", "{{scheme}}")], "ops")
series("Worker batches per second",
       [("sum by (instance, type) (rate(ds_worker_batches_total[1m]))",
         "{{instance}} {{type}}")], "ops",
       desc="Job batches (repair / rebalance / gc) executed by each worker")
series("Storage node availability",
       [("ds_node_up", "{{node}}")], w=12)
series("Storage node requests/s",
       [('sum by (instance) (rate(ds_http_requests_total{service="storage-node"}[1m]))',
         "{{instance}}")], "reqps")

dashboard = {
    "title": "Distributed Storage - Overview", "uid": "ds-storage",
    "tags": ["storage"], "timezone": "browser", "schemaVersion": 39,
    "version": 1, "refresh": "5s", "time": {"from": "now-30m", "to": "now"},
    "panels": [dict(p, id=i + 1) for i, p in enumerate(panels)],
    "templating": {"list": []}, "annotations": {"list": []},
}
for out in (Path("monitoring/grafana/dashboards/storage.json"),
            Path("k8s/monitoring/storage.json")):
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(dashboard, indent=2))
    print("wrote", out)
