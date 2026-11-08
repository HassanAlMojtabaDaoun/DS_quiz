"""Minimal Prometheus metrics (text exposition format 0.0.4), no dependencies.

Every service exposes GET /metrics. instrument_app() adds an ASGI
middleware that records, for each request:
    ds_http_requests_total{service, method, route, status}
    ds_http_request_duration_seconds{service, method, route}  (histogram)
    ds_http_request_bytes_total / ds_http_response_bytes_total{service}
`route` is the path with ids/keys replaced (low label cardinality).
"""

import math
import re
import threading
import time

DEFAULT_BUCKETS = (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60)


def _escape(v) -> str:
    return str(v).replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')


def _labels(names, values) -> str:
    if not names:
        return ""
    return "{" + ",".join(f'{n}="{_escape(v)}"' for n, v in zip(names, values)) + "}"


def _num(v) -> str:
    if v == math.inf:
        return "+Inf"
    return repr(float(v)) if isinstance(v, float) else str(v)


class _Metric:
    def __init__(self, name, help_, labels, kind):
        self.name, self.help, self.labelnames, self.kind = name, help_, tuple(labels), kind
        self._values = {}
        self._lock = threading.Lock()

    def _key(self, labels):
        return tuple(str(labels.get(n, "")) for n in self.labelnames)

    def header(self):
        return [f"# HELP {self.name} {self.help}", f"# TYPE {self.name} {self.kind}"]


class Counter(_Metric):
    def __init__(self, name, help_, labels=()):
        super().__init__(name, help_, labels, "counter")

    def inc(self, amount=1, **labels):
        key = self._key(labels)
        with self._lock:
            self._values[key] = self._values.get(key, 0) + amount

    def render(self):
        with self._lock:
            items = list(self._values.items())
        return self.header() + [f"{self.name}{_labels(self.labelnames, k)} {_num(v)}"
                                for k, v in items]


class Gauge(_Metric):
    def __init__(self, name, help_, labels=(), fn=None):
        super().__init__(name, help_, labels, "gauge")
        self.fn = fn               # optional: () -> [(labels_dict, value)]

    def set(self, value, **labels):
        with self._lock:
            self._values[self._key(labels)] = value

    def render(self):
        if self.fn is not None:
            try:
                items = [(self._key(lbl), val) for lbl, val in self.fn()]
            except Exception:
                items = []
        else:
            with self._lock:
                items = list(self._values.items())
        return self.header() + [f"{self.name}{_labels(self.labelnames, k)} {_num(v)}"
                                for k, v in items if v is not None]


class Histogram(_Metric):
    def __init__(self, name, help_, labels=(), buckets=DEFAULT_BUCKETS):
        super().__init__(name, help_, labels, "histogram")
        self.buckets = tuple(sorted(buckets)) + (math.inf,)

    def observe(self, value, **labels):
        key = self._key(labels)
        with self._lock:
            counts, total = self._values.get(key, ([0] * len(self.buckets), [0.0, 0]))
            for i, b in enumerate(self.buckets):
                if value <= b:
                    counts[i] += 1
            total[0] += value
            total[1] += 1
            self._values[key] = (counts, total)

    def render(self):
        out = self.header()
        with self._lock:
            items = [(k, (list(c), list(t))) for k, (c, t) in self._values.items()]
        names = self.labelnames + ("le",)
        for key, (counts, (s, n)) in items:
            for b, c in zip(self.buckets, counts):
                out.append(f"{self.name}_bucket{_labels(names, key + (_num(b),))} {c}")
            out.append(f"{self.name}_sum{_labels(self.labelnames, key)} {_num(s)}")
            out.append(f"{self.name}_count{_labels(self.labelnames, key)} {n}")
        return out


class Registry:
    def __init__(self):
        self._metrics = {}

    def _add(self, metric):
        return self._metrics.setdefault(metric.name, metric)

    def counter(self, name, help_, labels=()):
        return self._add(Counter(name, help_, labels))

    def gauge(self, name, help_, labels=(), fn=None):
        return self._add(Gauge(name, help_, labels, fn))

    def histogram(self, name, help_, labels=(), buckets=DEFAULT_BUCKETS):
        return self._add(Histogram(name, help_, labels, buckets))

    def render(self) -> str:
        lines = []
        for m in list(self._metrics.values()):
            lines += m.render()
        return "\n".join(lines) + "\n"


REGISTRY = Registry()

HTTP_REQUESTS = REGISTRY.counter("ds_http_requests_total", "HTTP requests",
                                 ["service", "method", "route", "status"])
HTTP_LATENCY = REGISTRY.histogram("ds_http_request_duration_seconds",
                                  "HTTP request duration (until the last byte)",
                                  ["service", "method", "route"])
HTTP_IN = REGISTRY.counter("ds_http_request_bytes_total", "Request body bytes",
                           ["service"])
HTTP_OUT = REGISTRY.counter("ds_http_response_bytes_total", "Response body bytes",
                            ["service"])

_ROUTES = [
    (r"^/v1/objects/[^/]+/.+", "/v1/objects/{bucket}/{key}"),
    (r"^/v1/objects/[^/]+$", "/v1/objects/{bucket}"),
    (r"^/v1/(meta|tags)/[^/]+/.+", r"/v1/\1/{bucket}/{key}"),
    (r"^/v1/chunks/[^/]+$", "/v1/chunks/{hash}"),
    (r"^/v1/files/[^/]+/download$", "/v1/files/{id}/download"),
    (r"^/v1/files/[^/]+$", "/v1/files/{id}"),
    (r"^/v1/uploads/[^/]+/parts/\d+$", "/v1/uploads/{id}/parts/{n}"),
    (r"^/v1/uploads/[^/]+/complete$", "/v1/uploads/{id}/complete"),
    (r"^/v1/uploads/[^/]+$", "/v1/uploads/{id}"),
    (r"^/v1/buckets/[^/]+/stats$", "/v1/buckets/{bucket}/stats"),
    (r"^/v1/buckets/[^/]+$", "/v1/buckets/{bucket}"),
    (r"^/v1/(api-keys|admin/nodes)/[^/]+$", r"/v1/\1/{id}"),
    (r"^/v1/debug/corrupt/.+", "/v1/debug/corrupt/{hash}"),
    (r"^/buckets/[^/]+/(objects|metadata|tags)/.+", r"/buckets/{bucket}/\1/{key}"),
    (r"^/buckets/[^/]+/(objects|uploads|stats)$", r"/buckets/{bucket}/\1"),
    (r"^/buckets/[^/]+$", "/buckets/{bucket}"),
    (r"^/uploads/[^/]+/parts/\d+$", "/uploads/{id}/parts/{n}"),
    (r"^/uploads/[^/]+/complete$", "/uploads/{id}/complete"),
    (r"^/uploads/[^/]+$", "/uploads/{id}"),
    (r"^/admin/(api-keys|nodes)/[^/]+$", r"/admin/\1/{id}"),
]
_ROUTES = [(re.compile(p), r) for p, r in _ROUTES]
_ID = re.compile(r"/([0-9a-f]{16,}|[0-9a-f-]{36}|\d+)(?=/|$)")


def route_label(path: str) -> str:
    for pattern, repl in _ROUTES:
        if pattern.match(path):
            return pattern.sub(repl, path)
    path = _ID.sub("/{id}", path)
    return path if len(path) <= 60 else path[:60]


class MetricsMiddleware:
    """Pure ASGI middleware (streams are not buffered)."""

    def __init__(self, app, service: str):
        self.app, self.service = app, service

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http" or scope["path"] == "/metrics":
            return await self.app(scope, receive, send)
        start = time.perf_counter()
        method, route = scope["method"], route_label(scope["path"])
        status = {"code": 500}
        service = self.service

        async def counting_receive():
            message = await receive()
            if message["type"] == "http.request":
                HTTP_IN.inc(len(message.get("body", b"")), service=service)
            return message

        async def counting_send(message):
            if message["type"] == "http.response.start":
                status["code"] = message["status"]
            elif message["type"] == "http.response.body":
                HTTP_OUT.inc(len(message.get("body", b"")), service=service)
            await send(message)

        try:
            await self.app(scope, counting_receive, counting_send)
        finally:
            HTTP_REQUESTS.inc(service=service, method=method, route=route,
                              status=str(status["code"]))
            HTTP_LATENCY.observe(time.perf_counter() - start, service=service,
                                 method=method, route=route)


def instrument_app(app, service: str) -> None:
    app.add_middleware(MetricsMiddleware, service=service)
