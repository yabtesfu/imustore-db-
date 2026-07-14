"""A tiny, dependency-free metrics registry with Prometheus text exposition.

The Prometheus exposition format is just text, so there is no need to pull in a
client library. Metrics support labels (e.g. ``command="get"``) and render into
the same format a Prometheus server or Grafana Agent scrapes.

A single registry-wide lock guards mutation and rendering, so the admin HTTP
thread can scrape while the server thread records -- without a dict-changed-size
race.
"""

from __future__ import annotations

import threading

DEFAULT_BUCKETS = (0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0)


def _label_key(labels):
    return tuple(sorted(labels.items()))


def _format_labels(label_key, extra=None):
    pairs = list(label_key) + (list(extra) if extra else [])
    if not pairs:
        return ""
    return "{" + ",".join(f'{name}="{value}"' for name, value in pairs) + "}"


class Counter:
    kind = "counter"

    def __init__(self, name, help="", lock=None):
        self.name = name
        self.help = help
        self._lock = lock or threading.Lock()
        self._values = {}

    def inc(self, amount=1, **labels):
        key = _label_key(labels)
        with self._lock:
            self._values[key] = self._values.get(key, 0) + amount

    def value(self, **labels):
        with self._lock:
            return self._values.get(_label_key(labels), 0)

    def _lines(self):
        for key in sorted(self._values):
            yield f"{self.name}{_format_labels(key)} {self._values[key]}"


class Gauge:
    kind = "gauge"

    def __init__(self, name, help="", lock=None):
        self.name = name
        self.help = help
        self._lock = lock or threading.Lock()
        self._values = {}

    def set(self, amount, **labels):
        with self._lock:
            self._values[_label_key(labels)] = amount

    def inc(self, amount=1, **labels):
        key = _label_key(labels)
        with self._lock:
            self._values[key] = self._values.get(key, 0) + amount

    def dec(self, amount=1, **labels):
        self.inc(-amount, **labels)

    def value(self, **labels):
        with self._lock:
            return self._values.get(_label_key(labels), 0)

    def _lines(self):
        for key in sorted(self._values):
            yield f"{self.name}{_format_labels(key)} {self._values[key]}"


class Histogram:
    kind = "histogram"

    def __init__(self, name, help="", buckets=DEFAULT_BUCKETS, lock=None):
        self.name = name
        self.help = help
        self._buckets = tuple(buckets)
        self._lock = lock or threading.Lock()
        self._state = {}  # label_key -> [bucket_counts, sum, count]

    def observe(self, value, **labels):
        key = _label_key(labels)
        with self._lock:
            state = self._state.get(key)
            if state is None:
                state = [[0] * len(self._buckets), 0.0, 0]
                self._state[key] = state
            for i, edge in enumerate(self._buckets):
                if value <= edge:
                    state[0][i] += 1
            state[1] += value
            state[2] += 1

    def _lines(self):
        # observe() already accumulates cumulative bucket counts (it bumps every
        # bucket whose edge is >= the value), so emit them directly.
        for key in sorted(self._state):
            counts, total, count = self._state[key]
            for edge, bucket_count in zip(self._buckets, counts):
                yield f"{self.name}_bucket{_format_labels(key, [('le', edge)])} {bucket_count}"
            yield f"{self.name}_bucket{_format_labels(key, [('le', '+Inf')])} {count}"
            yield f"{self.name}_sum{_format_labels(key)} {total}"
            yield f"{self.name}_count{_format_labels(key)} {count}"


class Registry:
    def __init__(self):
        self._lock = threading.Lock()
        self._metrics = {}

    def _get(self, cls, name, help, **kwargs):
        metric = self._metrics.get(name)
        if metric is None:
            metric = cls(name, help, lock=self._lock, **kwargs)
            self._metrics[name] = metric
        elif not isinstance(metric, cls):
            raise TypeError(f"metric {name!r} already registered as {type(metric).__name__}")
        return metric

    def counter(self, name, help=""):
        return self._get(Counter, name, help)

    def gauge(self, name, help=""):
        return self._get(Gauge, name, help)

    def histogram(self, name, help="", buckets=DEFAULT_BUCKETS):
        return self._get(Histogram, name, help, buckets=buckets)

    def render(self) -> str:
        lines = []
        with self._lock:  # snapshot everything while mutations are held off
            for name in sorted(self._metrics):
                metric = self._metrics[name]
                if metric.help:
                    lines.append(f"# HELP {name} {metric.help}")
                lines.append(f"# TYPE {name} {metric.kind}")
                lines.extend(metric._lines())
        return "\n".join(lines) + "\n"


REGISTRY = Registry()
