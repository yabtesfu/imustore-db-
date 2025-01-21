from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class AuditReport:
    keys: int
    nodes: int
    values: int
    height: int
    errors: tuple[str, ...] = ()

    @property
    def ok(self) -> bool:
        return not self.errors

    def as_dict(self) -> dict[str, object]:
        return {
            "keys": self.keys,
            "nodes": self.nodes,
            "values": self.values,
            "height": self.height,
            "ok": self.ok,
            "errors": list(self.errors),
        }
