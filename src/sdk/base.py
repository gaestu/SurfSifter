from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, Iterator, Protocol


@dataclass(slots=True)
class EmittedRecord:
    kind: str
    data: Dict[str, Any]


class Extractor(Protocol):
    name: str

    def run(self, target_files: Iterable[str], context: Dict[str, Any]) -> Iterator[EmittedRecord]:
        ...
