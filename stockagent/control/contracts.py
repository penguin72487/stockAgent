"""Immutable engineering-work identity, separate from market/PIT contracts."""

from __future__ import annotations

from dataclasses import dataclass
import re
from types import MappingProxyType
from typing import Any, Mapping

from stockagent.runtime_identity import identity_sha256

WORK_CONTRACT_VERSION = 1
SUPPORTED_KINDS = frozenset({'verify-code-release'})
_NAME = re.compile(r'[a-zA-Z0-9][a-zA-Z0-9_.:-]{0,127}\Z')
_SHA = re.compile(r'[0-9a-f]{64}\Z')


def identifier(value: str) -> str:
    if not isinstance(value, str) or not _NAME.fullmatch(value):
        raise ValueError('work/node/worker identifier must be 1..128 safe characters')
    return value


def integer(value: Any, name: str, *, minimum: int = 0, maximum: int | None = None) -> int:
    if type(value) is not int or value < minimum or (maximum is not None and value > maximum):
        raise ValueError(f'invalid {name}')
    return value


@dataclass(frozen=True)
class WorkSpec:
    key: str
    inputs: Mapping[str, str]
    kind: str = 'verify-code-release'
    dependencies: tuple[str, ...] = ()
    priority: int = 0
    max_attempts: int = 2
    cpu_slots: int = 1
    memory_bytes: int = 256 * 1024 * 1024
    scratch_bytes: int = 0

    def __post_init__(self):
        identifier(self.key)
        if self.kind not in SUPPORTED_KINDS:
            raise ValueError('unsupported work kind; arbitrary commands are not work contracts')
        if not isinstance(self.inputs, Mapping) or set(self.inputs) != {'receipt_sha256', 'source_sha256'}:
            raise ValueError('verify-code-release requires exactly receipt_sha256 and source_sha256')
        for value in self.inputs.values():
            if not isinstance(value, str) or not _SHA.fullmatch(value):
                raise ValueError('work inputs require exact lowercase SHA256 identities')
        if not isinstance(self.dependencies, tuple) or len(set(self.dependencies)) != len(self.dependencies):
            raise ValueError('dependencies must be a unique tuple')
        for name in self.dependencies:
            identifier(name)
            if name == self.key:
                raise ValueError('work cannot depend on itself')
        integer(self.priority, 'priority', maximum=100)
        integer(self.max_attempts, 'max_attempts', minimum=1, maximum=10)
        integer(self.cpu_slots, 'cpu_slots', minimum=1, maximum=1024)
        integer(self.memory_bytes, 'memory_bytes', minimum=1, maximum=2**60)
        integer(self.scratch_bytes, 'scratch_bytes', maximum=2**60)
        # A frozen dataclass must not retain a caller-owned mutable identity.
        object.__setattr__(self, 'inputs', MappingProxyType(dict(sorted(self.inputs.items()))))

    def as_dict(self) -> dict[str, Any]:
        return {'schema_version': WORK_CONTRACT_VERSION, 'key': self.key, 'kind': self.kind,
                'inputs': dict(self.inputs), 'dependencies': list(self.dependencies),
                'priority': self.priority, 'max_attempts': self.max_attempts,
                'resources': {'cpu_slots': self.cpu_slots, 'memory_bytes': self.memory_bytes,
                              'scratch_bytes': self.scratch_bytes}}

    @property
    def identity_sha256(self) -> str:
        return identity_sha256(self.as_dict())

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> WorkSpec:
        required = {'schema_version', 'key', 'kind', 'inputs', 'dependencies', 'priority',
                    'max_attempts', 'resources'}
        if (not isinstance(value, Mapping) or set(value) != required
                or type(value['schema_version']) is not int or value['schema_version'] != WORK_CONTRACT_VERSION):
            raise ValueError('unsupported or incomplete work contract')
        resources = value['resources']
        if not isinstance(resources, Mapping) or set(resources) != {'cpu_slots', 'memory_bytes', 'scratch_bytes'}:
            raise ValueError('unsupported resource contract')
        if not isinstance(value['dependencies'], list):
            raise ValueError('dependencies must be a list')
        return cls(key=value['key'], inputs=value['inputs'], kind=value['kind'],
                   dependencies=tuple(value['dependencies']), priority=value['priority'],
                   max_attempts=value['max_attempts'], **resources)
