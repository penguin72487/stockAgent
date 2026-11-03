"""Shared FinMind owner errors, including when an owner runs with ``python -m``."""


class SourceError(RuntimeError):
    def __init__(self, code: str, *, retry_after: int = 300) -> None:
        super().__init__(code)
        self.code = code
        self.retry_after = retry_after
