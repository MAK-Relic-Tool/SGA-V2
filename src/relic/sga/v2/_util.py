from __future__ import annotations

from typing import Any, Optional


def _repr_name(t: Any) -> str: # pragma: nocover # Its a __repr__
    klass = t.__class__
    module = klass.__module__
    return ".".join([module, klass.__qualname__])


def _repr_obj(self: Any, *args: str, name: Optional[str] = None, **kwargs: Any) -> str: # pragma: nocover # Its a __repr__
    klass_name = _repr_name(self)
    for arg in args:
        kwargs[arg] = getattr(self, arg)
    kwarg_line = ", ".join(f"{k}='{v}'" for k, v in kwargs.items())
    if len(kwarg_line) > 0:
        kwarg_line = f" ({kwarg_line})"  # space at start to avoid if below
    if name is None:
        return f"<{klass_name}{kwarg_line}>"
    return f"<{klass_name} '{name}'{kwarg_line}>"
