from __future__ import annotations

from relic.core.errors import RelicToolError


class ArcivError(RelicToolError): ...


class ArcivWriterError(ArcivError): ...


class ArcivEncoderError(ArcivError): ...


class ArcivLayoutError(ArcivError): ...
