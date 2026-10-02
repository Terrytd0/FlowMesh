"""Import the generated protobuf package, then expose the stubs.

Every module that needs `fraud_pb2` imports it from here rather than from the
generated path. The reason is one line of `sys.path` surgery, and that line
needs exactly one home:

`protoc` generates `from flowmesh.v1 import fraud_pb2`, which only resolves if
the directory containing the `flowmesh/` package is on `sys.path`. Every module
that reached past this one to do that itself would be a second copy of the
insertion, and a second copy is a second ordering to reason about when an import
fails.

So: this package adds its *own* directory -- the one holding `flowmesh/` -- to
`sys.path` and re-exports the two modules. Callers write
`from backend.grpc_service import fraud_pb2, fraud_pb2_grpc` and never learn that
the generated code is package-shaped.
"""

from __future__ import annotations

import sys
from pathlib import Path

# `Path(__file__).parent`, not `parent / "generated"`: this module *is* the
# directory that contains `flowmesh/`.
_PACKAGE_ROOT = Path(__file__).resolve().parent
if str(_PACKAGE_ROOT) not in sys.path:  # pragma: no cover - import-time side effect
    sys.path.insert(0, str(_PACKAGE_ROOT))

from flowmesh.v1 import fraud_pb2, fraud_pb2_grpc  # noqa: E402 - after the path fix

__all__ = ["fraud_pb2", "fraud_pb2_grpc"]
