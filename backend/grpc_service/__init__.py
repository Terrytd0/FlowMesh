"""The typed service boundary: gRPC stubs, the server, and the clients.

Re-exports the generated protobuf modules so that no module in this project
imports `flowmesh.v1` directly. `backend/grpc_service/generated/__init__.py` owns
the one `sys.path` insertion that protoc's package-shaped imports need, and this
package is the only place that reaches into `generated/` for anything else.

Use `from backend.grpc_service import fraud_pb2, fraud_pb2_grpc` and
`from backend.grpc_service.client import ...`; do not import the generated
modules by their generated path.
"""

from __future__ import annotations

from backend.grpc_service.generated import fraud_pb2, fraud_pb2_grpc

__all__ = ["fraud_pb2", "fraud_pb2_grpc"]
