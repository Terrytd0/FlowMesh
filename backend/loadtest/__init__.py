"""Re-export the load-test pieces.

`backend/scripts/loadtest.py` is the CLI; this is the importable surface, so a
test or a CI job can build a `Budget`, run a shorter version of the same run, and
assert on the report without shelling out.

`run_load_test` lives here rather than only in the script because the thing worth
reusing is "run a load test with these parameters", and duplicating that logic in
a test is how a test ends up measuring something the script does not.
"""

from __future__ import annotations

from backend.loadtest.harness import build_pipeline, drive, reconcile, run_pipeline
from backend.loadtest.metrics_snapshot import snapshot_metrics
from backend.loadtest.report import render_markdown
from backend.scripts.loadtest import Budget, OrderGenerator, run_load_test

__all__ = [
    "Budget",
    "OrderGenerator",
    "build_pipeline",
    "drive",
    "reconcile",
    "render_markdown",
    "run_load_test",
    "run_pipeline",
    "snapshot_metrics",
]
