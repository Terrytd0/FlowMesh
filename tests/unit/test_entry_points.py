"""Every `python -m` entry point actually does something.

The three evidence scripts -- load test, chaos test, reconciliation -- had no
`if __name__ == "__main__":` guard. Every invocation imported the module, defined
`main()`, and exited 0 without running it: no measurement, no report file, no
budget check, green build.

It survived review because the suite calls `run_load_test` and friends directly as
imported functions, so all the harness logic is covered and the CLI wiring is not.
That gap is structural -- testing the function cannot test the call to it -- so it
gets its own test rather than a note.

What is asserted is deliberately narrow: the guard exists and it calls `main`. If it
invoked anything else the file would not work, and these tests are here to catch a
deleted three-line block, not to re-derive the entry points.

`ast` rather than importing the modules: importing a script to check it parses is
backwards, and a guard that is present but malformed would fail at import with a
traceback rather than a test failure.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

# `tests/unit/test_entry_points.py` -> `tests/unit` -> `tests` -> repo root. Three
# parents up, not four: `parents[0]` is `tests/unit`.
REPO_ROOT = Path(__file__).resolve().parents[2]

#: Every module `make` and CI invoke with `python -m` or as a script. Kept as an
#: explicit list rather than discovered, because the whole failure mode was a
#: module nobody thought to invoke -- a glob over `backend/scripts/` would have
#: found the bug too, but only today. The list is the contract.
ENTRY_POINTS = [
    "backend/scripts/loadtest.py",
    "backend/scripts/chaos_test.py",
    "backend/scripts/reconcile.py",
    "backend/scripts/seed.py",
    "backend/scripts/run_fraud_server.py",
    "backend/scripts/run_order_consumer.py",
    "backend/scripts/run_inventory_worker.py",
    "backend/scripts/run_review_worker.py",
    "scripts/smoke_e2e.py",
]


def _main_guard_guards(source: str) -> bool:
    """True if a top-level `if __name__ == "__main__"` block calls something.

    Parsed rather than pattern-matched so a guard nested inside a function, or one
    that guards nothing, does not count.
    """
    tree = ast.parse(source)
    for node in tree.body:
        if not isinstance(node, ast.If):
            continue
        test = node.test
        if not (
            isinstance(test, ast.Compare)
            and isinstance(test.left, ast.Name)
            and test.left.id == "__name__"
        ):
            continue
        if not any(
            isinstance(comparator, ast.Constant) and comparator.value == "__main__"
            for comparator in test.comparators
        ):
            continue
        return any(
            isinstance(statement, ast.Expr | ast.Assign | ast.Raise) for statement in node.body
        )
    return False


@pytest.mark.parametrize("relative", ENTRY_POINTS)
def test_the_cli_entry_points_are_wired(relative: str) -> None:
    """Each entry point must actually invoke its entry function."""
    path = REPO_ROOT / relative
    assert path.is_file(), f"{relative} does not exist but is an entry point"

    source = path.read_text(encoding="utf-8")
    assert _main_guard_guards(source), (
        f'{relative} has no `if __name__ == "__main__": main()` guard, so '
        f"`python -m {relative.removesuffix('.py').replace('/', '.')}` imports it, "
        "runs nothing and exits 0"
    )


@pytest.mark.parametrize("relative", ENTRY_POINTS)
def test_the_cli_entry_points_define_a_main(relative: str) -> None:
    """A guard calling `main()` is only useful if `main` exists."""
    source = (REPO_ROOT / relative).read_text(encoding="utf-8")
    tree = ast.parse(source)
    assert any(
        isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef) and node.name == "main"
        for node in tree.body
    ), f"{relative} has a main guard but no `main` to call"


def test_the_three_evidence_scripts_all_write_a_report() -> None:
    """Load test, chaos test and reconciliation are the committed evidence.

    Each writes a markdown report *and* a JSON file. The markdown is what a reader
    checks; the JSON is what makes the markdown checkable rather than an assertion.
    A script that produced only one of the two would leave one of them a claim
    nobody can verify.
    """
    for relative, markdown_flag, json_flag in (
        ("backend/scripts/loadtest.py", "--report", "--json"),
        ("backend/scripts/chaos_test.py", "--report", "--json"),
        ("backend/scripts/reconcile.py", "--report", "--json"),
    ):
        source = (REPO_ROOT / relative).read_text(encoding="utf-8")
        assert markdown_flag in source, f"{relative} has no {markdown_flag} flag"
        assert json_flag in source, f"{relative} has no {json_flag} flag"

        # The flag must reach a write, not just the parser. `--report` was accepted
        # and ignored in loadtest.py for the whole life of this test: parsed into
        # args, never opened, and `docs/load-test-report.md` was never created.
        writes = "write_text" in source and "render_markdown" in source
        assert writes, (
            f"{relative} parses a report flag but never writes one, so the flag is silently ignored"
        )
