"""
tests/conftest.py
Pytest configuration shared across all test files.
"""


def pytest_addoption(parser):
    parser.addoption(
        "--run-integration",
        action="store_true",
        default=False,
        help="Run live integration tests (requires internet + Playwright)",
    )


def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "integration: live network tests (skipped unless --run-integration)",
    )


def pytest_collection_modifyitems(config, items):
    if not config.getoption("--run-integration"):
        import pytest
        skip = pytest.mark.skip(reason="Pass --run-integration to run live tests")
        for item in items:
            if "integration" in item.keywords:
                item.add_marker(skip)
