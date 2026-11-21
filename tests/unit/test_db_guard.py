"""The guard that keeps integration tests away from any database not named `*_test`."""

import pytest

from tests.integration.db_helpers import NotATestDatabaseError, require_test_database

PASSWORD = "pw-MARKER-0d2e"  # gitleaks:allow


def test_test_database_is_accepted() -> None:
    url = f"postgresql+psycopg://u:{PASSWORD}@127.0.0.1:5432/event_pipeline_test"

    assert require_test_database(url) == url


@pytest.mark.parametrize(
    "url",
    [
        f"postgresql+psycopg://u:{PASSWORD}@127.0.0.1:5432/event_pipeline",
        f"postgresql+psycopg://u:{PASSWORD}@127.0.0.1:5432/test_event_pipeline",
        f"postgresql+psycopg://u:{PASSWORD}@127.0.0.1:5432",
        f"garbage {PASSWORD}",
        f"postgresql+psycopg://u:{PASSWORD}",
        # libpq takes these from the query string over the path: the connection would land in
        # event_pipeline although the path says _test.
        f"postgresql+psycopg://u:{PASSWORD}@127.0.0.1:5432/x_test?dbname=event_pipeline",
        f"postgresql+psycopg://u:{PASSWORD}@127.0.0.1:5432/x_test?service=dev",
    ],
    ids=[
        "development_database",
        "test_prefix_only",
        "no_database",
        "unparseable",
        "port_not_a_number",
        "query_overrides_database",
        "query_names_a_service",
    ],
)
def test_other_databases_are_refused_without_echoing_the_url(url: str) -> None:
    with pytest.raises(NotATestDatabaseError) as exc_info:
        require_test_database(url)

    assert PASSWORD not in str(exc_info.value)
