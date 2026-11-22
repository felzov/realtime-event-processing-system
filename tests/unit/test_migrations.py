import shutil
import subprocess
import sys
from pathlib import Path

from alembic import command
from alembic.script import ScriptDirectory

from event_pipeline.data.tables import EXPECTED_REVISION
from tests.migration_helpers import ALEMBIC_INI, alembic_config

MIGRATIONS = ALEMBIC_INI.parent / "migrations"
PYPROJECT = ALEMBIC_INI.parent / "pyproject.toml"


def test_expected_revision_is_the_only_head() -> None:
    # A second head means two migrations claim the same parent, and `upgrade head` would refuse
    # to run; a different single head means a migration was added without updating the code.
    script = ScriptDirectory.from_config(alembic_config())

    assert script.get_heads() == [EXPECTED_REVISION]


def test_db_revision_template_generates_a_lint_clean_child_of_the_head(tmp_path: Path) -> None:
    # What `make db-revision` writes, generated into a copy so the real versions/ stays untouched.
    shutil.copytree(
        MIGRATIONS, tmp_path / "migrations", ignore=shutil.ignore_patterns("__pycache__")
    )
    config = alembic_config()
    config.set_main_option("script_location", str(tmp_path / "migrations"))

    command.revision(config, message="probe", rev_id="9999")

    script = ScriptDirectory.from_config(config)
    assert script.get_heads() == ["9999"]
    assert script.get_revision("9999").down_revision == EXPECTED_REVISION
    generated = tmp_path / "migrations" / "versions" / "9999_probe.py"
    for ruff_args in (["check"], ["format", "--check"]):
        result = subprocess.run(
            [sys.executable, "-m", "ruff", *ruff_args, "--config", str(PYPROJECT), str(generated)],
            capture_output=True,
            text=True,
            timeout=60,
            check=False,
        )
        assert result.returncode == 0, result.stdout + result.stderr
