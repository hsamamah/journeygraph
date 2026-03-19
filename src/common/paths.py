from pathlib import Path


def _find_project_root() -> Path:
    """Walk up from this file until we find pyproject.toml.
    This is robust regardless of where the script is invoked from."""
    for parent in Path(__file__).resolve().parents:
        if (parent / "pyproject.toml").exists():
            return parent
    raise RuntimeError("Could not locate project root (no pyproject.toml found).")


PROJECT_ROOT = _find_project_root()
DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"  # downloaded zips, API dumps
GTFS_DIR = DATA_DIR / "gtfs"  # extracted GTFS CSVs
LOG_DIR = PROJECT_ROOT / "logs"
SLICES_DIR = PROJECT_ROOT / "src" / "llm" / "slices"  # LLM schema slice YAML files


def get_gtfs_path(filename: str) -> Path:
    return GTFS_DIR / filename


def get_raw_path(filename: str) -> Path:
    return RAW_DIR / filename
