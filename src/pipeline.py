# src/pipeline.py
"""
JourneyGraph ETL Pipeline
=========================
Entry point for loading GTFS static data and WMATA API data into Neo4j.

Usage:
    # Run all layers
    python -m src.pipeline

    # Run specific layers (dependencies resolved automatically)
    python -m src.pipeline --layers fare
    python -m src.pipeline --layers physical fare
    python -m src.pipeline --layers fare accessibility

    # Download GTFS feed first, then run layers
    python -m src.pipeline --download
    python -m src.pipeline --download --layers fare

    # Force re-download even if feed is already cached
    python -m src.pipeline --force-download

    # Dry run — resolve and print execution plan without running
    python -m src.pipeline --layers fare --dry-run

Layer execution order is always determined by dependency resolution,
not by the order arguments are passed. See src/common/layers.py.
"""

import argparse
import sys
import time

from src.common.config import get_config
from src.common.layers import Layer, resolve_layers, validate_layer_names
from src.common.logger import get_logger
from src.common.neo4j_tools import Neo4jManager
from src.ingest.gtfs_loader import load

log = get_logger(__name__)

# Lazy import registry — each layer module is imported only when that layer
# is actually executed. This means unimplemented layers do not cause import
# errors at startup (e.g. when running --download-only or --layers fare).
_LAYER_MODULES: dict[Layer, str] = {
    Layer.PHYSICAL: "src.layers.physical",
    Layer.SERVICE_SCHEDULE: "src.layers.service_schedule",
    Layer.FARE: "src.layers.fare",
    Layer.ACCESSIBILITY: "src.layers.accessibility",
    Layer.INTERRUPTION: "src.layers.interruption",
}


# ── Argument parsing ──────────────────────────────────────────────────────────


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="pipeline",
        description="JourneyGraph ETL pipeline — loads GTFS data into Neo4j",
    )
    parser.add_argument(
        "--layers",
        nargs="+",
        metavar="LAYER",
        default=[l.value for l in Layer],
        help=(
            "Layers to run. Dependencies are resolved automatically. "
            f"Valid: {[l.value for l in Layer]}. "
            "Default: all layers."
        ),
    )
    parser.add_argument(
        "--download",
        action="store_true",
        help=(
            "Download the GTFS feed before running layers. "
            "Skipped if data/gtfs/ already has files unless --force-download is set."
        ),
    )
    parser.add_argument(
        "--force-download",
        action="store_true",
        help="Re-download and re-extract the GTFS feed even if already cached.",
    )
    parser.add_argument(
        "--download-only",
        action="store_true",
        help="Download and extract the GTFS feed then exit without running any layers.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print resolved execution plan without running anything.",
    )
    return parser.parse_args(argv)


# ── Execution ─────────────────────────────────────────────────────────────────


def _execution_plan(requested: list[Layer]) -> list[Layer]:
    """Resolve requested layers to an ordered execution plan."""
    return resolve_layers(requested)


def _print_plan(plan: list[Layer], requested: list[Layer]) -> None:
    """Log the resolved execution plan clearly."""
    auto_added = [l for l in plan if l not in requested]
    log.info("Resolved execution plan (%d layers):", len(plan))
    for i, layer in enumerate(plan, 1):
        suffix = " ← dependency auto-added" if layer in auto_added else ""
        log.info("  %d. %s%s", i, layer.value, suffix)


def _execute_layer(
    layer: Layer,
    gtfs_data: dict,
    neo4j: Neo4jManager,
    config,
) -> None:
    """
    Execute a single layer, importing its module lazily.
    Accessibility is a special case — it also receives a WMATAClient instance.
    """
    import importlib

    module_path = _LAYER_MODULES[layer]
    try:
        module = importlib.import_module(module_path)
    except ModuleNotFoundError as exc:
        raise ModuleNotFoundError(
            f"Layer '{layer.value}' is not yet implemented "
            f"(could not import {module_path}): {exc}"
        ) from exc

    runner = module.run

    if layer == Layer.ACCESSIBILITY:
        from src.ingest.api_client import WMATAClient

        api_client = WMATAClient(api_key=config.wmata_api_key)
        runner(gtfs_data, neo4j, api_client)
    else:
        runner(gtfs_data, neo4j)


def _run_pipeline(
    plan: list[Layer], dry_run: bool = False, force_download: bool = False
) -> None:
    config = get_config()
    neo4j = Neo4jManager(
        uri=config.neo4j_uri,
        user=config.neo4j_user,
        password=config.neo4j_password,
    )

    log.info("Loading GTFS feed")
    gtfs_data = load(
        force_download=force_download,
        force_extract=force_download,
    )
    log.info("GTFS load complete — %d files available", len(gtfs_data))

    results: dict[Layer, str] = {}
    pipeline_start = time.monotonic()

    for layer in plan:
        if dry_run:
            log.info("[dry-run] would execute: %s", layer.value)
            continue

        log.info("─── Starting layer: %s ───", layer.value)
        layer_start = time.monotonic()

        try:
            _execute_layer(layer, gtfs_data, neo4j, config)
            elapsed = time.monotonic() - layer_start
            results[layer] = f"✅  {elapsed:.1f}s"
            log.info("─── Completed layer: %s (%.1fs) ───", layer.value, elapsed)

        except Exception as exc:
            elapsed = time.monotonic() - layer_start
            results[layer] = f"❌  {elapsed:.1f}s — {exc}"
            log.exception("─── FAILED layer: %s (%.1fs) ───", layer.value, elapsed)
            _log_summary(results, plan, time.monotonic() - pipeline_start)
            sys.exit(1)

    _log_summary(results, plan, time.monotonic() - pipeline_start)


def _log_summary(
    results: dict[Layer, str],
    plan: list[Layer],
    total_elapsed: float,
) -> None:
    log.info("═══ Pipeline summary (%.1fs total) ═══", total_elapsed)
    for layer in plan:
        status = results.get(layer, "⏭  skipped")
        log.info("  %-20s %s", layer.value, status)


# ── Entry point ───────────────────────────────────────────────────────────────


def main(argv: list[str] | None = None) -> None:
    args = _parse_args(argv)

    # Validate and resolve layer names from CLI strings
    try:
        requested = validate_layer_names(args.layers)
    except ValueError as exc:
        log.error("%s", exc)
        sys.exit(1)

    # --download-only: fetch feed and exit without running any layers
    if args.download_only:
        from src.ingest.gtfs_loader import load

        log.info("--download-only: fetching GTFS feed")
        data = load(force_download=True, force_extract=True)
        log.info("--download-only complete — %d files available", len(data))
        return

    plan = _execution_plan(requested)
    _print_plan(plan, requested)

    if args.dry_run:
        log.info("Dry run — exiting without executing")
        return

    _run_pipeline(plan, dry_run=False, force_download=args.force_download)


if __name__ == "__main__":
    main()
