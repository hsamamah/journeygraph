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
from typing import Optional, List

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


def _parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
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
            "Layers to run. Default behavior: runs only the specified layers "
            "(isolated mode). Use --with-deps to include upstream dependencies, "
            "--cascade to include downstream dependents. "
            f"Valid: {[l.value for l in Layer]}. "
            "Default: all layers."
        ),
    )
    parser.add_argument(
        "--with-deps",
        action="store_true",
        help=(
            "Include upstream dependencies. "
            "E.g. --layers fare --with-deps runs [physical, fare]."
        ),
    )
    parser.add_argument(
        "--cascade",
        action="store_true",
        help=(
            "Include downstream dependents. "
            "E.g. --layers service_schedule --cascade runs "
            "[service_schedule, interruption]."
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


def _execution_plan(
    requested: list[Layer],
    *,
    with_deps: bool = False,
    cascade: bool = False,
) -> list[Layer]:
    """Resolve requested layers to an ordered execution plan."""
    return resolve_layers(requested, with_deps=with_deps, cascade=cascade)


def _print_plan(
    plan: list[Layer],
    requested: list[Layer],
    *,
    with_deps: bool = False,
    cascade: bool = False,
) -> None:
    """Log the resolved execution plan clearly."""
    mode_parts = []
    if with_deps:
        mode_parts.append("with-deps")
    if cascade:
        mode_parts.append("cascade")
    mode = f" ({', '.join(mode_parts)})" if mode_parts else " (isolated)"

    log.info("Resolved execution plan%s — %d layer(s):", mode, len(plan))
    for i, layer in enumerate(plan, 1):
        if layer in requested:
            suffix = ""
        elif with_deps:
            suffix = " ← upstream dependency"
        elif cascade:
            suffix = " ← downstream dependent"
        else:
            suffix = ""
        log.info("  %d. %s%s", i, layer.value, suffix)


def _execute_layer(
    layer: Layer,
    gtfs_data: dict,
    neo4j: Neo4jManager,
    config,
) -> None:
    """
    Execute a single layer, importing its module lazily.
    Accessibility and Interruption layers also receive a WMATAClient instance.
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

    if layer in (Layer.ACCESSIBILITY, Layer.INTERRUPTION):
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


def main(argv: Optional[List[str]] = None) -> None:
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

    plan = _execution_plan(
        requested, with_deps=args.with_deps, cascade=args.cascade
    )
    _print_plan(
        plan, requested, with_deps=args.with_deps, cascade=args.cascade
    )

    if args.dry_run:
        log.info("Dry run — exiting without executing")
        return

    _run_pipeline(plan, dry_run=False, force_download=args.force_download)


if __name__ == "__main__":
    main()
