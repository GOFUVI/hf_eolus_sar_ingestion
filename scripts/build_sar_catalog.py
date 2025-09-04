#!/usr/bin/env python3
"""Generate a STAC catalog for SAR GeoParquet assets.

This utility scans GeoParquet files produced by ``upload_SAR.R`` and builds
one STAC collection with an item per Parquet file. Parquets must live under an
``assets`` directory inside the provided root. STAC items are saved under
``items`` and link to assets with relative hrefs.
"""
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

import pyarrow.parquet as pq
from shapely.geometry import box, mapping
import shapely.wkb as wkb
import pystac
from pystac.extensions.table import TableExtension, Column, Table
from pystac.layout import TemplateLayoutStrategy
from parquet_stac_utils import PyarrowS3IO
import traceback

# Prefer explicit STAC validation error if available
try:
    from pystac.errors import STACValidationError  # type: ignore
except Exception:  # pragma: no cover - fallback for older pystac
    STACValidationError = Exception  # type: ignore


# Column definitions for the Table Extension
COLUMN_DEFS = [
    {
        "name": "rowid",
        "description": "Unique row identifier",
        "type": "integer",
    },
    {
        "name": "firstMeasurementTime",
        "description": "Time of the first measurement (UTC)",
        "type": "datetime",
    },
    {
        "name": "lastMeasurementTime",
        "description": "Time of the last measurement (UTC)",
        "type": "datetime",
    },
    {
        "name": "owiLon",
        "description": "Longitude of the pixel center (degrees East)",
        "type": "number",
    },
    {
        "name": "owiLat",
        "description": "Latitude of the pixel center (degrees North)",
        "type": "number",
    },
    {
        "name": "owiWindSpeed",
        "description": "Surface wind speed (m/s)",
        "type": "number",
    },
    {
        "name": "owiWindDirection",
        "description": "Direction of the surface wind vector (degrees clockwise from North)",
        "type": "number",
    },
    {
        "name": "owiMask",
        "description": "Wind field mask",
        "type": "number",
    },
    {
        "name": "owiInversionQuality",
        "description": "Wind inversion quality index",
        "type": "number",
    },
    {
        "name": "owiHeading",
        "description": "Satellite heading (degrees clockwise from North)",
        "type": "number",
    },
    {
        "name": "owiWindQuality",
        "description": "Wind quality flag",
        "type": "number",
    },
    {
        "name": "owiRadVel",
        "description": "Radial wind velocity (m/s)",
        "type": "number",
    },
    {
        "name": "date",
        "description": "Date of the observation (UTC)",
        "type": "date",
    },
    {
        "name": "geometry",
        "description": "Point geometry of the observation in WGS84 encoded as WKB",
        "type": "binary",
    },
]


# RFC3339 formatter that forces a trailing 'Z' for UTC
def _to_rfc3339_z(dt: datetime) -> str:
    if isinstance(dt, datetime):
        dt = dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt.astimezone(timezone.utc)
        s = dt.isoformat()
        # Force trailing 'Z' instead of '+00:00'
        return s[:-6] + "Z" if s.endswith("+00:00") else (s if s.endswith("Z") else s + "Z")
    raise TypeError("_to_rfc3339_z expects a datetime instance")


def _bbox_and_times(parquet_path: Path) -> tuple[
    tuple[float, float, float, float], datetime, datetime, int
]:
    """Return spatial bounds, temporal range, and row count for ``parquet_path``."""

    table = pq.read_table(
        parquet_path, columns=["geometry", "firstMeasurementTime", "lastMeasurementTime"]
    )
    geoms = table.column("geometry").to_pylist()
    first_times = table.column("firstMeasurementTime").to_pylist()
    last_times = table.column("lastMeasurementTime").to_pylist()
    if not geoms:
        raise ValueError(f"No geometry column in {parquet_path}")
    minx = miny = float("inf")
    maxx = maxy = float("-inf")
    for g in geoms:
        geom = wkb.loads(g)
        b = geom.bounds
        minx = min(minx, b[0])
        miny = min(miny, b[1])
        maxx = max(maxx, b[2])
        maxy = max(maxy, b[3])
    start = min(first_times)
    end = max(last_times)
    # Ensure timezone-aware datetimes in UTC so serialization matches STAC RFC3339 pattern
    if isinstance(start, datetime):
        start = start.replace(tzinfo=timezone.utc) if start.tzinfo is None else start.astimezone(timezone.utc)
    if isinstance(end, datetime):
        end = end.replace(tzinfo=timezone.utc) if end.tzinfo is None else end.astimezone(timezone.utc)
    row_count = pq.ParquetFile(parquet_path).metadata.num_rows
    return (minx, miny, maxx, maxy), start, end, row_count


def build_catalog(root: Path, collection_id: str, item_props_path: str | None, collection_props_path: str | None) -> None:
    assets_dir = root / "assets"
    items_dir = root / "items"
    items_dir.mkdir(parents=True, exist_ok=True)

    pystac.StacIO.set_default(PyarrowS3IO)

    item_props = json.load(open(item_props_path)) if item_props_path else {}
    collection_props = json.load(open(collection_props_path)) if collection_props_path else {}

    items = []
    overall_bbox = None
    start_times = []
    end_times = []
    total_rows = 0

    for parquet_file in sorted(assets_dir.rglob("*.parquet")):
        bbox, start, end, row_count = _bbox_and_times(parquet_file)
        geom = box(*bbox)
        rel_href = Path("assets") / parquet_file.relative_to(assets_dir)
        item = pystac.Item(
            id=parquet_file.stem,
            geometry=mapping(geom),
            bbox=list(bbox),
            datetime=start,
            properties={
                **item_props,
                # Prefer 'Z' suffix for UTC (instead of '+00:00')
                "start_datetime": _to_rfc3339_z(start),
                "end_datetime": _to_rfc3339_z(end),
            },
        )
        item.add_asset(
            "data",
            pystac.Asset(href=str(rel_href), media_type=pystac.MediaType.PARQUET, roles=["data"]),
        )
        TableExtension.add_to(item)
        item_ext = TableExtension.ext(item)
        item_ext.columns = [Column(col) for col in COLUMN_DEFS]
        item_ext.primary_geometry = "geometry"
        item_ext.row_count = row_count
        # Validate each item immediately after construction to catch issues early
        try:
            item.validate()
        except Exception as e:
            parts = [f"ERROR: Item validation failed for item ID {item.id}: {e}"]
            if hasattr(e, "errors"):
                parts += [str(err) for err in getattr(e, "errors", [])]
            parts += traceback.format_exc().splitlines()
            print(" | ".join(parts))
            raise e.__class__(f"Validation failed for STAC item '{item.id}': {e}") from e
        items.append(item)
        start_times.append(start)
        end_times.append(end)
        total_rows += row_count
        if overall_bbox is None:
            overall_bbox = list(bbox)
        else:
            overall_bbox = [
                min(overall_bbox[0], bbox[0]),
                min(overall_bbox[1], bbox[1]),
                max(overall_bbox[2], bbox[2]),
                max(overall_bbox[3], bbox[3]),
            ]

    if overall_bbox is None:
        raise RuntimeError("No Parquet files found under assets directory")

    spatial_extent = pystac.SpatialExtent([overall_bbox])
    temporal_extent = pystac.TemporalExtent([[min(start_times), max(end_times)]])

    collection = pystac.Collection(
        id=collection_id,
        description="Synthetic Aperture Radar wind vectors for HF-EOLUS Project area of interest (NW Iberian Peninsula and S Bay of Biscay) derived from Copernicus Sentinel-1 Level-2 OCN OWI products, processed into a GeoParquet dataset. The dataset contains wind speed, direction, and quality flag at approximately 10 m above sea level, along with satellite metadata. Data is provided in daily files covering the period from November 2020 to February 2023. Each file contains point geometries in WGS84 (EPSG:4326) with associated attributes.",
        extent=pystac.Extent(spatial_extent, temporal_extent),
        extra_fields=collection_props,
    )
    TableExtension.add_to(collection)
    coll_ext = TableExtension.ext(collection)
    coll_ext.tables = [
        Table(
            {
                "name": "owi",
                "description": "Sentinel-1 Ocean Wind Field measurements",
                "columns": COLUMN_DEFS,
                "row_count": total_rows,
            }
        ).to_dict()
    ]
    for item in items:
        collection.add_item(item)

    # Defer collection validation until after HREFs are normalized and the catalog is saved,
    # because pre-save links may have unset hrefs (None), which violates the schema.

    # Place collection.json at root and items under items/<id>.json
    layout = TemplateLayoutStrategy(
        collection_template="collection.json", item_template="items/${id}.json"
    )
    collection.normalize_hrefs(str(root), strategy=layout)
    collection.save(catalog_type=pystac.CatalogType.SELF_CONTAINED)

    # Validate again after saving to ensure hrefs and layout are coherent
    try:
        collection.validate()
        for it in collection.get_all_items():
            it.validate()
    except Exception as e:
        parts = [
            f"ERROR: Post-save validation failed for collection '{collection.id}': {e}"
        ]
        if hasattr(e, "errors"):
            parts += [str(err) for err in getattr(e, "errors", [])]
        parts += traceback.format_exc().splitlines()
        print(" | ".join(parts))
        raise e.__class__(
            f"Validation failed after saving STAC catalog for '{collection.id}': {e}"
        ) from e


def main() -> None:
    parser = argparse.ArgumentParser(description="Build a STAC catalog for SAR GeoParquet files")
    parser.add_argument("root", help="Root directory containing assets/")
    parser.add_argument("--collection-id", required=True, dest="collection_id")
    parser.add_argument("--item-properties", dest="item_props")
    parser.add_argument("--collection-properties", dest="collection_props")
    args = parser.parse_args()

    build_catalog(Path(args.root), args.collection_id, args.item_props, args.collection_props)


if __name__ == "__main__":
    main()
