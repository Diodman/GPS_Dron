from __future__ import annotations

import importlib.util
import math
import os
import sys
from dataclasses import dataclass
from contextlib import contextmanager
from types import ModuleType
from typing import Any, Dict, Iterable, List, Optional, Tuple

import networkx as nx


@dataclass
class MapScriptModules:
    data_service: ModuleType
    voronoi_paths: ModuleType
    station_placement: ModuleType


_CACHED: Optional[MapScriptModules] = None


def _load_module_from_path(module_name: str, file_path: str) -> ModuleType:
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Failed to load spec for {module_name} from {file_path}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)  # type: ignore[attr-defined]
    return mod


def _map_script_dir() -> str:
    root = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(root, "Map_script")


@contextmanager
def map_script_import_context(mods: MapScriptModules, map_script_dir: Optional[str] = None):
    """
    Temporarily expose Map_script modules under their expected absolute names.

    Needed because Map_script code performs runtime imports like:
    `from voronoi_paths import ...`
    """
    ms_dir = map_script_dir or _map_script_dir()

    old_modules = {
        "data_service": sys.modules.get("data_service"),
        "voronoi_paths": sys.modules.get("voronoi_paths"),
        "station_placement": sys.modules.get("station_placement"),
    }
    had_path = ms_dir in sys.path
    try:
        if not had_path:
            sys.path.insert(0, ms_dir)
        sys.modules["data_service"] = mods.data_service
        sys.modules["voronoi_paths"] = mods.voronoi_paths
        sys.modules["station_placement"] = mods.station_placement
        yield
    finally:
        if not had_path:
            try:
                sys.path.remove(ms_dir)
            except ValueError:
                pass
        for name, old in old_modules.items():
            if old is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = old


def load_map_script_modules(map_script_dir: Optional[str] = None) -> MapScriptModules:
    """
    Load Map_script modules in isolation.

    Important: Map_script uses absolute imports like `from data_service import DataService`.
    In the main app we already have a different `data_service.py`, so we temporarily
    shadow sys.modules entries to ensure Map_script resolves its own dependencies.
    """
    global _CACHED
    if _CACHED is not None:
        return _CACHED

    ms_dir = map_script_dir or _map_script_dir()
    ds_path = os.path.join(ms_dir, "data_service.py")
    vp_path = os.path.join(ms_dir, "voronoi_paths.py")
    sp_path = os.path.join(ms_dir, "station_placement.py")

    if not (os.path.isfile(ds_path) and os.path.isfile(vp_path) and os.path.isfile(sp_path)):
        raise FileNotFoundError(f"Map_script modules not found in {ms_dir}")

    # Snapshot existing modules to restore after load.
    old_data_service = sys.modules.get("data_service")
    old_voronoi_paths = sys.modules.get("voronoi_paths")
    old_station_placement = sys.modules.get("station_placement")

    try:
        ms_data_service = _load_module_from_path("map_script_data_service", ds_path)
        sys.modules["data_service"] = ms_data_service

        ms_voronoi_paths = _load_module_from_path("map_script_voronoi_paths", vp_path)
        sys.modules["voronoi_paths"] = ms_voronoi_paths

        ms_station_placement = _load_module_from_path("map_script_station_placement", sp_path)
        sys.modules["station_placement"] = ms_station_placement

        _CACHED = MapScriptModules(
            data_service=ms_data_service,
            voronoi_paths=ms_voronoi_paths,
            station_placement=ms_station_placement,
        )
        return _CACHED
    finally:
        # Restore originals so the rest of the app uses its own modules.
        if old_data_service is not None:
            sys.modules["data_service"] = old_data_service
        else:
            sys.modules.pop("data_service", None)

        if old_voronoi_paths is not None:
            sys.modules["voronoi_paths"] = old_voronoi_paths
        else:
            sys.modules.pop("voronoi_paths", None)

        if old_station_placement is not None:
            sys.modules["station_placement"] = old_station_placement
        else:
            sys.modules.pop("station_placement", None)


def _meters_between_latlon(a: Tuple[float, float], b: Tuple[float, float]) -> float:
    (lat1, lon1) = a
    (lat2, lon2) = b
    lat_m = (lat2 - lat1) * 111_000.0
    avg_lat = (lat1 + lat2) / 2.0
    lon_m = (lon2 - lon1) * 111_000.0 * max(0.01, math.cos(math.radians(avg_lat)))
    return float(math.hypot(lat_m, lon_m))


def _line_coords_to_length_m(latlon_coords: List[Tuple[float, float]]) -> float:
    if len(latlon_coords) < 2:
        return 0.0
    total = 0.0
    for i in range(len(latlon_coords) - 1):
        total += _meters_between_latlon(latlon_coords[i], latlon_coords[i + 1])
    return float(total)


def _iter_linestring_features(fc: Dict[str, Any]) -> Iterable[List[List[float]]]:
    feats = list((fc or {}).get("features") or [])
    for f in feats:
        if not isinstance(f, dict):
            continue
        g = f.get("geometry") or {}
        if g.get("type") != "LineString":
            continue
        coords = g.get("coordinates") or []
        if isinstance(coords, list) and len(coords) >= 2:
            yield coords


def build_graph_from_voronoi_fc(
    voronoi_fc: Dict[str, Any],
    *,
    round_ndigits: int = 6,
) -> nx.Graph:
    """
    Convert Map_script Voronoi FeatureCollection (lon/lat LineStrings) to nx.Graph
    with node attr `pos=(lat, lon)` and edge attr `weight` in meters.
    """
    G = nx.Graph()

    def node_key(lat: float, lon: float) -> Tuple[float, float]:
        return (round(float(lat), round_ndigits), round(float(lon), round_ndigits))

    for coords in _iter_linestring_features(voronoi_fc):
        # Map_script uses [lon, lat]
        latlon = []
        for c in coords:
            if not isinstance(c, (list, tuple)) or len(c) < 2:
                continue
            lon = float(c[0])
            lat = float(c[1])
            latlon.append((lat, lon))
        if len(latlon) < 2:
            continue

        a_lat, a_lon = latlon[0]
        b_lat, b_lon = latlon[-1]
        u = node_key(a_lat, a_lon)
        v = node_key(b_lat, b_lon)

        if u not in G:
            G.add_node(u, pos=(u[0], u[1]), type="air_voronoi")
        if v not in G:
            G.add_node(v, pos=(v[0], v[1]), type="air_voronoi")

        w = _line_coords_to_length_m(latlon)
        if w <= 0:
            continue

        # If multiple edges collapse to same endpoints, keep the shortest.
        if G.has_edge(u, v):
            prev = G[u][v].get("weight", float("inf"))
            if w < prev:
                G[u][v]["weight"] = w
        else:
            G.add_edge(u, v, weight=w, type="air_voronoi")

    return G


def extract_charging_stations_latlon(raw_pipeline: Dict[str, Any]) -> List[Tuple[float, float]]:
    """
    Extract charge stations from Map_script pipeline result.
    Returns list of (lat, lon).
    """
    gdf = raw_pipeline.get("charge_stations")
    if gdf is None or len(gdf) == 0:
        return []
    out: List[Tuple[float, float]] = []
    try:
        for _, row in gdf.iterrows():
            geom = row.geometry
            if geom is None or getattr(geom, "is_empty", True):
                continue
            lon = float(getattr(geom, "x"))
            lat = float(getattr(geom, "y"))
            out.append((lat, lon))
    except Exception:
        return out
    return out


def extract_cluster_centroids_latlon(geo: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Extract cluster centroids from Map_script geojson output.
    Returns list of dicts: {cluster_id:str, center:(lat,lon), weight:int, n_buildings:int}.
    """
    fc = (geo or {}).get("cluster_centroids") or {}
    feats = list((fc or {}).get("features") or [])
    out: List[Dict[str, Any]] = []
    for f in feats:
        if not isinstance(f, dict):
            continue
        props = f.get("properties") or {}
        geom = f.get("geometry") or {}
        if geom.get("type") != "Point":
            continue
        coords = geom.get("coordinates") or []
        if not isinstance(coords, (list, tuple)) or len(coords) < 2:
            continue
        try:
            lon = float(coords[0])
            lat = float(coords[1])
        except Exception:
            continue
        cid = props.get("cluster_id")
        if cid is None:
            continue
        try:
            weight = int(props.get("weight", 1) or 1)
        except Exception:
            weight = 1
        try:
            nb = int(props.get("n_buildings", 1) or 1)
        except Exception:
            nb = 1
        out.append(
            {
                "cluster_id": str(cid),
                "center": (lat, lon),
                "weight": max(1, weight),
                "n_buildings": max(1, nb),
            }
        )
    return out


def run_map_script_pipeline(
    city: str,
    *,
    network_type: str = "drive",
    simplify: bool = True,
    dbscan_eps_m: float = 180.0,
    dbscan_min_samples: int = 15,
    use_all_buildings: bool = False,
) -> Dict[str, Any]:
    mods = load_map_script_modules()
    with map_script_import_context(mods):
        ms_ds = mods.data_service.DataService()  # type: ignore[attr-defined]
        # run_full_pipeline returns a rich dict including voronoi_edges and charge_stations gdf
        return mods.station_placement.run_full_pipeline(  # type: ignore[attr-defined]
            ms_ds,
            city.strip(),
            network_type=network_type,
            simplify=simplify,
            demand_method="dbscan",
            demand_cell_m=250.0,
            dbscan_eps_m=float(dbscan_eps_m),
            dbscan_min_samples=int(dbscan_min_samples),
            candidates_per_cluster=25,
            use_all_buildings=bool(use_all_buildings),
            a_by_admin_districts=True,
            voronoi_buildings_per_centroid=60,
            inter_cluster_max_hull_gap_m=2000.0,
            inter_cluster_max_edge_length_m=2000.0,
            voronoi_intra_component_bridge_max_m=600.0,
        )


def build_city_assets_from_map_script(
    city: str,
) -> Tuple[nx.Graph, List[Tuple[float, float]], List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Returns:
    - city graph (nx.Graph) from Voronoi edges (echelon 1 preferred)
    - charging stations [(lat, lon), ...]
    - cluster centroids [(cluster_id, (lat, lon)), ...]
    - flight_levels (list of dicts)
    """
    mods = load_map_script_modules()
    raw = run_map_script_pipeline(city)
    # Convert raw pipeline output to GeoJSON layers (critical: raw stores GeoDataFrames, not FC).
    with map_script_import_context(mods):
        geo = mods.station_placement.pipeline_result_to_geojson(raw)  # type: ignore[attr-defined]

    # Voronoi edges: prefer echelon 1
    vor_fc = (geo or {}).get("voronoi_edges") or {}
    vbe = (geo or {}).get("voronoi_edges_by_echelon") or {}
    if isinstance(vbe, dict) and vbe.get("1"):
        vor_fc = vbe.get("1") or vor_fc
    G = build_graph_from_voronoi_fc(vor_fc if isinstance(vor_fc, dict) else {})

    stations = extract_charging_stations_latlon(raw)
    clusters = extract_cluster_centroids_latlon(geo)
    flight_levels = list((geo or {}).get("flight_levels") or [])
    return G, stations, clusters, flight_levels


def build_city_graph_and_stations_from_map_script(city: str) -> Tuple[nx.Graph, List[Tuple[float, float]]]:
    G, stations, _clusters, _levels = build_city_assets_from_map_script(city)
    return G, stations

