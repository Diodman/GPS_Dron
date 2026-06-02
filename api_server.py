from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import asyncio
import logging
from typing import Dict, List, Any, Optional, Tuple
from pydantic import BaseModel
import math
import time
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, Response
import os
import base64
import json
import hashlib
from pathlib import Path
from datetime import datetime
from redis import Redis
import numpy as np

from city_data_service import DataService as CityDataService
from data_service import DataService as InfraDataService
from graph_service import GraphService
from routing_service import RoutingService, MODE_EMPTY, MODE_LOADED
from station_placement import pipeline_result_to_geojson, run_full_pipeline
from voronoi_paths import build_voronoi_local_paths_fc

import networkx as nx
from scipy.spatial import cKDTree
from shapely.geometry import shape as shapely_shape, Point as ShapelyPoint
from shapely.prepared import prep as shapely_prep

logger = logging.getLogger(__name__)

# Planning constants (single source: routing_service has defaults)
# 20% резерв: нельзя использовать больше 80% на полезный маршрут (требование бизнес-логики)
RESERVE_PCT = 20.0
# Запас при планировании заказа (для грузового уже учтён CARGO_RESERVE_AFTER_DELIVERY_PCT)
PLAN_RESERVE_PCT = 20.0
# В тестовом режиме — высокий запас, маршрут через одну или несколько станций
PLAN_RESERVE_PCT_TEST = 55.0
# Макс. доля батареи на один сегмент без зарядки (80% usable): если сегмент «съедает» больше — строим через станции
MAX_SEGMENT_BATTERY_PCT = 80.0
# Допустимая доля батареи на перелёт до зарядки: можно «дотянуть» до станции с меньшим запасом (после посадки — 100%)
MAX_BATTERY_PCT_TO_REACH_CHARGER = 85.0
CHARGER_ARRIVAL_MIN_PCT = 5.0
# Порог заряда (%): при достижении дрон летит на станцию зарядки (если свободен или держит груз)
FLY_TO_CHARGER_AT_PCT = 20.0
# У грузового дрона после доставки должен оставаться минимум 20% заряда
CARGO_RESERVE_AFTER_DELIVERY_PCT = 20.0
STATION_NEAR_METERS = 20.0
# Станция зарядки: запас заряженных аккумуляторов (смена вместо зарядки на месте)
STATION_CHARGED_BATTERIES_MAX = 20
# Тиков на зарядку одного аккумулятора после смены
STATION_BATTERY_CHARGE_TICKS = 25
# Оценка: тиков на одну остановку на зарядку (база: 4% за тик, с 20% до 100% ≈ 20 тиков). 1 тик ≈ 1 сек.
CHARGE_TICKS_ESTIMATE_PER_STOP = 20
# Порог завершения зарядки: избегаем зависаний на 99.x% из-за плавающей точности.
CHARGE_COMPLETE_PCT = 99.0
# Скорость по умолчанию для оценки ETA (м/с), если ветер неизвестен
DEFAULT_SPEED_MPS = 12.0

# Infrastructure routing optimization knobs
MAX_NEAREST_STATIONS = 8
MAX_NEAREST_NETWORK_NODES = 12
MAX_ROUTE_CANDIDATES = 20
MAX_DRONE_ASSIGNMENT_CANDIDATES = 60
# Strongly prefer infrastructure "air highways" (trunk/branch/voronoi) over off-network legs.
# This makes routes mostly follow colored infrastructure lines between charging stations.
OFF_NETWORK_PENALTY = 4.0
# Short air-gap legs: start/end only to local|voronoi|branch nodes when infrastructure is nearby (m).
OFF_NETWORK_MAX_ENTRY_EXIT_M = 1200.0
# Only snap rendered/telemetry segments to infrastructure when endpoints are
# practically on the same infrastructure nodes. Pickup/dropoff legs may leave
# highways and fly free, so a wide snap radius makes them look like highway hops.
OFF_NETWORK_NEAR_INFRA_CONNECT_M = 80.0
# Legacy cap for rare city_graph escape segments (not preferred infra routing).
OFF_NETWORK_MAX_M = 3200.0
ECHELON_CHANGE_PENALTY_M = 250.0
OFF_NETWORK_LONG_PENALTY_M = 1400.0
# If free flight is clearly shorter than following the infrastructure graph,
# use it for pickup/dropoff legs. Cargo drones are allowed to leave airways.
FREE_FLIGHT_PREFER_RATIO = 0.85
# Scoring: off-network legs are expensive vs trunk (aligns with edge weight multipliers).
OFF_NETWORK_LEG_WEIGHT_MULT = 8.0
OFF_NETWORK_MAX_FRACTION_OF_TOTAL = 0.30
LONG_ROUTE_MIN_M = 1500.0
LONG_ROUTE_MIN_TRUNK_RATIO = 0.5
INFRA_ROUTE_SCORE_REJECT = 1e18

# Веса рёбер infrastructure_graph: cost = length_m * factor(layer); off_leg — множитель для стыков start/end.
INFRA_DRONE_PROFILES: Dict[str, Dict[str, Any]] = {
	"cargo": {
		"trunk": 0.35,
		"branch": 0.70,
		"local": 1.00,
		"voronoi": 1.00,
		"off_leg": 10.0,
		"require_trunk_on_graph": True,
		"require_long_route_trunk_ratio": True,
	},
	"operator": {
		"trunk": 0.60,
		"branch": 0.80,
		"local": 0.90,
		"voronoi": 0.90,
		"off_leg": 6.0,
		"require_trunk_on_graph": False,
		"require_long_route_trunk_ratio": False,
	},
	"cleaner": {
		"trunk": 1.00,
		"branch": 0.90,
		"local": 0.70,
		"voronoi": 0.70,
		"off_leg": 4.0,
		"require_trunk_on_graph": False,
		"require_long_route_trunk_ratio": False,
	},
}


def _normalized_drone_type(drone_type: str) -> str:
	t = str(drone_type or "").lower().strip()
	if t in INFRA_DRONE_PROFILES:
		return t
	return "cargo"


def _infra_layer_factor_for_drone(layer: str, drone_type: str) -> float:
	prof = INFRA_DRONE_PROFILES[_normalized_drone_type(drone_type)]
	lay = (layer or "").lower()
	if lay == "trunk":
		return float(prof["trunk"])
	if lay == "branch":
		return float(prof["branch"])
	if lay == "local":
		return float(prof["local"])
	if lay == "voronoi":
		return float(prof["voronoi"])
	return float(prof.get("default", 1.15))


def _infra_edge_weight_m(edge_attr: Dict[str, Any], drone_type: str) -> float:
	Lm = float((edge_attr or {}).get("length_m") or 0.0)
	if Lm <= 0.0:
		return 1e9
	return Lm * _infra_layer_factor_for_drone(str((edge_attr or {}).get("layer") or ""), drone_type)


# UI sanity limits are handled on the frontend (render caps).
# Do NOT cap station placement in the pipeline here: it breaks A/B balance and downstream local/voronoi networks.

# Failure reasons for plan_order_trip
NO_PATH_TO_PICKUP = "NO_PATH_TO_PICKUP"
NO_FEASIBLE_CHARGING_CHAIN_TO_PICKUP = "NO_FEASIBLE_CHARGING_CHAIN_TO_PICKUP"
NO_FEASIBLE_CHAIN_PICKUP_TO_DROPOFF_LOADED = "NO_FEASIBLE_CHAIN_PICKUP_TO_DROPOFF_LOADED"
NO_ESCAPE_AFTER_DROPOFF = "NO_ESCAPE_AFTER_DROPOFF"

app = FastAPI(title="Drone Planner API", version="0.2.0")
app.add_middleware(
	CORSMiddleware,
	allow_origins=["*"],
	allow_credentials=True,
	allow_methods=["*"],
	allow_headers=["*"],
)

# Mount static only when folder exists (avoid startup crash / 404 due to missing dir).
_static_dir = Path(__file__).resolve().parent / "static"
if _static_dir.is_dir():
	app.mount("/static", StaticFiles(directory=str(_static_dir)), name="static")

# Минимальная 1×1 PNG (прозрачный пиксель) — чтобы не было 404 на /favicon.ico
FAVICON_PNG = base64.b64decode(
	"iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8z8BQDwAEhQGAhKmMIQAAAABJRU5ErkJggg=="
)

@app.get("/favicon.ico", include_in_schema=False)
async def favicon():
	return Response(content=FAVICON_PNG, media_type="image/png")

# Services
_data_service = CityDataService()
_infra_data_service = InfraDataService()
_graph_service = GraphService()
_routing_service = RoutingService(_graph_service)

# Redis
REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
try:
    _redis: Redis | None = Redis.from_url(REDIS_URL, encoding="utf-8", decode_responses=True)
    _redis.ping()
except Exception:
    _redis = None
    logger.warning("Redis is not available; running without persistence")

# Models
class LoadCityRequest(BaseModel):
	city: str
	drone_type: str = "cargo"
	force_rebuild: bool = False

class AddOrderRequest(BaseModel):
	address_from: Optional[str] = None
	address_to: Optional[str] = None
	coords_from: Optional[List[float]] = None  # [lat, lon]
	coords_to: Optional[List[float]] = None
	type_hint: Optional[str] = None  # delivery|shooting|work
	# Явный тип дрона: cargo | operator | cleaner. Если не задан — выводится из type_hint.
	drone_type: Optional[str] = None
	# Приоритет (больше = выше приоритет в очереди). По умолчанию 5.
	priority: int = 5
	battery_level: float = 100
	waypoints: Optional[List[List[float]]] = None  # optional waypoint coords [ [lat,lon], ... ]
	# Для операторского дрона: область облёта (лассо) — список точек [ [lat,lon], ... ]
	area_polygon: Optional[List[List[float]]] = None  # [ [lat,lon], ... ]

class NoFlyZone(BaseModel):
	# Simple rectangular zone for MVP: {lat_min, lat_max, lon_min, lon_max}
	lat_min: float
	lat_max: float
	lon_min: float
	lon_max: float
	id: Optional[str] = None

# State
STATE: Dict[str, Any] = {
	"city": None,
	"city_graph": None,
	"orders": [],
	"drones": {},  # drone_id -> {pos, type, battery, route, target_idx, status}
	"no_fly_zones": [],
	"clients": set(),  # websockets
	"zone_version": 0,
	"weather": {"wind_mps": 3.0},
    "base": None,  # base location lat, lon
	"inventory": {},  # type -> count available at base
	"stations": [],  # list of (lat, lon)
    "station_queues": {},  # station_index -> {charging:[], queue:[], capacity:int} (legacy for base) or {charged_batteries:int, charging_queue:[ticks], queue:[drone_id]}
    "base_queue": {"charging": [], "queue": [], "capacity": 2},
	"charger_nodes": {"base": None, "stations": []},
	"battery_mode": "reality",  # "reality" | "test" — в тесте укороченная дальность для проверки маршрутов

	# Infrastructure (pipeline from station_placement/voronoi_paths)
	"infrastructure": None,         # normalized dict for logic (stations/paths/clusters/echelons)
	"infrastructure_geojson": None, # raw geojson payload for UI layers
	"infrastructure_graph": None,   # nx.Graph for routing over infrastructure
	"infrastructure_version": 0,

	# Infrastructure build status (anti-duplication)
	"infrastructure_build_status": "idle",  # idle|building|ready|error
	"infrastructure_build_city": None,
	"infrastructure_build_started_at": None,
	"infrastructure_build_finished_at": None,
	"infrastructure_build_last_error": None,
	"infrastructure_build_stage": None,
	"infrastructure_build_stage_detail": None,
	"infrastructure_build_stage_ts": None,
	# cache buster for placement cache keys (increment to ignore old saved placement)
	"placement_cache_buster": 0,
	# Filled by rebuild_infrastructure_for_city; used by /api/infrastructure/debug and readiness checks
	"infrastructure_graph_counts": {},
	"infrastructure_graph_invalid": False,
	"last_infra_route_diagnostics": {},
}

_broadcast_task: Optional[asyncio.Task] = None
_scheduler_task: Optional[asyncio.Task] = None

# Helpers
# Краткое описание погоды по коду WMO (Open-Meteo)
def _weather_code_to_desc(code: int) -> str:
	codes = {
		0: "Ясно", 1: "Преим. ясно", 2: "Переменная облачность", 3: "Пасмурно",
		45: "Туман", 48: "Изморозь", 51: "Морось", 53: "Морось", 55: "Морось",
		61: "Дождь", 63: "Дождь", 65: "Сильный дождь", 66: "Ледяной дождь", 67: "Ливень",
		71: "Снег", 73: "Снег", 75: "Снегопад", 77: "Снежные зёрна",
		80: "Ливень", 81: "Ливень", 82: "Ливень", 85: "Снег", 86: "Снегопад",
		95: "Гроза", 96: "Гроза с градом", 99: "Гроза с градом",
	}
	return codes.get(code, "—")


def _fetch_weather_sync(lat: float, lon: float) -> Optional[Dict[str, Any]]:
	"""Синхронный запрос погоды с Open-Meteo (бесплатный API, без ключа)."""
	try:
		import requests
		url = "https://api.open-meteo.com/v1/forecast"
		params = {
			"latitude": lat, "longitude": lon,
			"current": "relative_humidity_2m,weather_code,wind_speed_10m",
		}
		r = requests.get(url, params=params, timeout=5.0)
		r.raise_for_status()
		data = r.json()
		cur = data.get("current") or {}
		wind_kmh = float(cur.get("wind_speed_10m", 0) or 0)
		wind_mps = round(wind_kmh / 3.6, 1)
		humidity = int(cur.get("relative_humidity_2m", 0) or 0)
		code = int(cur.get("weather_code", 0) or 0)
		return {
			"wind_mps": max(0, min(40, wind_mps)),
			"humidity": max(0, min(100, humidity)),
			"weather_code": code,
			"description": _weather_code_to_desc(code),
		}
	except Exception as e:
		logger.warning("fetch_weather_from_api failed: %s", e)
		return None


async def fetch_weather_from_api(lat: float, lon: float) -> Optional[Dict[str, Any]]:
	return await asyncio.to_thread(_fetch_weather_sync, lat, lon)


def _station_states_for_ui() -> List[Dict[str, Any]]:
	"""Список по индексам станций: {charged, charging} для отображения в UI."""
	stations = STATE.get("stations") or []
	sqs = STATE.get("station_queues") or {}
	result = []
	for i in range(len(stations)):
		sq = sqs.get(str(i), {})
		if isinstance(sq, dict) and "charged_batteries" in sq:
			result.append({
				"charged": int(sq.get("charged_batteries", 0)),
				"charging": len(sq.get("charging_queue", [])),
			})
		else:
			result.append({"charged": STATION_CHARGED_BATTERIES_MAX, "charging": 0})
	return result


async def broadcast_state():
	payload = {
		"city": STATE["city"],
		"orders": STATE["orders"],
		"drones": STATE["drones"],
		"histories": {k: v.get("history", []) for k,v in STATE["drones"].items()},
		"no_fly_zones": STATE["no_fly_zones"],
		"stations": STATE.get("stations", []),
		"station_states": _station_states_for_ui(),
		"weather": STATE.get("weather", {}),
		"battery_mode": STATE.get("battery_mode", "reality"),
		"infrastructure_version": int(STATE.get("infrastructure_version") or 0),
	}
	dead: List[WebSocket] = []
	for ws in list(STATE["clients"]):
		try:
			await ws.send_json(payload)
		except Exception:
			dead.append(ws)
	for ws in dead:
		STATE["clients"].discard(ws)

async def broadcaster_loop():
	while True:
		await broadcast_state()
		await asyncio.sleep(1.0)

async def scheduler_loop():
	# very simple loop: assign queued orders, move drones, reroute on zones
	last_tick_log = 0.0
	while True:
		try:
			now = time.time()
			if now - last_tick_log >= 2.0:
				queued = sum(1 for o in (STATE.get("orders") or []) if o.get("status") == "queued")
				free = sum(
					1
					for _id, d in (STATE.get("drones") or {}).items()
					if (d or {}).get("status") in (None, "idle")
				)
				logger.info("scheduler_tick orders_queued=%s drones_free=%s infra_ready=%s", queued, free, is_infrastructure_routing_ready())
				last_tick_log = now
			if is_infrastructure_routing_ready():
				await assign_orders()
			simulate_step()
		except Exception:
			logger.exception("scheduler_loop error")
		# periodic save
		try:
			await persist_state()
		except Exception:
			logger.exception("persist_state error")
		await asyncio.sleep(1.0)

async def delayed_scheduler_loop():
	# Let ASGI startup finish and bind the HTTP socket before potentially heavy order planning.
	await asyncio.sleep(1.0)
	await scheduler_loop()

@app.on_event("startup")
async def on_startup():
	global _broadcast_task, _scheduler_task
	await restore_state()
	# Попытка автозагрузки погоды на базе, чтобы UI не показывал только "ветер=по умолчанию".
	try:
		w = STATE.get("weather") or {}
		base = STATE.get("base")
		needs_weather = (w.get("description") is None) or (w.get("humidity") is None)
		if needs_weather and base and isinstance(base, (list, tuple)) and len(base) == 2:
			lat, lon = float(base[0]), float(base[1])
			data = await fetch_weather_from_api(lat, lon)
			if data:
				STATE["weather"] = data
				await persist_state()
	except Exception:
		logger.exception("auto weather fetch failed")
	# initialize drones at base from inventory if needed
	try:
		ensure_base_drones()
	except Exception:
		logger.exception("ensure_base_drones on startup failed")
	if _broadcast_task is None or _broadcast_task.done():
		_broadcast_task = asyncio.create_task(broadcaster_loop())
	if _scheduler_task is None or _scheduler_task.done():
		_scheduler_task = asyncio.create_task(delayed_scheduler_loop())

@app.on_event("shutdown")
async def on_shutdown():
	global _broadcast_task, _scheduler_task
	await persist_state()
	for task in (_broadcast_task, _scheduler_task):
		if task and not task.done():
			task.cancel()
	_broadcast_task = None
	_scheduler_task = None

# API endpoints
@app.post("/api/load_city")
async def load_city(body: LoadCityRequest):
	try:
		if body.force_rebuild:
			logger.info("load_city rebuild requested city=%s force_rebuild=true", body.city)
			STATE["placement_cache_buster"] = int(STATE.get("placement_cache_buster") or 0) + 1
			_clear_infrastructure_runtime()
		# City graph uses CityDataService (geocoding/legacy graph) which may not support force_refresh.
		city_data = _data_service.get_city_data(body.city)
		# inject current API no-fly zones into data prior to build
		city_data['no_fly_zones'] = list(STATE["no_fly_zones"]) or city_data.get('no_fly_zones', [])
		city_graph = _graph_service.build_city_graph(city_data, body.drone_type)
		_routing_service.city_graphs[body.city] = city_graph
		STATE["city"] = body.city
		STATE["city_graph"] = city_graph
		STATE["drone_type"] = body.drone_type
		# Ensure we have a base/inventory so drones exist even while infrastructure is building.
		if not STATE.get("base"):
			# Volgograd default (project-wide convention)
			STATE["base"] = (48.7080, 44.5133)
		if not isinstance(STATE.get("inventory"), dict) or not STATE.get("inventory"):
			STATE["inventory"] = {"cargo": 30, "operator": 10, "cleaner": 10}
		# Build UAV infrastructure right after city load (anti-dup; may run in background)
		_ensure_infrastructure_build(body.city, force=bool(body.force_rebuild))
		refresh_charger_nodes()
		ensure_base_drones()
		await persist_state()
		return {"ok": True, "stats": {
			"nodes": len(city_graph.nodes),
			"edges": len(city_graph.edges)
		}}
	except Exception as e:
		logger.exception("load_city failed")
		return JSONResponse(status_code=500, content={"ok": False, "error": str(e)})

@app.get("/api/state")
async def get_state():
	return {
		"city": STATE["city"],
		"orders_count": len(STATE["orders"]),
		"drones_count": len(STATE["drones"]),
		"no_fly_zones": STATE["no_fly_zones"],
		"weather": STATE["weather"],
		"base": STATE["base"],
		"inventory": STATE["inventory"],
		"stations": STATE["stations"],
		"station_states": _station_states_for_ui(),
		"battery_mode": STATE.get("battery_mode", "reality"),
		"infrastructure_version": int(STATE.get("infrastructure_version") or 0),
	}


@app.get("/api/infrastructure")
async def get_infrastructure():
	"""Normalized infrastructure payload used by server logic and UI toggles."""
	return {
		"ok": True,
		"city": STATE.get("city"),
		"version": int(STATE.get("infrastructure_version") or 0),
		"build_status": STATE.get("infrastructure_build_status"),
		"build_city": STATE.get("infrastructure_build_city"),
		"last_error": STATE.get("infrastructure_build_last_error"),
		"infrastructure": STATE.get("infrastructure"),
	}


@app.get("/api/infrastructure/geojson")
async def get_infrastructure_geojson():
	"""GeoJSON layers for Leaflet UI."""
	return {
		"ok": True,
		"city": STATE.get("city"),
		"version": int(STATE.get("infrastructure_version") or 0),
		"build_status": STATE.get("infrastructure_build_status"),
		"build_city": STATE.get("infrastructure_build_city"),
		"last_error": STATE.get("infrastructure_build_last_error"),
		"geojson": STATE.get("infrastructure_geojson"),
	}


@app.get("/api/infrastructure/debug")
async def infrastructure_debug():
	geo = STATE.get("infrastructure_geojson") or {}
	G = STATE.get("infrastructure_graph")
	def _examples(fc):
		if isinstance(fc, dict) and fc.get("type") == "FeatureCollection":
			return (fc.get("features") or [])[:3]
		return []
	drones = STATE.get("drones") or {}
	by_type = {"cargo":0,"operator":0,"cleaner":0}
	for _id, d in drones.items():
		t = (d or {}).get("type")
		if t in by_type:
			by_type[t] += 1
	return {
		"ok": True,
		"city": STATE.get("city"),
		"build_status": STATE.get("infrastructure_build_status"),
		"build_city": STATE.get("infrastructure_build_city"),
		"version": int(STATE.get("infrastructure_version") or 0),
		"pipeline_params": STATE.get("infrastructure_pipeline_params") or _pipeline_params_app_defaults(),
		"raw_counts": STATE.get("infrastructure_raw_counts") or {},
		"started_at": STATE.get("infrastructure_build_started_at"),
		"finished_at": STATE.get("infrastructure_build_finished_at"),
		"last_error": STATE.get("infrastructure_build_last_error"),
		"build_stage_hint": {
			"stage": STATE.get("infrastructure_build_stage"),
			"detail": STATE.get("infrastructure_build_stage_detail"),
			"ts": STATE.get("infrastructure_build_stage_ts"),
		},
		"build_elapsed_s": (
			(datetime.utcnow() - datetime.fromisoformat(STATE["infrastructure_build_started_at"])).total_seconds()
			if STATE.get("infrastructure_build_started_at")
			else None
		),
		"geojson_keys": sorted(list(geo.keys())) if isinstance(geo, dict) else [],
		"ui_drawn_keys_hint": sorted([k for k in (geo.keys() if isinstance(geo, dict) else []) if isinstance(geo.get(k), dict) and geo.get(k, {}).get("type") == "FeatureCollection"]),
		"layer_counts": _layer_counts(geo if isinstance(geo, dict) else {}),
		"graph_counts": STATE.get("infrastructure_graph_counts") or {},
		"graph_layer_counts": (
			dict((G.graph or {}).get("layer_counts") or {})
			if G is not None and getattr(G, "graph", None) is not None
			else {}
		),
		"route_diagnostics": STATE.get("last_infra_route_diagnostics") or {},
		"voronoi_edges_by_echelon_counts": (
			{str(k): _fc_count(v) for k, v in (geo.get("voronoi_edges_by_echelon") or {}).items()}
			if isinstance(geo, dict) and isinstance(geo.get("voronoi_edges_by_echelon"), dict)
			else {}
		),
		"layer_examples": {
			"charge_a": _examples((geo or {}).get("charging_type_a")),
			"charge_b": _examples((geo or {}).get("charging_type_b")),
			"maintenance": _examples((geo or {}).get("to_stations")),
			"garages": _examples((geo or {}).get("garages")),
			"trunk": _examples((geo or {}).get("trunk")),
			"branch": _examples((geo or {}).get("branch_edges")),
			"local": _examples((geo or {}).get("local_edges")),
			"voronoi": _examples((geo or {}).get("voronoi_edges")),
			"cluster_hulls": _examples((geo or {}).get("cluster_hulls")),
		},
		"infrastructure_graph": {
			"nodes": int(G.number_of_nodes()) if G is not None else 0,
			"edges": int(G.number_of_edges()) if G is not None else 0,
			"ok_graph": bool(G is not None and int(G.number_of_nodes()) > 0 and int(G.number_of_edges()) > 0),
			"graph_invalid": bool(STATE.get("infrastructure_graph_invalid")),
		},
		"drones_by_type": by_type,
		"last_route_debug": STATE.get("last_route_debug"),
	}


@app.get("/api/drones")
async def list_drones():
	"""Debug-friendly: full in-memory drones (includes route_segments)."""
	return {"ok": True, "drones": STATE.get("drones", {})}


@app.post("/api/clear_cache")
async def clear_cache(body: Dict[str, Any] | None = None):
	"""
	Clear runtime infrastructure caches and bump placement cache buster.
	Does not delete code or disk files; only affects in-memory/Redis placement keys.
	"""
	STATE["placement_cache_buster"] = int(STATE.get("placement_cache_buster") or 0) + 1
	_clear_infrastructure_runtime()
	# Also clear drone placement so next load_city can respawn from infra when ready
	STATE["drones"] = {}
	STATE["orders"] = []
	refresh_charger_nodes()
	await persist_state()
	return {"ok": True, "placement_cache_buster": int(STATE.get("placement_cache_buster") or 0)}


@app.post("/api/rebuild_infrastructure")
async def api_rebuild_infrastructure(body: Dict[str, Any] | None = None):
	city = (body or {}).get("city") if isinstance(body, dict) else None
	city = (city or STATE.get("city") or "").strip()
	force = True if not isinstance(body, dict) else bool((body or {}).get("force", True))
	if not city:
		return JSONResponse(status_code=400, content={"ok": False, "error": "city is required (or load_city first)"})
	try:
		logger.info("rebuild requested city=%s force=%s", city, force)
		if force:
			STATE["placement_cache_buster"] = int(STATE.get("placement_cache_buster") or 0) + 1
		started = _ensure_infrastructure_build(city, force=force)
		refresh_charger_nodes()
		await persist_state()
		return {"ok": True, "city": city, "started": started, "version": int(STATE.get("infrastructure_version") or 0)}
	except Exception as e:
		logger.exception("rebuild_infrastructure failed")
		return JSONResponse(status_code=500, content={"ok": False, "error": str(e)})

@app.post("/api/reverse")
async def reverse_geocode(body: Dict[str, float]):
	lat = body.get("lat") ; lon = body.get("lon")
	if lat is None or lon is None:
		return JSONResponse(status_code=400, content={"ok": False, "error": "lat/lon required"})
	addr = _data_service.coords_to_address((float(lat), float(lon)), language='ru')
	return {"ok": True, "address": addr}

@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
	await ws.accept()
	STATE["clients"].add(ws)
	try:
		while True:
			# keep alive / receive optional messages later
			_ = await ws.receive_text()
	except WebSocketDisconnect:
		pass
	finally:
		STATE["clients"].discard(ws)

@app.post("/api/no_fly_zones")
async def add_no_fly_zone(zone: NoFlyZone):
	# store rectangle; graph reweighting can be applied lazily
	z = zone.model_dump()
	if not z.get("id"):
		z["id"] = f"zone_{len(STATE['no_fly_zones'])+1}"
	STATE["no_fly_zones"].append(z)
	STATE["zone_version"] += 1
	# Rebuild graph with new zones if city loaded
	await rebuild_graph_with_zones()
	await persist_state()
	return {"ok": True, "zone": z}

@app.get("/api/no_fly_zones")
async def list_no_fly_zones():
	return STATE["no_fly_zones"]

@app.delete("/api/no_fly_zones/{zone_id}")
async def remove_no_fly_zone(zone_id: str):
	before = len(STATE["no_fly_zones"])
	STATE["no_fly_zones"] = [z for z in STATE["no_fly_zones"] if z.get("id") != zone_id]
	removed = before - len(STATE["no_fly_zones"])
	if removed:
		STATE["zone_version"] += 1
		await rebuild_graph_with_zones()
		await persist_state()
	return {"ok": True, "removed": removed }

def _centroid(points: List[Tuple[float, float]]) -> Tuple[float, float]:
	if not points:
		return (0.0, 0.0)
	n = len(points)
	return (sum(p[0] for p in points) / n, sum(p[1] for p in points) / n)


@app.post("/api/orders")
async def add_order(order: AddOrderRequest):
	# Classify order type
	order_type = classify_order(order)
	req_drone_type = (order.drone_type or "").strip().lower() or map_order_to_drone_type(order_type)
	if req_drone_type not in ("cargo", "operator", "cleaner"):
		req_drone_type = map_order_to_drone_type(order_type)
	start = await resolve_point(order.coords_from, order.address_from)
	end = await resolve_point(order.coords_to, order.address_to)
	start = normalize_latlon(start, ctx="add_order.start") if start else start
	end = normalize_latlon(end, ctx="add_order.end") if end else end
	# Guard: if route would be zero but points differ, keep order queued (validation later in planner too)
	# Операторский/сервисный: точка назначения может быть без «откуда» — старт с базы
	if not start and req_drone_type in ("operator", "cleaner"):
		base = STATE.get("base")
		if isinstance(base, (list, tuple)) and len(base) == 2:
			start = tuple(base)
	if not end and getattr(order, "area_polygon", None):
		pts = [tuple(p) for p in order.area_polygon if isinstance(p, (list, tuple)) and len(p) == 2]
		if pts:
			end = _centroid(pts)
	if not start and end and req_drone_type in ("operator", "cleaner"):
		start = end
	if not start or not end:
		return JSONResponse(status_code=400, content={"ok": False, "error": "Invalid start or end"})
	order_id = f"ord_{len(STATE['orders'])+1}"
	priority = int(getattr(order, "priority", 5))
	priority = max(0, min(10, priority))
	entry = {
		"id": order_id,
		"type": order_type,
		"drone_type": req_drone_type,
		"priority": priority,
		"start": start,
		"end": end,
		"battery_level": max(1.0, min(100.0, float(order.battery_level))),
		"status": "queued",
		"waypoints": [tuple(wp) for wp in (order.waypoints or []) if isinstance(wp, (list, tuple)) and len(wp)==2],
	}
	if getattr(order, "area_polygon", None):
		entry["area_polygon"] = [tuple(p) for p in order.area_polygon if isinstance(p, (list, tuple)) and len(p)==2]
	STATE["orders"].append(entry)
	await persist_state()
	return {"ok": True, "order": entry, "queue_size": len(STATE["orders"]) }

@app.get("/api/orders")
async def list_orders():
	return STATE["orders"]

@app.post("/api/orders/{order_id}/cancel")
async def cancel_order(order_id: str):
    for o in STATE["orders"]:
        if o.get("id") == order_id and o.get("status") in ("queued", "assigned"):
            o["status"] = "cancelled"
            # if assigned, release drone
            did = o.get("drone_id")
            if did and did in STATE["drones"]:
                d = STATE["drones"][did]
                # Fully release drone: clear all route/order-related fields and leave charging queues.
                try:
                    _remove_drone_from_charge_queues(did)
                except Exception:
                    pass
                d["route"] = []
                d["route_segments"] = []
                d["target_idx"] = 0
                d["resume_route"] = []
                d["resume_route_segments"] = []
                d.pop("active_order_id", None)
                d.pop("saved_order_id", None)
                d.pop("saved_target_idx", None)
                d.pop("loaded_after_waypoint_count", None)
                d.pop("waypoints_completed", None)
                d.pop("force_charge_after_route", None)
                d["status"] = "idle"
            o.pop("drone_id", None)
            await persist_state()
            return {"ok": True, "order": o}
    return JSONResponse(status_code=404, content={"ok": False, "error": "Order not found or not cancellable"})

class UpdateOrderRequest(BaseModel):
    new_address_to: Optional[str] = None
    new_coords_to: Optional[List[float]] = None

@app.post("/api/orders/{order_id}/update_destination")
async def update_order_destination(order_id: str, body: UpdateOrderRequest):
    # update destination and trigger reroute if assigned
    for o in STATE["orders"]:
        if o.get("id") == order_id:
            new_end = await resolve_point(body.new_coords_to, body.new_address_to)
            if not new_end:
                return JSONResponse(status_code=400, content={"ok": False, "error": "Invalid destination"})
            new_end = normalize_latlon(new_end, ctx="update_destination.end") or new_end
            o["end"] = new_end
            # if assigned, replan
            did = o.get("drone_id")
            if did and did in STATE["drones"]:
                d = STATE["drones"][did]
                required_type = o.get("drone_type") or map_order_to_drone_type(o.get("type", "delivery"))
                # Plan from current drone position with current battery; do not mutate STATE on failure.
                try:
                    pos = tuple(d.get("pos", o.get("start", (0, 0))))
                except Exception:
                    pos = o.get("start")
                battery = float(d.get("battery", 100.0))
                tmp_order = dict(o)
                tmp_order["end"] = new_end
                res = _run_plan_for_order(pos, battery, tmp_order, required_type)
                if not res or not res.get("ok") or not (res.get("coords") or []):
                    # keep order assigned but mark error for UI/debug
                    o["last_plan_error"] = (res or {}).get("reason") or "REPLAN_FAILED"
                else:
                    full_coords = res.get("coords") or []
                    # Clear any previous resume fragments before applying new plan
                    d["resume_route"] = []
                    d["resume_route_segments"] = []
                    apply_midroute_charging(d, full_coords, full_segments=res.get("route_segments"))
                    d["target_idx"] = 0
                    d["active_order_id"] = o.get("id")
                    # Ensure state-machine consistency
                    if d.get("status") in ("idle", "holding"):
                        d["status"] = "enroute"
                    # Sync order metadata for UI
                    o["status"] = "assigned"
                    o["route_length"] = float(res.get("route_length") or 0.0)
                    o["pickup_waypoint_count"] = int(res.get("pickup_waypoint_count") or 0)
                    o["chargers_used"] = res.get("chargers_used") or []
                    o["battery_plan"] = res.get("battery_plan") or []
                    o["segments"] = res.get("segments") or []
            await persist_state()
            return {"ok": True, "order": o}
    return JSONResponse(status_code=404, content={"ok": False, "error": "Order not found"})

@app.get("/")
async def root_page():
	# Prefer `static/index.html`, fallback to `./index.html`.
	base_dir = Path(__file__).resolve().parent
	candidates = [
		base_dir / "static" / "index.html",
		base_dir / "index.html",
	]
	for p in candidates:
		try:
			if p.is_file():
				return HTMLResponse(p.read_text(encoding="utf-8"))
		except Exception:
			logger.exception("Failed to read client html: %s", p)
	# Do not return 404: server is running, but frontend is missing.
	return HTMLResponse(
		"<h3>Client not found.</h3><p>Expected <code>static/index.html</code> (preferred) or <code>index.html</code> рядом с сервером.</p>",
		status_code=200,
	)

@app.post("/api/weather")
async def set_weather(w: Dict[str, Any]):
	wind = float(w.get("wind_mps", 3.0))
	STATE["weather"] = {
		"wind_mps": max(0.0, min(40.0, wind)),
		"humidity": STATE["weather"].get("humidity"),
		"description": STATE["weather"].get("description"),
		"weather_code": STATE["weather"].get("weather_code"),
	}
	await persist_state()
	return {"ok": True, "weather": STATE["weather"]}


@app.get("/api/weather/fetch")
async def fetch_weather(lat: Optional[float] = None, lon: Optional[float] = None):
	"""Загрузить погоду из Open-Meteo по координатам (или по базе). При недоступности API — 200, текущее состояние."""
	if lat is None or lon is None:
		base = STATE.get("base")
		if base and len(base) == 2:
			lat, lon = float(base[0]), float(base[1])
		else:
			# Default fallback: Volgograd
			lat, lon = 48.7080, 44.5133
	data = await fetch_weather_from_api(lat, lon)
	if data:
		STATE["weather"] = data
		await persist_state()
		return {"ok": True, "fetched": True, "weather": STATE["weather"]}
	# API недоступен — не ломаем UI: 200, текущая погода, fetched: False
	return {
		"ok": True,
		"fetched": False,
		"weather": STATE.get("weather", {"wind_mps": 3.0}),
		"error": "Weather API unavailable",
	}


@app.get("/api/battery_mode")
async def get_battery_mode():
	"""Текущий режим расхода: reality — реальные данные, test — укороченная дальность для тестов."""
	return {"mode": STATE.get("battery_mode", "reality")}


@app.post("/api/battery_mode")
async def set_battery_mode(body: Dict[str, str]):
	"""Переключить режим: {"mode": "reality"} или {"mode": "test"}."""
	mode = (body.get("mode") or "").strip().lower()
	if mode not in ("reality", "test"):
		return JSONResponse(status_code=400, content={"ok": False, "error": "mode must be 'reality' or 'test'"})
	STATE["battery_mode"] = mode
	_routing_service.set_battery_mode(mode)
	# При переключении в «Тест» сбрасываем назначенные заказы, чтобы маршруты пересчитались через зарядки
	if mode == "test":
		for o in STATE.get("orders", []):
			if o.get("status") == "assigned":
				did = o.get("drone_id")
				if did and did in STATE.get("drones", {}):
					d = STATE["drones"][did]
					d["route"] = []
					d["target_idx"] = 0
					d["status"] = "idle"
				o["status"] = "queued"
				o.pop("drone_id", None)
		logger.info("battery_mode=test: assigned orders reset to queued for replan with chargers")
	return {"ok": True, "mode": mode}


# Base and inventory management
class BaseConfig(BaseModel):
    base: Optional[Tuple[float, float]] = None
    inventory: Optional[Dict[str, int]] = None  # type->count

@app.post("/api/base")
async def set_base(cfg: BaseConfig):
    if cfg.base:
        STATE["base"] = tuple(cfg.base)
        # Update weather by base coordinates (best effort).
        try:
            lat, lon = float(STATE["base"][0]), float(STATE["base"][1])
            data = await fetch_weather_from_api(lat, lon)
            if data:
                STATE["weather"] = data
        except Exception:
            logger.exception("fetch weather on set_base failed")
    if cfg.inventory is not None:
        # sanitize counts
        inv: Dict[str, int] = {}
        for k, v in (cfg.inventory or {}).items():
            try:
                inv[str(k)] = max(0, int(v))
            except Exception:
                continue
        STATE["inventory"] = inv
    refresh_charger_nodes()
    ensure_base_drones()
    await persist_state()
    return {"ok": True, "base": STATE["base"], "inventory": STATE["inventory"]}

@app.get("/api/base")
async def get_base():
    return {"base": STATE["base"], "inventory": STATE["inventory"], "stations": STATE["stations"]}

class StationsConfig(BaseModel):
    stations: List[Tuple[float, float]]
    capacity: Optional[int] = 2

@app.post("/api/stations")
async def set_stations(cfg: StationsConfig):
    try:
        stations = []
        for s in cfg.stations:
            if isinstance(s, (list, tuple)) and len(s) == 2:
                stations.append((float(s[0]), float(s[1])))
        STATE["stations"] = stations
        # Станции: смена аккумулятора, запас 20 заряженных; очередь на зарядку
        STATE["station_queues"] = {
            str(i): {
                "charged_batteries": STATION_CHARGED_BATTERIES_MAX,
                "charging_queue": [],  # список тиков до готовности каждого аккумулятора
                "queue": [],  # drone_id, ожидающие заряженный аккумулятор
            }
            for i in range(len(stations))
        }
        refresh_charger_nodes()
        await persist_state()
        return {"ok": True, "stations": stations}
    except Exception as e:
        return JSONResponse(status_code=400, content={"ok": False, "error": str(e)})

def refresh_charger_nodes() -> None:
	"""Bind base and stations to nearest graph nodes. Call after load_city, set_base, set_stations."""
	# Chargers are used by door-to-door + plan_with_chargers logic, which must be stable on city_graph.
	G = STATE.get("city_graph")
	if G is None:
		STATE["charger_nodes"] = {"base": None, "stations": []}
		return
	cn = {"base": None, "stations": []}
	base = STATE.get("base")
	if base and isinstance(base, (list, tuple)) and len(base) == 2:
		cn["base"] = _routing_service._find_nearest_node(G, tuple(base))
	stations = STATE.get("stations") or []
	cn["stations"] = []
	for s in stations:
		if isinstance(s, (list, tuple)) and len(s) == 2:
			node = _routing_service._find_nearest_node(G, tuple(s))
			cn["stations"].append(node)
		else:
			cn["stations"].append(None)
	STATE["charger_nodes"] = cn
	logger.debug("charger_nodes refreshed (city_graph): base=%s stations=%s", cn["base"], cn["stations"])


def get_active_routing_graph():
	"""Routing graph used for default door-to-door planning (always city_graph)."""
	return STATE.get("city_graph")


def is_infrastructure_routing_ready() -> bool:
	"""True only when infra layers and graph are sufficient for routing (otherwise use city_graph)."""
	try:
		geo = STATE.get("infrastructure_geojson") or {}
		lc = _layer_counts(geo if isinstance(geo, dict) else {})
		G = STATE.get("infrastructure_graph")
		local_or_voronoi = (
			int(lc.get("local") or 0) > 0
			or int(lc.get("voronoi") or 0) > 0
			or int(lc.get("voronoi_by_echelon") or 0) > 0
		)
		ok_layers = (
			int(lc.get("charge_a") or 0) > 0
			and int(lc.get("charge_b") or 0) > 0
			and int(lc.get("branch") or 0) > 0
			and local_or_voronoi
		)
		ok_graph = G is not None and int(G.number_of_nodes()) > 0 and int(G.number_of_edges()) > 0
		return bool(ok_layers and ok_graph and not STATE.get("infrastructure_graph_invalid"))
	except Exception:
		return False


def limited_charger_nodes(point: Tuple[float, float]) -> Dict[str, Any]:
	"""
	Return charger_nodes dict but only with nearest station nodes (performance guard).
	Stations are matched by index to STATE["stations"] and STATE["charger_nodes"]["stations"].
	"""
	out = {"base": None, "stations": []}
	cn = STATE.get("charger_nodes") or {"base": None, "stations": []}
	out["base"] = cn.get("base")
	stations = STATE.get("stations") or []
	nodes = cn.get("stations") or []
	cands = []
	for i, s in enumerate(stations):
		if i >= len(nodes):
			break
		n = nodes[i]
		if not n:
			continue
		try:
			pos = tuple(s)
			d = haversine_m(tuple(point), pos)
			cands.append((d, n))
		except Exception:
			continue
	cands.sort(key=lambda x: x[0])
	out["stations"] = [n for _d, n in cands[:MAX_NEAREST_STATIONS]]
	return out


def _rect_from_zone(z: Dict[str, Any]) -> Optional[Tuple[float, float, float, float]]:
	try:
		return (float(z["lat_min"]), float(z["lat_max"]), float(z["lon_min"]), float(z["lon_max"]))
	except Exception:
		return None


def _segment_intersects_any_zone(a: Tuple[float, float], b: Tuple[float, float]) -> bool:
	"""Fast check for rectangular zones (MVP zones in this server)."""
	lat1, lon1 = float(a[0]), float(a[1])
	lat2, lon2 = float(b[0]), float(b[1])
	min_lat, max_lat = (lat1, lat2) if lat1 <= lat2 else (lat2, lat1)
	min_lon, max_lon = (lon1, lon2) if lon1 <= lon2 else (lon2, lon1)
	for z in (STATE.get("no_fly_zones") or []):
		r = _rect_from_zone(z) if isinstance(z, dict) else None
		if not r:
			continue
		zlat_min, zlat_max, zlon_min, zlon_max = r
		# bbox overlap (coarse) — enough for MVP rectangles
		if max_lat < zlat_min or min_lat > zlat_max or max_lon < zlon_min or min_lon > zlon_max:
			continue
		return True
	return False


def _geojson_features_to_lines(geo: Dict[str, Any]) -> List[Dict[str, Any]]:
	fc = geo if isinstance(geo, dict) else {}
	if fc.get("type") == "FeatureCollection":
		return list(fc.get("features") or [])
	return []


def _round_key(lon: float, lat: float, p: int = 6) -> Tuple[float, float]:
	return (round(float(lon), p), round(float(lat), p))


def _infra_layer_rank(layer: str) -> int:
	l = (layer or "").lower()
	if l == "trunk":
		return 4
	if l == "branch":
		return 3
	if l in ("local", "voronoi"):
		return 2
	if l == "off_network":
		return 1
	return 0


def build_infrastructure_graph(infra_geojson: Dict[str, Any]) -> nx.Graph:
	"""
	Build a lightweight routing graph from infrastructure geojson layers:
	trunk, branch_edges, local_edges, voronoi_edges / voronoi_edges_by_echelon.
	Each LineString contributes one edge per consecutive vertex pair.
	On duplicate undirected edges, keep the higher-priority layer (trunk > branch > local/voronoi > off_network).
	"""
	G = nx.Graph()

	def add_lines(features: List[Dict[str, Any]], *, layer: str, echelon_level: int):
		for f in features:
			geom = (f or {}).get("geometry") or {}
			gt = (geom.get("type") or "").lower()
			if gt != "linestring":
				continue
			coords = geom.get("coordinates") or []
			if not isinstance(coords, list) or len(coords) < 2:
				continue
			for i in range(len(coords) - 1):
				try:
					lon1, lat1 = coords[i]
					lon2, lat2 = coords[i + 1]
					k1 = _round_key(lon1, lat1)
					k2 = _round_key(lon2, lat2)
					if k1 == k2:
						continue
					if k1 not in G:
						G.add_node(k1, pos=(float(lat1), float(lon1)))
					if k2 not in G:
						G.add_node(k2, pos=(float(lat2), float(lon2)))
					a = (float(lat1), float(lon1))
					b = (float(lat2), float(lon2))
					length_m = float(haversine_m(a, b))
					if length_m <= 0:
						continue
					edata = G.get_edge_data(k1, k2) or {}
					old_layer = (edata.get("layer") or "").lower() or "off_network"
					new_rank = _infra_layer_rank(layer)
					old_rank = _infra_layer_rank(old_layer)
					if edata:
						if new_rank < old_rank:
							continue
						if new_rank == old_rank and float(length_m) >= float(edata.get("length_m", 1e18)):
							continue
					G.add_edge(
						k1,
						k2,
						length_m=length_m,
						layer=str(layer),
						flight_level=int(echelon_level),
					)
				except Exception:
					continue

	add_lines(_geojson_features_to_lines(infra_geojson.get("trunk") or {}), layer="trunk", echelon_level=4)
	add_lines(_geojson_features_to_lines(infra_geojson.get("branch_edges") or {}), layer="branch", echelon_level=4)
	add_lines(_geojson_features_to_lines(infra_geojson.get("local_edges") or {}), layer="local", echelon_level=4)

	vbe = infra_geojson.get("voronoi_edges_by_echelon") or {}
	if isinstance(vbe, dict) and vbe:
		for sk, sub in vbe.items():
			try:
				level = int(sk)
			except Exception:
				level = 2
			add_lines(_geojson_features_to_lines(sub), layer="voronoi", echelon_level=level)
	else:
		add_lines(_geojson_features_to_lines(infra_geojson.get("voronoi_edges") or {}), layer="voronoi", echelon_level=2)

	layer_counts: Dict[str, int] = {}
	for _u, _v, attr in G.edges(data=True):
		lay = str((attr or {}).get("layer") or "unknown")
		layer_counts[lay] = layer_counts.get(lay, 0) + 1
	G.graph["layer_counts"] = layer_counts

	return G


def _infra_graph_build_kdtree(G: nx.Graph) -> None:
	"""Caches KDTree for infrastructure graph nodes to speed up nearest-node queries and candidate generation."""
	try:
		coords = []
		nodes = []
		for n, a in G.nodes(data=True):
			pos = (a or {}).get("pos")
			if not pos or len(pos) != 2:
				continue
			coords.append([float(pos[0]), float(pos[1])])  # [lat, lon]
			nodes.append(n)
		if not coords:
			return
		G.graph["_infra_node_kdtree"] = {
			"kdtree": cKDTree(np.asarray(coords, dtype=float)),
			"nodes": nodes,
			"coords": coords,
		}
	except Exception:
		return


def _infra_graph_nearest_nodes(G: nx.Graph, point: Tuple[float, float], k: int) -> List[Tuple[Any, float]]:
	"""Return up to k nearest infrastructure nodes as (node_id, approx_dist_m)."""
	if G is None or G.number_of_nodes() == 0:
		logger.debug("_infra_graph_nearest_nodes: empty graph k=%s", k)
		return []
	try:
		k_req = int(k)
	except Exception:
		k_req = 1
	if k_req <= 0:
		k_req = 1

	# Ensure KDTree cache exists
	cache = (G.graph or {}).get("_infra_node_kdtree") if getattr(G, "graph", None) is not None else None
	if not (isinstance(cache, dict) and cache.get("kdtree") is not None):
		_infra_graph_build_kdtree(G)
		cache = (G.graph or {}).get("_infra_node_kdtree") if getattr(G, "graph", None) is not None else None

	nodes_count = int(G.number_of_nodes())
	try:
		pt = [float(point[0]), float(point[1])]  # [lat, lon]
	except Exception:
		return []

	# Primary: KDTree query
	if isinstance(cache, dict) and cache.get("kdtree") is not None:
		try:
			nodes = cache.get("nodes") or []
			k_eff = max(1, min(k_req, len(nodes)))
			if k_eff <= 0:
				logger.debug("_infra_graph_nearest_nodes: nodes=%s k=%s -> 0 candidates", nodes_count, k_req)
				return []

			dists, idxs = cache["kdtree"].query(pt, k=k_eff)
			if k_eff == 1:
				dists = [float(dists)]
				idxs = [int(idxs)]

			out: List[Tuple[Any, float]] = []
			for d, idx in zip(dists, idxs):
				i = int(idx)
				if i < 0 or i >= len(nodes):
					continue
				approx_m = float(d) * 111_000.0
				out.append((nodes[i], approx_m))

			logger.debug(
				"_infra_graph_nearest_nodes: nodes=%s k=%s -> %s candidates",
				nodes_count,
				k_eff,
				len(out),
			)
			return out
		except Exception:
			# Fall back to slow scan below
			pass

	# Fallback: slow scan (rough degree-distance)
	try:
		best: List[Tuple[Any, float]] = []
		lat, lon = float(point[0]), float(point[1])
		for n, a in G.nodes(data=True):
			pos = (a or {}).get("pos")
			if not pos or len(pos) != 2:
				continue
			d = float(np.hypot(lat - float(pos[0]), lon - float(pos[1])))
			best.append((n, d * 111_000.0))
		best.sort(key=lambda x: x[1])
		out = best[: max(1, min(k_req, len(best)))]
		logger.debug(
			"_infra_graph_nearest_nodes: nodes=%s k=%s -> %s candidates (slow)",
			nodes_count,
			k_req,
			len(out),
		)
		return out
	except Exception:
		return []


def _infra_cluster_index():
	"""
	Builds (and caches in STATE) a spatial index for cluster hulls/centroids/stations to assign cluster_id to points.
	Returns dict with prepared hulls, centroid KDTree, and station KDTree.
	"""
	cache = STATE.get("_infra_cluster_index")
	ver = int(STATE.get("infrastructure_version") or 0)
	if isinstance(cache, dict) and cache.get("version") == ver:
		return cache

	geo = STATE.get("infrastructure_geojson") or {}
	hulls_fc = (geo.get("cluster_hulls") or {})
	cent_fc = (geo.get("cluster_centroids") or {})
	charge_a = (geo.get("charging_type_a") or {})
	charge_b = (geo.get("charging_type_b") or {})

	# Hulls: list of (cluster_id, prepared_polygon)
	hulls = []
	try:
		for f in (hulls_fc.get("features") or []):
			p = (f or {}).get("properties") or {}
			cid = p.get("cluster_id")
			g = (f or {}).get("geometry")
			if cid is None or not g:
				continue
			poly = shapely_shape(g)
			if poly is None or getattr(poly, "is_empty", True):
				continue
			hulls.append((cid, shapely_prep(poly)))
	except Exception:
		hulls = []

	# Centroids KDTree
	cent_points = []
	cent_ids = []
	try:
		for f in (cent_fc.get("features") or []):
			p = (f or {}).get("properties") or {}
			cid = p.get("cluster_id")
			g = (f or {}).get("geometry") or {}
			if cid is None or (g.get("type") or "").lower() != "point":
				continue
			lon, lat = g.get("coordinates") or [None, None]
			if lat is None or lon is None:
				continue
			cent_points.append([float(lat), float(lon)])
			cent_ids.append(cid)
	except Exception:
		cent_points, cent_ids = [], []
	cent_tree = cKDTree(np.asarray(cent_points, dtype=float)) if cent_points else None

	# Charging station KDTree (most reliable cluster_id)
	st_points = []
	st_ids = []
	try:
		for fc in (charge_a, charge_b):
			for f in (fc.get("features") or []):
				p = (f or {}).get("properties") or {}
				cid = p.get("cluster_id")
				g = (f or {}).get("geometry") or {}
				if cid is None or (g.get("type") or "").lower() != "point":
					continue
				lon, lat = g.get("coordinates") or [None, None]
				if lat is None or lon is None:
					continue
				st_points.append([float(lat), float(lon)])
				st_ids.append(cid)
	except Exception:
		st_points, st_ids = [], []
	st_tree = cKDTree(np.asarray(st_points, dtype=float)) if st_points else None

	out = {
		"version": ver,
		"hulls": hulls,
		"cent_tree": cent_tree,
		"cent_ids": cent_ids,
		"st_tree": st_tree,
		"st_ids": st_ids,
		"point_cache": {},
	}
	STATE["_infra_cluster_index"] = out
	return out


def _infer_cluster_id_for_point(point: Tuple[float, float]) -> Any:
	"""Best-effort cluster_id for a WGS84 point (lat, lon). Returns None if unknown."""
	try:
		idx = _infra_cluster_index()
		key = (round(float(point[0]), 6), round(float(point[1]), 6))
		pc = idx.get("point_cache") or {}
		if key in pc:
			return pc[key]

		pt = ShapelyPoint(float(point[1]), float(point[0]))  # (lon, lat)
		# 1) Inside hull
		for cid, prepared in (idx.get("hulls") or []):
			try:
				if prepared.contains(pt):
					pc[key] = cid
					idx["point_cache"] = pc
					return cid
			except Exception:
				continue

		# 2) Nearest station with cluster_id (within ~3km)
		st_tree = idx.get("st_tree")
		if st_tree is not None and (idx.get("st_ids") or []):
			d, i = st_tree.query([float(point[0]), float(point[1])], k=1)
			i = int(i)
			if 0 <= i < len(idx["st_ids"]) and float(d) * 111000.0 <= 3000.0:
				pc[key] = idx["st_ids"][i]
				idx["point_cache"] = pc
				return pc[key]

		# 3) Nearest centroid (within ~6km)
		cent_tree = idx.get("cent_tree")
		if cent_tree is not None and (idx.get("cent_ids") or []):
			d, i = cent_tree.query([float(point[0]), float(point[1])], k=1)
			i = int(i)
			if 0 <= i < len(idx["cent_ids"]) and float(d) * 111000.0 <= 6000.0:
				pc[key] = idx["cent_ids"][i]
				idx["point_cache"] = pc
				return pc[key]

		pc[key] = None
		idx["point_cache"] = pc
		return None
	except Exception:
		return None


def _infer_cluster_id_for_node(G: nx.Graph, node: Any) -> Any:
	"""Cache node->cluster_id on infra graph."""
	try:
		cache = (G.graph or {}).setdefault("_node_cluster_id", {})
		if node in cache:
			return cache[node]
		pos = (G.nodes[node] or {}).get("pos")
		if not pos:
			cache[node] = None
			return None
		cid = _infer_cluster_id_for_point(tuple(pos))
		cache[node] = cid
		return cid
	except Exception:
		return None


def _sort_infra_nodes_by_distance(
	G: nx.Graph,
	nodes: Any,
	ref: Tuple[float, float],
	*,
	limit: int,
) -> List[Any]:
	"""Stable ordering: nearest graph nodes to ref (meters), capped."""
	scored: List[Tuple[float, Any]] = []
	for n in nodes:
		p = G.nodes[n].get("pos")
		if not p:
			continue
		scored.append((float(haversine_m(ref, tuple(p))), n))
	scored.sort(key=lambda x: x[0])
	return [n for _, n in scored[: max(1, int(limit))]]


def _nodes_incident_to_layers(G: nx.Graph, layers: Tuple[str, ...]) -> set:
	"""Node ids that touch at least one edge whose layer is in `layers`."""
	want = set(layers)
	out: set = set()
	for u, v, a in G.edges(data=True):
		lay = (a or {}).get("layer")
		if lay in want:
			out.add(u)
			out.add(v)
	return out


def _graph_has_trunk_edges(G: Optional[nx.Graph]) -> bool:
	if G is None:
		return False
	for _u, _v, a in G.edges(data=True):
		if (a or {}).get("layer") == "trunk":
			return True
	return False


def _infra_graph_has_air_network_edges(G: Optional[nx.Graph]) -> bool:
	"""Есть ли рёбра trunk/branch/local/voronoi (воздушная сеть, не только off)."""
	if G is None:
		return False
	for _u, _v, a in G.edges(data=True):
		lay = str((a or {}).get("layer") or "").lower()
		if lay in ("trunk", "branch", "local", "voronoi"):
			return True
	return False


def _charging_station_infra_portal_nodes(G: nx.Graph, *, max_per_station: int = 2, max_total: int = 28) -> List[Any]:
	"""Nearest infrastructure graph nodes to charging stations (A/B) — якоря для обходных магистральных маршрутов."""
	geo = STATE.get("infrastructure_geojson") or {}
	out: List[Any] = []
	seen: set = set()
	for key in ("charging_type_a", "charging_type_b"):
		fc = geo.get(key) or {}
		for f in (fc.get("features") or []):
			try:
				g = (f or {}).get("geometry") or {}
				if (g.get("type") or "").lower() != "point":
					continue
				lon, lat = g.get("coordinates") or [None, None]
				if lat is None or lon is None:
					continue
				pt = (float(lat), float(lon))
				for n, d in _infra_graph_nearest_nodes(G, pt, max(3, max_per_station)):
					if float(d) <= 900.0 and n not in seen:
						seen.add(n)
						out.append(n)
						if len(out) >= max_total:
							return out
			except Exception:
				continue
	return out


def _enrich_route_diagnostics(diag: Dict[str, Any]) -> Dict[str, Any]:
	"""Добавляет total_m, infra_ratio, доли для UI и проверок качества."""
	tm = float(diag.get("trunk_m") or 0.0)
	bm = float(diag.get("branch_m") or 0.0)
	lm = float(diag.get("local_m") or 0.0)
	vm = float(diag.get("voronoi_m") or 0.0)
	om = float(diag.get("off_network_m") or 0.0)
	infra = tm + bm + lm + vm
	total = float(infra + om)
	if total <= 1e-6:
		total = max(1e-6, float(diag.get("start_end_m") or 0.0))
	diag["total_m"] = float(total)
	diag["infra_m"] = float(infra)
	diag["infra_ratio"] = float(infra / max(1e-6, total))
	# trunk_ratio: доля магистрали среди «воздушных» рёбер (без off_network)
	diag["trunk_ratio"] = float(tm / max(1e-6, infra)) if infra > 1e-6 else 0.0
	diag["local_voronoi_m"] = float(lm + vm)
	diag["local_voronoi_ratio"] = float((lm + vm) / max(1e-6, total))
	diag["local_ratio"] = float((lm + vm) / max(1e-6, total))
	diag["off_network_ratio"] = float(om / max(1e-6, total))
	diag["trunk_pct"] = float(100.0 * tm / max(1e-6, total))
	diag["local_voronoi_pct"] = float(100.0 * (lm + vm) / max(1e-6, total))
	diag["off_network_pct"] = float(100.0 * om / max(1e-6, total))
	return diag


def _aggregate_route_diagnostics_from_legs(leg_diags: List[Dict[str, Any]]) -> Dict[str, Any]:
	"""Суммирует метры по нескольким плечам для общего блока total."""
	tm = bm = lm = vm = om = 0.0
	for d in leg_diags:
		if not d:
			continue
		tm += float(d.get("trunk_m") or 0.0)
		bm += float(d.get("branch_m") or 0.0)
		lm += float(d.get("local_m") or 0.0)
		vm += float(d.get("voronoi_m") or 0.0)
		om += float(d.get("off_network_m") or 0.0)
	base = {
		"trunk_m": float(tm),
		"branch_m": float(bm),
		"local_m": float(lm),
		"voronoi_m": float(vm),
		"off_network_m": float(om),
		"start_end_m": float(sum(float(d.get("start_end_m") or 0.0) for d in leg_diags if d)),
	}
	return _enrich_route_diagnostics(base)


def concat_coords_without_duplicate_endpoints(
	*legs: Optional[List[Tuple[float, float]]],
) -> List[Tuple[float, float]]:
	"""Склеивает polylines; убирает дубликат последней точки предыдущего leg и первой следующего."""
	out: List[Tuple[float, float]] = []
	for leg in legs:
		if not leg:
			continue
		for p in leg:
			try:
				pt = (float(p[0]), float(p[1]))
			except Exception:
				continue
			if out and (abs(out[-1][0] - pt[0]) < 1e-9 and abs(out[-1][1] - pt[1]) < 1e-9):
				continue
			out.append(pt)
	return out


def _validate_infra_route_quality(
	diag: Dict[str, Any],
	*,
	graph_has_trunk: bool,
	drone_type: str = "cargo",
) -> Tuple[bool, float, str]:
	"""
	Жёсткие отбрасывания плохих маршрутов: слишком много off_network; для cargo — требования к магистрали.
	Возвращает (ok, penalty, reason).
	"""
	d = _enrich_route_diagnostics(dict(diag))
	prof = INFRA_DRONE_PROFILES[_normalized_drone_type(drone_type)]
	total = float(d.get("total_m") or 0.0)
	off_m = float(d.get("off_network_m") or 0.0)
	tr_ratio = float(d.get("trunk_ratio") or 0.0)
	tm = float(d.get("trunk_m") or 0.0)
	start_end = float(d.get("start_end_m") or 0.0)
	off_limit = 0.75 if start_end <= LONG_ROUTE_MIN_M else max(OFF_NETWORK_MAX_FRACTION_OF_TOTAL, 0.45)
	if total > 1e-6 and off_m / total > off_limit + 1e-9:
		return False, INFRA_ROUTE_SCORE_REJECT, "off_network_frac"
	if prof.get("require_trunk_on_graph") and start_end > LONG_ROUTE_MIN_M and graph_has_trunk and tm < 1.0:
		return False, INFRA_ROUTE_SCORE_REJECT, "no_trunk_while_graph_has_trunk"
	if (
		prof.get("require_long_route_trunk_ratio")
		and start_end > LONG_ROUTE_MIN_M
		and graph_has_trunk
		and tr_ratio + 1e-9 < LONG_ROUTE_MIN_TRUNK_RATIO
	):
		return False, INFRA_ROUTE_SCORE_REJECT, "long_route_low_trunk_ratio"
	return True, 0.0, "ok"


def plan_infrastructure_strict_layer_route(
	start_coord: Tuple[float, float],
	end_coord: Tuple[float, float],
	drone_type: str,
	mode: str = "empty",
) -> Tuple[Optional[List[Any]], Optional[List[Tuple[float, float]]], float, List[Dict[str, Any]]]:
	"""
	Strict schema: local/voronoi -> branch -> trunk -> branch -> local, plus off_network legs to endpoints.
	If cluster_id cannot be inferred, still tries nearest local/voronoi/branch portals (relaxed mode).
	"""
	G = STATE.get("infrastructure_graph")
	if G is None or G.number_of_nodes() == 0 or G.number_of_edges() == 0:
		logger.info("plan_infrastructure_strict_layer_route: refuse reason=no_graph")
		return None, None, 0.0, []

	start = (float(start_coord[0]), float(start_coord[1]))
	end = (float(end_coord[0]), float(end_coord[1]))

	c_start = _infer_cluster_id_for_point(start)
	c_end = _infer_cluster_id_for_point(end)
	relaxed_cluster = c_start is None or c_end is None
	if relaxed_cluster:
		logger.info(
			"plan_infrastructure_strict_layer_route: relaxed cluster inference c_start=%s c_end=%s",
			c_start,
			c_end,
		)

	# nearest nodes (limit) — резерв, если нет узлов local/voronoi рядом
	start_nodes = [n for n, _d in _infra_graph_nearest_nodes(G, start, MAX_NEAREST_NETWORK_NODES)]
	end_nodes = [n for n, _d in _infra_graph_nearest_nodes(G, end, MAX_NEAREST_NETWORK_NODES)]
	if not start_nodes or not end_nodes:
		logger.info("plan_infrastructure_strict_layer_route: refuse reason=no_start_nodes=%s no_end_nodes=%s", not start_nodes, not end_nodes)
		return None, None, 0.0, []

	if not relaxed_cluster:
		start_nodes = [n for n in start_nodes if _infer_cluster_id_for_node(G, n) == c_start] or start_nodes
		end_nodes = [n for n in end_nodes if _infer_cluster_id_for_node(G, n) == c_end] or end_nodes

	# Build portal sets
	branch_nodes = set()
	trunk_nodes = set()
	for u, v, a in G.edges(data=True):
		lay = (a or {}).get("layer")
		if lay == "branch":
			branch_nodes.add(u)
			branch_nodes.add(v)
		elif lay == "trunk":
			trunk_nodes.add(u)
			trunk_nodes.add(v)

	if not trunk_nodes:
		logger.info("plan_infrastructure_strict_layer_route: refuse reason=no_trunk_nodes")
		return None, None, 0.0, []

	# Cluster-filtered portal candidates; relaxed: nearest branch nodes to start/end
	if not relaxed_cluster:
		start_branch = [n for n in branch_nodes if _infer_cluster_id_for_node(G, n) == c_start]
		end_branch = [n for n in branch_nodes if _infer_cluster_id_for_node(G, n) == c_end]
	else:
		start_branch, end_branch = [], []
	if not start_branch:
		start_branch = _sort_infra_nodes_by_distance(G, branch_nodes, start, limit=12)
	if not end_branch:
		end_branch = _sort_infra_nodes_by_distance(G, branch_nodes, end, limit=12)
	if not start_branch or not end_branch:
		logger.info(
			"plan_infrastructure_strict_layer_route: refuse reason=no_branch_portals start=%s end=%s",
			len(start_branch or []),
			len(end_branch or []),
		)
		return None, None, 0.0, []

	# Subgraphs by layer
	def edge_ok_local(a): return (a.get("layer") in ("local", "voronoi"))
	def edge_ok_branch(a): return (a.get("layer") == "branch")
	def edge_ok_trunk(a): return (a.get("layer") == "trunk")

	def sp(src, dst, ok_fn):
		return nx.shortest_path(
			G,
			src,
			dst,
			weight=lambda u, v, a: _infra_edge_weight_m(a or {}, drone_type) if ok_fn(a or {}) else 1e12,
		)

	best = None
	best_len = float("inf")

	lv_nodes = _nodes_incident_to_layers(G, ("local", "voronoi"))
	start_entry_cands: List[Any] = []
	for n in _sort_infra_nodes_by_distance(G, lv_nodes, start, limit=20):
		pp = G.nodes[n].get("pos")
		if pp and haversine_m(start, tuple(pp)) <= OFF_NETWORK_MAX_ENTRY_EXIT_M:
			start_entry_cands.append(n)
	if len(start_entry_cands) < 2:
		for n in _sort_infra_nodes_by_distance(G, branch_nodes, start, limit=16):
			pp = G.nodes[n].get("pos")
			if not pp:
				continue
			if haversine_m(start, tuple(pp)) <= OFF_NETWORK_MAX_ENTRY_EXIT_M and n not in start_entry_cands:
				start_entry_cands.append(n)
	if not start_entry_cands:
		for n in _sort_infra_nodes_by_distance(G, lv_nodes, start, limit=10):
			pp = G.nodes[n].get("pos")
			if pp and haversine_m(start, tuple(pp)) <= 650.0:
				start_entry_cands.append(n)
	if not start_entry_cands:
		start_entry_cands = start_nodes[:4]

	end_exit_cands = []
	for n in _sort_infra_nodes_by_distance(G, lv_nodes, end, limit=20):
		pp = G.nodes[n].get("pos")
		if pp and haversine_m(end, tuple(pp)) <= OFF_NETWORK_MAX_ENTRY_EXIT_M:
			end_exit_cands.append(n)
	if len(end_exit_cands) < 2:
		for n in _sort_infra_nodes_by_distance(G, branch_nodes, end, limit=16):
			pp = G.nodes[n].get("pos")
			if not pp:
				continue
			if haversine_m(end, tuple(pp)) <= OFF_NETWORK_MAX_ENTRY_EXIT_M and n not in end_exit_cands:
				end_exit_cands.append(n)
	if not end_exit_cands:
		for n in _sort_infra_nodes_by_distance(G, lv_nodes, end, limit=10):
			pp = G.nodes[n].get("pos")
			if pp and haversine_m(end, tuple(pp)) <= 650.0:
				end_exit_cands.append(n)
	if not end_exit_cands:
		end_exit_cands = end_nodes[:4]

	mid_ref = ((start[0] + end[0]) * 0.5, (start[1] + end[1]) * 0.5)
	trunk_cands_full = _sort_infra_nodes_by_distance(
		G, trunk_nodes, mid_ref, limit=min(48, len(trunk_nodes))
	)
	ref2 = start
	if start_branch:
		bp = G.nodes[start_branch[0]].get("pos")
		if bp:
			ref2 = tuple(bp)
	trunk_cands_full = list(
		dict.fromkeys(
			trunk_cands_full + _sort_infra_nodes_by_distance(G, trunk_nodes, ref2, limit=min(24, len(trunk_nodes)))
		)
	)

	start_branch_cands = start_branch[:6]
	end_branch_cands = end_branch[:6]
	trunk_cands = trunk_cands_full[:12]
	start_entry_cands = start_entry_cands[:8]
	end_exit_cands = end_exit_cands[:8]

	for entry in start_entry_cands:
		entry_pos = G.nodes[entry].get("pos")
		if not entry_pos or haversine_m(start, tuple(entry_pos)) > OFF_NETWORK_MAX_ENTRY_EXIT_M:
			continue
		for exitn in end_exit_cands:
			exit_pos = G.nodes[exitn].get("pos")
			if not exit_pos or haversine_m(tuple(exit_pos), end) > OFF_NETWORK_MAX_ENTRY_EXIT_M:
				continue

			for sb in start_branch_cands:
				for eb in end_branch_cands:
					# pick trunk portals closest to branches (cheap heuristic)
					for t1 in trunk_cands[:6]:
						for t2 in trunk_cands[:6]:
							try:
								p_local_a = sp(entry, sb, edge_ok_local)
								p_branch_a = sp(sb, t1, edge_ok_branch)
								p_trunk = sp(t1, t2, edge_ok_trunk)
								p_branch_b = sp(t2, eb, edge_ok_branch)
								p_local_b = sp(eb, exitn, edge_ok_local)

								path_nodes = []
								for seg in (p_local_a, p_branch_a[1:], p_trunk[1:], p_branch_b[1:], p_local_b[1:]):
									path_nodes.extend(seg)

								coords = [start]
								coords.append(tuple(entry_pos))
								for n in path_nodes:
									pp = G.nodes[n].get("pos")
									if not pp:
										continue
									ppt = tuple(pp)
									if coords and coords[-1] == ppt:
										continue
									coords.append(ppt)
								coords.append(tuple(exit_pos))
								coords.append(end)
								total_len = _route_length(coords)
								if total_len < best_len:
									best_len = total_len
									best = (path_nodes, coords)
							except Exception:
								continue

	if not best:
		logger.info("plan_infrastructure_strict_layer_route: refuse reason=no_strict_path")
		return None, None, 0.0, []

	path_nodes, coords = best
	segs = _route_segments_from_coords(coords, mode=mode, drone_type=drone_type)
	return path_nodes, coords, float(best_len), segs


def _infra_subpath_segments(
	G: nx.Graph,
	path_nodes: List[Any],
	*,
	mode: str,
) -> List[Dict[str, Any]]:
	"""One UI segment per edge along an infrastructure node path."""
	out: List[Dict[str, Any]] = []
	if not path_nodes or len(path_nodes) < 2:
		return out
	for i in range(1, len(path_nodes)):
		u, v = path_nodes[i - 1], path_nodes[i]
		pu = G.nodes[u].get("pos")
		pv = G.nodes[v].get("pos")
		if not pu or not pv:
			continue
		a = tuple(pu)
		b = tuple(pv)
		ed = G.get_edge_data(u, v) or {}
		length_m = float(haversine_m(a, b))
		out.append(
			{
				"from": [float(a[0]), float(a[1])],
				"to": [float(b[0]), float(b[1])],
				"layer": str(ed.get("layer") or "off_network"),
				"echelon": int(ed.get("flight_level") or 2),
				"length_m": length_m,
				"mode": str(mode),
			}
		)
	return out


def _route_segment_single(
	a: Tuple[float, float],
	b: Tuple[float, float],
	*,
	mode: str,
	layer: str = "off_network",
	echelon: int = 2,
) -> Dict[str, Any]:
	length_m = float(haversine_m(a, b))
	return {
		"from": [float(a[0]), float(a[1])],
		"to": [float(b[0]), float(b[1])],
		"layer": str(layer),
		"echelon": int(echelon),
		"length_m": length_m,
		"mode": str(mode),
	}


def _infra_leg_segments_or_fallback(
	G: nx.Graph,
	a: Tuple[float, float],
	b: Tuple[float, float],
	*,
	mode: str,
	drone_type: str = "cargo",
	near_m: float = 120.0,
) -> List[Dict[str, Any]]:
	"""
	If endpoints are near the infrastructure graph, expand attribution along shortest_path
	between nearest nodes instead of painting the whole chord as off_network.
	"""
	if G is None or G.number_of_edges() == 0:
		return [_route_segment_single(a, b, mode=mode)]
	try:
		n1 = _routing_service._find_nearest_node(G, a)
		n2 = _routing_service._find_nearest_node(G, b)
		if n1 is None or n2 is None:
			return [_route_segment_single(a, b, mode=mode)]
		p1 = G.nodes[n1].get("pos")
		p2 = G.nodes[n2].get("pos")
		if not p1 or not p2:
			return [_route_segment_single(a, b, mode=mode)]
		p1t = tuple(p1)
		p2t = tuple(p2)
		da = float(haversine_m(a, p1t))
		db = float(haversine_m(b, p2t))
		if da > near_m or db > near_m:
			return [_route_segment_single(a, b, mode=mode)]
		if n1 == n2:
			return [_route_segment_single(a, b, mode=mode)]
		if G.has_edge(n1, n2):
			ed = G.get_edge_data(n1, n2) or {}
			lay = str(ed.get("layer") or "off_network")
			fl = int(ed.get("flight_level") or 2)
			out: List[Dict[str, Any]] = []
			if da > 3.0:
				out.append(_route_segment_single(a, p1t, mode=mode, layer="off_network", echelon=2))
			out.append(_route_segment_single(p1t, p2t, mode=mode, layer=lay, echelon=fl))
			if db > 3.0:
				out.append(_route_segment_single(p2t, b, mode=mode, layer="off_network", echelon=2))
			return out
		path_nodes = nx.shortest_path(
			G,
			n1,
			n2,
			weight=lambda u, v, d: _infra_edge_weight_m(d or {}, drone_type),
		)
		out = []
		if da > 3.0:
			out.append(_route_segment_single(a, p1t, mode=mode, layer="off_network", echelon=2))
		out.extend(_infra_subpath_segments(G, path_nodes, mode=mode))
		if db > 3.0:
			out.append(_route_segment_single(p2t, b, mode=mode, layer="off_network", echelon=2))
		return out if out else [_route_segment_single(a, b, mode=mode)]
	except Exception:
		return [_route_segment_single(a, b, mode=mode)]


def _route_segments_from_coords(
	coords: List[Tuple[float, float]],
	*,
	mode: str,
	drone_type: str = "cargo",
) -> List[Dict[str, Any]]:
	"""Build per-edge segments for UI/logic. Uses infra edge attributes when available; else off_network."""
	if not coords or len(coords) < 2:
		return []
	G = STATE.get("infrastructure_graph")
	segments: List[Dict[str, Any]] = []
	for i in range(1, len(coords)):
		a = tuple(coords[i - 1])
		b = tuple(coords[i])
		if G is not None and G.number_of_edges() > 0:
			try:
				segments.extend(
					_infra_leg_segments_or_fallback(
						G, a, b, mode=mode, drone_type=drone_type, near_m=OFF_NETWORK_NEAR_INFRA_CONNECT_M
					)
				)
				continue
			except Exception:
				pass
		segments.append(_route_segment_single(a, b, mode=mode))
	return segments


def _off_network_segments_from_coords(
	coords: List[Tuple[float, float]],
	*,
	mode: str,
) -> List[Dict[str, Any]]:
	"""Build free-flight segments without snapping them back to infrastructure."""
	out: List[Dict[str, Any]] = []
	if not coords or len(coords) < 2:
		return out
	for i in range(1, len(coords)):
		out.append(_route_segment_single(tuple(coords[i - 1]), tuple(coords[i]), mode=mode, layer="off_network", echelon=2))
	return out


def _infra_route_diagnostics_from_path(
	infra_G: nx.Graph,
	path_nodes: List[Any],
	*,
	off_a: float,
	off_b: float,
	start_end_m: float,
	drone_type: str = "cargo",
) -> Dict[str, Any]:
	trunk_m = branch_m = local_m = voronoi_m = 0.0
	sum_w = 0.0
	for i in range(1, len(path_nodes)):
		ed = infra_G.get_edge_data(path_nodes[i - 1], path_nodes[i]) or {}
		Lm = float(ed.get("length_m") or 0.0)
		if Lm <= 0.0:
			pu = infra_G.nodes[path_nodes[i - 1]].get("pos")
			pv = infra_G.nodes[path_nodes[i]].get("pos")
			if pu and pv:
				Lm = float(haversine_m(tuple(pu), tuple(pv)))
		sum_w += float(_infra_edge_weight_m(ed, drone_type))
		lay = str((ed.get("layer") or "")).lower()
		if lay == "trunk":
			trunk_m += Lm
		elif lay == "branch":
			branch_m += Lm
		elif lay == "local":
			local_m += Lm
		elif lay == "voronoi":
			voronoi_m += Lm
	off_network_m = float(off_a + off_b)
	base = {
		"trunk_m": float(trunk_m),
		"branch_m": float(branch_m),
		"local_m": float(local_m),
		"voronoi_m": float(voronoi_m),
		"off_network_m": float(off_network_m),
		"weighted_infra_sum": float(sum_w),
		"start_end_m": float(start_end_m),
	}
	return _enrich_route_diagnostics(base)


def _infra_route_diagnostics_from_segments(route_segments: List[Dict[str, Any]], *, start_end_m: float) -> Dict[str, Any]:
	trunk_m = branch_m = local_m = voronoi_m = off_network_m = 0.0
	for s in route_segments or []:
		Lm = float(s.get("length_m") or 0.0)
		lay = str((s.get("layer") or "off_network")).lower()
		if lay == "trunk":
			trunk_m += Lm
		elif lay == "branch":
			branch_m += Lm
		elif lay == "local":
			local_m += Lm
		elif lay == "voronoi":
			voronoi_m += Lm
		else:
			off_network_m += Lm
	base = {
		"trunk_m": float(trunk_m),
		"branch_m": float(branch_m),
		"local_m": float(local_m),
		"voronoi_m": float(voronoi_m),
		"off_network_m": float(off_network_m),
		"start_end_m": float(start_end_m),
	}
	return _enrich_route_diagnostics(base)


def plan_infrastructure_preferred_route(
	start_coord: Tuple[float, float],
	end_coord: Tuple[float, float],
	drone_type: str,
	mode: str = "empty",
	prefer_network: bool = True,
	*,
	long_leg_quality_threshold_m: float = 0.0,
	long_leg_min_infra_ratio: float = 0.7,
	long_leg_max_off_network_ratio: float = 0.3,
	long_leg_extra_pairs: int = 0,
) -> Tuple[Optional[List[Any]], Optional[List[Tuple[float, float]]], float, List[Dict[str, Any]], Dict[str, Any]]:
	"""
	Sets up network-preferred routing on infrastructure_graph (trunk/branch/local/voronoi weights).
	Returns (path_nodes, coords, length_m, route_segments, diagnostics).
	"""
	empty_diag: Dict[str, Any] = {}
	infra_G = STATE.get("infrastructure_graph")
	if infra_G is None or infra_G.number_of_nodes() == 0 or infra_G.number_of_edges() == 0:
		return None, None, 0.0, [], empty_diag

	start_n = normalize_latlon(start_coord, ctx="infra_route.start") or (float(start_coord[0]), float(start_coord[1]))
	end_n = normalize_latlon(end_coord, ctx="infra_route.end") or (float(end_coord[0]), float(end_coord[1]))
	start = (float(start_n[0]), float(start_n[1]))
	end = (float(end_n[0]), float(end_n[1]))
	start_end_m = float(haversine_m(start, end))

	graph_has_trunk = _graph_has_trunk_edges(infra_G)
	_use_long = (
		float(long_leg_quality_threshold_m) > 1e-6
		and float(start_end_m) > float(long_leg_quality_threshold_m)
		and _infra_graph_has_air_network_edges(infra_G)
	)
	_extra_pairs = int(long_leg_extra_pairs) if int(long_leg_extra_pairs) > 0 else (56 if _use_long else 0)

	def _passes_long_leg_quality(diag: Dict[str, Any]) -> bool:
		if not _use_long:
			return True
		d = _enrich_route_diagnostics(dict(diag))
		ir = float(d.get("infra_ratio") or 0.0)
		offr = float(d.get("off_network_ratio") or 1.0)
		return ir + 1e-9 >= float(long_leg_min_infra_ratio) and offr <= float(long_leg_max_off_network_ratio) + 1e-9

	# 1) Строгий маршрут: start → local/voronoi → branch → trunk → branch → local/voronoi → end
	if prefer_network:
		pn, co, ln, segs = plan_infrastructure_strict_layer_route(
			start_coord, end_coord, drone_type, mode=mode
		)
		if co and segs:
			diag = _infra_route_diagnostics_from_segments(segs, start_end_m=start_end_m)
			diag["strict_layer"] = True
			ok_q, _, reason_q = _validate_infra_route_quality(
				diag, graph_has_trunk=graph_has_trunk, drone_type=drone_type
			)
			if ok_q and _passes_long_leg_quality(diag):
				STATE["last_infra_route_diagnostics"] = dict(diag)
				return pn, co, ln, segs, diag
			if ok_q and not _passes_long_leg_quality(diag):
				dq = _enrich_route_diagnostics(dict(diag))
				logger.debug(
					"plan_infrastructure_preferred_route: strict rejected long_leg_quality drone_type=%s infra_ratio=%.3f off_ratio=%.3f need_infra>=%.2f off<=%.2f",
					drone_type,
					float(dq.get("infra_ratio") or 0.0),
					float(dq.get("off_network_ratio") or 0.0),
					float(long_leg_min_infra_ratio),
					float(long_leg_max_off_network_ratio),
				)
			elif not ok_q:
				logger.debug(
					"plan_infrastructure_preferred_route: strict rejected drone_type=%s reason=%s trunk_ratio=%.3f infra_ratio=%.3f off%%=%.1f",
					drone_type,
					reason_q,
					float(diag.get("trunk_ratio") or 0.0),
					float(diag.get("infra_ratio") or 0.0),
					100.0 * float(diag.get("off_network_ratio") or 0.0),
				)

	if haversine_m(start, end) > 30.0:
		logger.debug("infra_route: start=%s end=%s", start, end)

	def _ech_penalty_for_path(path_nodes: List[Any]) -> float:
		prev_fl = None
		changes = 0
		for i in range(1, len(path_nodes)):
			ed = infra_G.get_edge_data(path_nodes[i - 1], path_nodes[i]) or {}
			fl = ed.get("flight_level")
			if fl is not None and prev_fl is not None and int(fl) != int(prev_fl):
				changes += 1
			if fl is not None:
				prev_fl = int(fl)
		return float(changes) * ECHELON_CHANGE_PENALTY_M

	def _coords_from_entry_exit(
		path_nodes: List[Any],
		entry_pos: Tuple[float, float],
		exit_pos: Tuple[float, float],
		off_a_m: float,
		off_b_m: float,
	) -> List[Tuple[float, float]]:
		infra_coords = [tuple(infra_G.nodes[n].get("pos")) for n in path_nodes if infra_G.nodes[n].get("pos")]
		coords: List[Tuple[float, float]] = [start]
		if off_a_m > 5.0:
			coords.append(entry_pos)
		for p in infra_coords:
			if coords and coords[-1] == tuple(p):
				continue
			coords.append(tuple(p))
		if off_b_m > 5.0:
			if coords[-1] != exit_pos:
				coords.append(exit_pos)
			coords.append(end)
		else:
			if coords[-1] != end:
				coords.append(end)
		return coords

	def _consider_path(path_nodes: List[Any], entry_exit_bias: float) -> None:
		nonlocal best, best_score, best_diag
		if not path_nodes or len(path_nodes) < 1:
			return
		entry_pos = infra_G.nodes[path_nodes[0]].get("pos")
		exit_pos = infra_G.nodes[path_nodes[-1]].get("pos")
		if not entry_pos or not exit_pos:
			return
		entry_pos = tuple(entry_pos)
		exit_pos = tuple(exit_pos)
		off_a = float(haversine_m(start, entry_pos))
		off_b = float(haversine_m(exit_pos, end))
		if off_a > OFF_NETWORK_MAX_ENTRY_EXIT_M or off_b > OFF_NETWORK_MAX_ENTRY_EXIT_M:
			return
		diag = _infra_route_diagnostics_from_path(
			infra_G, path_nodes, off_a=off_a, off_b=off_b, start_end_m=start_end_m, drone_type=drone_type
		)
		ok_q, _, _reason_q = _validate_infra_route_quality(
			diag, graph_has_trunk=graph_has_trunk, drone_type=drone_type
		)
		if not ok_q:
			return
		if not _passes_long_leg_quality(diag):
			return
		coords = _coords_from_entry_exit(path_nodes, entry_pos, exit_pos, off_a, off_b)
		total_len = float(_route_length(coords))
		sum_w = float(diag.get("weighted_infra_sum") or 0.0)
		ech_pen = _ech_penalty_for_path(path_nodes)
		trunk_m = float(diag.get("trunk_m") or 0.0)
		trunk_bonus = 0.28 * trunk_m
		_off_leg = float(INFRA_DRONE_PROFILES[_normalized_drone_type(drone_type)].get("off_leg", OFF_NETWORK_LEG_WEIGHT_MULT))
		off_cost = float(off_a + off_b) * _off_leg
		score = float(sum_w + off_cost + ech_pen + float(entry_exit_bias) * 0.05 - trunk_bonus)
		if score < best_score:
			best_score = score
			best = (path_nodes, coords, total_len)
			best_diag = dict(diag)
			best_diag["score"] = float(score)
			best_diag["strict_layer"] = False

	start_candidates = _infra_graph_nearest_nodes(infra_G, start, MAX_NEAREST_NETWORK_NODES)
	end_candidates = _infra_graph_nearest_nodes(infra_G, end, MAX_NEAREST_NETWORK_NODES)
	if not start_candidates or not end_candidates:
		logger.debug("infra_route: no entry/exit candidates start=%s end=%s", start, end)
		return None, None, 0.0, [], empty_diag

	start_candidates.sort(key=lambda x: x[1])
	end_candidates.sort(key=lambda x: x[1])

	pairs: List[Tuple[Any, Any, float]] = []
	for sn, sd in start_candidates[:MAX_NEAREST_NETWORK_NODES]:
		for en, ed in end_candidates[:MAX_NEAREST_NETWORK_NODES]:
			pairs.append((sn, en, float(sd + ed)))
	pairs.sort(key=lambda x: x[2])
	_pair_cap = min(len(pairs), MAX_ROUTE_CANDIDATES + int(_extra_pairs))
	pairs = pairs[:_pair_cap]

	best = None
	best_score = float("inf")
	best_diag: Dict[str, Any] = {}

	for entry_node, exit_node, entry_exit_bias in pairs:
		try:
			if entry_node not in infra_G.nodes or exit_node not in infra_G.nodes:
				continue
			path_nodes = nx.shortest_path(
				infra_G,
				entry_node,
				exit_node,
				weight=lambda u, v, d: _infra_edge_weight_m(d or {}, drone_type),
			)
			_consider_path(path_nodes, entry_exit_bias)
		except Exception:
			continue

	# Через порталы у зарядок — ищем магистральный обход при плохом прямом shortest_path
	portals = _charging_station_infra_portal_nodes(infra_G)
	_portal_pair_n = 24 if _use_long else 14
	for entry_node, exit_node, entry_exit_bias in pairs[:_portal_pair_n]:
		if entry_node not in infra_G.nodes or exit_node not in infra_G.nodes:
			continue
		for mid in portals[:16]:
			if mid not in infra_G.nodes:
				continue
			try:
				p1 = nx.shortest_path(
					infra_G,
					entry_node,
					mid,
					weight=lambda u, v, d: _infra_edge_weight_m(d or {}, drone_type),
				)
				p2 = nx.shortest_path(
					infra_G,
					mid,
					exit_node,
					weight=lambda u, v, d: _infra_edge_weight_m(d or {}, drone_type),
				)
				if not p1 or not p2:
					continue
				merged = list(p1) + list(p2[1:])
				_consider_path(merged, entry_exit_bias + 120.0)
			except Exception:
				continue

	if not best:
		return None, None, 0.0, [], empty_diag
	path_nodes, coords, total_len = best
	route_segments = _route_segments_from_coords(coords, mode=mode, drone_type=drone_type)
	best_diag = _enrich_route_diagnostics(_infra_route_diagnostics_from_segments(route_segments, start_end_m=start_end_m))
	best_diag.setdefault("score", float(best_score))
	best_diag["strict_layer"] = False
	STATE["last_infra_route_diagnostics"] = dict(best_diag)
	return path_nodes, coords, float(total_len), route_segments, best_diag


def _extract_station_list_from_fc(fc: Dict[str, Any], *, station_type: str) -> List[Dict[str, Any]]:
	out: List[Dict[str, Any]] = []
	if not isinstance(fc, dict) or fc.get("type") != "FeatureCollection":
		return out
	for i, f in enumerate(fc.get("features") or []):
		try:
			geom = (f or {}).get("geometry") or {}
			if (geom.get("type") or "").lower() != "point":
				continue
			lon, lat = geom.get("coordinates") or [None, None]
			if lat is None or lon is None:
				continue
			props = (f or {}).get("properties") or {}
			cid = props.get("cluster_id")
			cap = props.get("cluster_drones_total")
			# fallback to legacy capacity naming
			if cap is None:
				cap = props.get("cluster_drones_capacity")
			try:
				cap_i = int(round(float(cap))) if cap is not None else None
			except Exception:
				cap_i = None
			sid = props.get("id") or f"{station_type}_{i}"
			out.append(
				{
					"id": str(sid),
					"station_type": station_type,
					"pos": (float(lat), float(lon)),
					"cluster_id": cid,
					"capacity": cap_i,
					"cluster_drones_total": props.get("cluster_drones_total"),
					"cluster_drones_cargo": props.get("cluster_drones_cargo"),
					"cluster_drones_monitoring": props.get("cluster_drones_monitoring"),
					"cluster_drones_service": props.get("cluster_drones_service"),
					"vertical_echelons_connected": props.get("vertical_echelons_connected"),
				}
			)
		except Exception:
			continue
	return out


def _gdf_or_list_to_fc(obj: Any) -> Dict[str, Any]:
	"""Normalize demand_hulls/no_fly_zones-like objects into GeoJSON FeatureCollection."""
	if obj is None:
		return {"type": "FeatureCollection", "features": []}
	if isinstance(obj, dict) and obj.get("type") == "FeatureCollection":
		return obj
	# GeoDataFrame-like: prefer to_json if present (avoid hard dependency checks)
	try:
		to_json = getattr(obj, "to_json", None)
		if callable(to_json):
			return json.loads(to_json())
	except Exception:
		pass
	# Fallback: iterate rows and shapely mapping (works for GeoDataFrame without importing geopandas here)
	try:
		from shapely.geometry import mapping
		import pandas as pd
		geom_col = getattr(obj, "geometry", None)
		iterrows = getattr(obj, "iterrows", None)
		cols = list(getattr(obj, "columns", []) or [])
		if callable(iterrows) and geom_col is not None and "geometry" in cols:
			features: List[Dict[str, Any]] = []
			for _, row in iterrows():
				geom = getattr(row, "geometry", None)
				if geom is None or getattr(geom, "is_empty", True):
					continue
				props: Dict[str, Any] = {}
				for col in cols:
					if col == "geometry":
						continue
					val = row[col]
					try:
						if pd.isna(val):
							props[col] = None
						elif hasattr(val, "item"):
							props[col] = val.item()
						else:
							props[col] = val
					except Exception:
						props[col] = str(val)
				features.append({"type": "Feature", "geometry": mapping(geom), "properties": props})
			return {"type": "FeatureCollection", "features": features}
	except Exception:
		pass
	return {"type": "FeatureCollection", "features": []}


def _round_geojson_coords(obj, precision: int = 6):
	"""Round float coords in GeoJSON for stable payloads (same idea as app.py)."""
	if isinstance(obj, dict):
		return {k: _round_geojson_coords(v, precision) for k, v in obj.items()}
	if isinstance(obj, list):
		return [_round_geojson_coords(v, precision) for v in obj]
	if isinstance(obj, float):
		return round(obj, precision)
	return obj


def _pipeline_params_app_defaults() -> Dict[str, Any]:
	"""Must match defaults from app.py get_stations_placement."""
	p = {
		"network_type": "drive",
		"simplify": True,
		"demand_method": "dbscan",
		"demand_cell_m": 250.0,
		"dbscan_eps_m": 180.0,
		"dbscan_min_samples": 15,
		"a_by_admin_districts": True,
		"candidates_per_cluster": 25,
		"use_all_buildings": False,
		"voronoi_buildings_per_centroid": 60,
		"inter_cluster_max_hull_gap_m": 2000.0,
		"inter_cluster_max_edge_length_m": 2000.0,
		"voronoi_intra_component_bridge_max_m": 600.0,
		# Disable forced-per-cluster/per-region explosion (keeps A/B/local sane without empty-hull forced fallback).
		"force_type_b_per_cluster": False,
		"force_type_a_per_region": False,
	}
	return p


def _placement_cache_key(city: str, params: Dict[str, Any]) -> str:
	"""Same deterministic cache key scheme as app.py (placement_schema must match)."""
	payload = {
		"city": (city or "").strip(),
		"network_type": str(params.get("network_type") or "drive"),
		"simplify": bool(params.get("simplify", True)),
		"dbscan_eps_m": float(params.get("dbscan_eps_m", 180.0)),
		"dbscan_min_samples": int(params.get("dbscan_min_samples", 15)),
		"buildings_per_centroid": int(params.get("voronoi_buildings_per_centroid", 60)),
		"inter_cluster_max_hull_gap_m": float(params.get("inter_cluster_max_hull_gap_m", 2000.0)),
		"inter_cluster_max_edge_length_m": float(params.get("inter_cluster_max_edge_length_m", 2000.0)),
		"use_all_buildings": bool(params.get("use_all_buildings", False)),
		"voronoi_intra_bridge_max_m": float(params.get("voronoi_intra_component_bridge_max_m", 600.0)),
		"cache_buster": int(STATE.get("placement_cache_buster") or 0),
		"placement_schema": 46,
	}
	raw = json.dumps(payload, ensure_ascii=False, sort_keys=True)
	digest = hashlib.sha1(raw.encode("utf-8")).hexdigest()
	return f"drone_planner:placement:{digest}"


def _fc_count(x: Any) -> int:
	if isinstance(x, dict) and x.get("type") == "FeatureCollection":
		return len(x.get("features") or [])
	return 0


def _fc_count_voronoi_by_echelon(geo: Dict[str, Any]) -> int:
	"""Total Feature count across voronoi_edges_by_echelon sub-layers."""
	vbe = (geo or {}).get("voronoi_edges_by_echelon") or {}
	if not isinstance(vbe, dict):
		return 0
	n = 0
	for sub in vbe.values():
		n += _fc_count(sub if isinstance(sub, dict) else {})
	return int(n)


def _voronoi_layers_empty(geo: Dict[str, Any]) -> bool:
	"""True when neither flat voronoi_edges nor per-echelon collections have features."""
	if not isinstance(geo, dict):
		return True
	if _fc_count(geo.get("voronoi_edges") or {}) > 0:
		return False
	return _fc_count_voronoi_by_echelon(geo) <= 0


def _geo_has_nonempty_path_layers(geo: Dict[str, Any]) -> bool:
	"""True if any GeoJSON layer that should contribute to infrastructure_graph has features."""
	if not isinstance(geo, dict):
		return False
	for k in ("trunk", "branch_edges", "local_edges", "voronoi_edges"):
		if _fc_count(geo.get(k) or {}) > 0:
			return True
	return _fc_count_voronoi_by_echelon(geo) > 0


def _layer_counts(geo: Dict[str, Any]) -> Dict[str, int]:
	return {
		"charge_a": _fc_count(geo.get("charging_type_a") or {}),
		"charge_b": _fc_count(geo.get("charging_type_b") or {}),
		"maintenance": _fc_count(geo.get("to_stations") or {}),
		"garages": _fc_count(geo.get("garages") or {}),
		"trunk": _fc_count(geo.get("trunk") or {}),
		"branch": _fc_count(geo.get("branch_edges") or {}),
		"local": _fc_count(geo.get("local_edges") or {}),
		"voronoi": _fc_count(geo.get("voronoi_edges") or {}),
		"voronoi_by_echelon": _fc_count_voronoi_by_echelon(geo),
		"cluster_hulls": _fc_count(geo.get("cluster_hulls") or {}),
	}


def _pipeline_raw_counts(raw: Any) -> Dict[str, int]:
	"""Best-effort counts from raw pipeline result (may contain GeoDataFrames)."""
	if not isinstance(raw, dict):
		return {}
	out: Dict[str, int] = {}
	for k in ("demand", "demand_hulls", "charge_stations", "garages", "to_stations", "trunk_edges", "branch_edges", "local_edges", "voronoi_edges"):
		v = raw.get(k)
		try:
			out[k] = int(len(v)) if v is not None else 0
		except Exception:
			out[k] = 0
	return out


def _ensure_infrastructure_build(city: str, *, force: bool = False) -> bool:
	"""Start build if needed. Returns True if started, False if skipped."""
	city = (city or "").strip()
	if not city:
		return False
	status = STATE.get("infrastructure_build_status")
	cur_city = (STATE.get("infrastructure_build_city") or "").strip()
	if status == "building" and cur_city.lower() == city.lower():
		logger.info("infra build skipped: already building city=%s", city)
		return False
	if (
		not force
		and status == "ready"
		and cur_city.lower() == city.lower()
		and (STATE.get("infrastructure_version") or 0) > 0
		and isinstance(STATE.get("infrastructure_geojson"), dict)
		and STATE["infrastructure_geojson"]
	):
		logger.info("infra build skipped: already ready city=%s version=%s", city, STATE.get("infrastructure_version"))
		return False

	# Force rebuild: clear previous infra artifacts so callers don't read stale data while building.
	if force:
		_clear_infrastructure_runtime()

	STATE["infrastructure_build_status"] = "building"
	STATE["infrastructure_build_city"] = city
	STATE["infrastructure_build_started_at"] = datetime.utcnow().isoformat()
	STATE["infrastructure_build_finished_at"] = None
	STATE["infrastructure_build_last_error"] = None
	STATE["infrastructure_build_stage"] = "starting"
	STATE["infrastructure_build_stage_detail"] = None
	STATE["infrastructure_build_stage_ts"] = datetime.utcnow().isoformat()

	async def _task():
		try:
			pp = _pipeline_params_app_defaults()
			logger.info("infra build start city=%s params=%s", city, pp)
			STATE["infrastructure_build_stage"] = "run_full_pipeline"
			STATE["infrastructure_build_stage_ts"] = datetime.utcnow().isoformat()
			await asyncio.to_thread(rebuild_infrastructure_for_city, city, force=force)
			if STATE.get("infrastructure_graph_invalid"):
				STATE["infrastructure_build_status"] = "error"
				STATE["infrastructure_build_last_error"] = str(
					STATE.get("infrastructure_graph_invalid_reason") or "infrastructure_graph has zero edges with non-empty path GeoJSON"
				)
				STATE["infrastructure_build_finished_at"] = datetime.utcnow().isoformat()
				STATE["infrastructure_build_stage"] = "error"
				STATE["infrastructure_build_stage_ts"] = datetime.utcnow().isoformat()
				logger.error(
					"infra build finished with graph error city=%s version=%s detail=%s",
					city,
					STATE.get("infrastructure_version"),
					STATE.get("infrastructure_build_last_error"),
				)
			else:
				STATE["infrastructure_build_status"] = "ready"
				STATE["infrastructure_build_finished_at"] = datetime.utcnow().isoformat()
				STATE["infrastructure_build_stage"] = "ready"
				STATE["infrastructure_build_stage_ts"] = datetime.utcnow().isoformat()
				logger.info(
					"infra build done city=%s version=%s counts=%s graph=%s",
					city,
					STATE.get("infrastructure_version"),
					_layer_counts(STATE.get("infrastructure_geojson") or {}),
					STATE.get("infrastructure_graph_counts") or {},
				)
		except Exception as e:
			STATE["infrastructure_build_status"] = "error"
			STATE["infrastructure_build_last_error"] = str(e)
			STATE["infrastructure_build_finished_at"] = datetime.utcnow().isoformat()
			STATE["infrastructure_build_stage"] = "error"
			STATE["infrastructure_build_stage_ts"] = datetime.utcnow().isoformat()
			logger.exception("infra build failed city=%s", city)

	asyncio.create_task(_task())
	return True


def _clear_infrastructure_runtime() -> None:
	STATE["infrastructure"] = None
	STATE["infrastructure_geojson"] = None
	STATE["infrastructure_graph"] = None
	STATE["infrastructure_version"] = 0
	STATE["infrastructure_raw_counts"] = {}
	STATE["infrastructure_pipeline_params"] = {}
	STATE["infrastructure_build_stage"] = None
	STATE["infrastructure_build_stage_detail"] = None
	STATE["infrastructure_build_stage_ts"] = None
	STATE.pop("_infra_cluster_index", None)
	STATE["infrastructure_graph_counts"] = {}
	STATE["infrastructure_graph_invalid"] = False
	STATE.pop("infrastructure_graph_invalid_reason", None)
	STATE["last_infra_route_diagnostics"] = {}
	try:
		G = STATE.get("infrastructure_graph")
		if G is not None and getattr(G, "graph", None) is not None:
			G.graph.pop("_infra_node_kdtree", None)
	except Exception:
		pass


def rebuild_infrastructure_for_city(city: str, *, force: bool = False) -> None:
	"""
	Build infrastructure for the given city using the same pipeline as app.py,
	store it into STATE, and update legacy `stations` for backward-compatible charging logic.
	"""
	city = (city or "").strip()
	if not city:
		raise ValueError("city is required")

	STATE["infrastructure_graph_invalid"] = False
	STATE.pop("infrastructure_graph_invalid_reason", None)

	pp = _pipeline_params_app_defaults()
	STATE["infrastructure_pipeline_params"] = dict(pp)
	redis_client = None
	try:
		redis_client = _infra_data_service.get_redis_client()
	except Exception:
		redis_client = None

	cache_key = _placement_cache_key(city, pp)
	if redis_client is not None and not force:
		try:
			cached = redis_client.get(cache_key)
			if cached:
				geo = json.loads(cached.decode("utf-8") if isinstance(cached, (bytes, bytearray)) else cached)
				# ensure rounding-compatible payload
				geo = _round_geojson_coords(geo, precision=6)
				# keep cluster hulls if present in cached payload
				raw = {"demand_hulls": None}  # for infra normalization only
				logger.info("infra placement loaded from Redis cache key=%s", cache_key)
			else:
				geo = None
		except Exception:
			geo = None
	else:
		geo = None

	if geo is None:
		# Attach best-effort progress callback for stage hints
		try:
			def _cb(stage: str, pct: int, message: str):
				STATE["infrastructure_build_stage"] = str(stage)
				STATE["infrastructure_build_stage_detail"] = f"{int(pct)}% {message}"
				STATE["infrastructure_build_stage_ts"] = datetime.utcnow().isoformat()
			if hasattr(_infra_data_service, "progress_callbacks"):
				cbs = getattr(_infra_data_service, "progress_callbacks") or []
				if _cb not in cbs:
					cbs.append(_cb)
					_infra_data_service.progress_callbacks = cbs
		except Exception:
			pass
		STATE["infrastructure_build_stage"] = "run_full_pipeline"
		STATE["infrastructure_build_stage_ts"] = datetime.utcnow().isoformat()
		raw = run_full_pipeline(_infra_data_service, city, force_refresh=bool(force), **pp)
		STATE["infrastructure_raw_counts"] = _pipeline_raw_counts(raw)
		STATE["infrastructure_build_stage"] = "pipeline_result_to_geojson"
		STATE["infrastructure_build_stage_ts"] = datetime.utcnow().isoformat()
		geo = pipeline_result_to_geojson(raw)
		geo = _round_geojson_coords(geo, precision=6)
		STATE["infrastructure_build_stage"] = "add_cluster_hulls"
		STATE["infrastructure_build_stage_ts"] = datetime.utcnow().isoformat()
		# Добавляем только cluster_hulls (в app.py оно выдаётся отдельным эндпоинтом /api/buildings/clusters)
		cluster_hulls_fc = _gdf_or_list_to_fc(raw.get("demand_hulls"))
		geo["cluster_hulls"] = _round_geojson_coords(cluster_hulls_fc, precision=6)
		# Save freshly built placement into Redis (bugfix: previously saved only when loaded from cache).
		if redis_client is not None:
			try:
				redis_client.set(cache_key, json.dumps(geo, ensure_ascii=False).encode("utf-8"))
				logger.info("infra placement saved to Redis cache key=%s", cache_key)
			except Exception:
				logger.warning("infra placement failed to save to Redis")
	else:
		STATE["infrastructure_raw_counts"] = {}

	# Voronoi fallback for BOTH fresh pipeline and Redis cache: empty voronoi_edges AND empty by_echelon.
	if _voronoi_layers_empty(geo):
		STATE["infrastructure_build_stage"] = "build_voronoi_local_paths_fc"
		STATE["infrastructure_build_stage_ts"] = datetime.utcnow().isoformat()
		try:
			vfc = build_voronoi_local_paths_fc(
				_infra_data_service,
				city=city,
				network_type=str(pp.get("network_type") or "drive"),
				simplify=bool(pp.get("simplify", True)),
				dbscan_eps_m=float(pp.get("dbscan_eps_m", 180.0)),
				dbscan_min_samples=int(pp.get("dbscan_min_samples", 15)),
				use_all_buildings=bool(pp.get("use_all_buildings", False)),
				buildings_per_centroid=int(pp.get("voronoi_buildings_per_centroid", 60)),
				inter_cluster_max_hull_gap_m=float(pp.get("inter_cluster_max_hull_gap_m", 2000.0)),
				inter_cluster_max_edge_length_m=float(pp.get("inter_cluster_max_edge_length_m", 2000.0)),
				voronoi_intra_component_bridge_max_m=float(pp.get("voronoi_intra_component_bridge_max_m", 600.0)),
			)
			geo["voronoi_edges"] = _round_geojson_coords(vfc, precision=6)
			logger.info(
				"voronoi fallback: edges=%s by_echelon=%s",
				_fc_count(geo.get("voronoi_edges") or {}),
				_fc_count_voronoi_by_echelon(geo),
			)
			if redis_client is not None:
				try:
					redis_client.set(cache_key, json.dumps(geo, ensure_ascii=False).encode("utf-8"))
					logger.info("infra placement re-saved to Redis after voronoi fallback key=%s", cache_key)
				except Exception:
					logger.warning("infra placement failed to re-save Redis after voronoi fallback")
		except Exception:
			logger.exception("build_voronoi_local_paths_fc fallback failed city=%s", city)

	charging_a = geo.get("charging_type_a") or {"type": "FeatureCollection", "features": []}
	charging_b = geo.get("charging_type_b") or {"type": "FeatureCollection", "features": []}
	charging_stations = _extract_station_list_from_fc(charging_a, station_type="charge_a") + _extract_station_list_from_fc(
		charging_b, station_type="charge_b"
	)

	infra = {
		"charging_stations": charging_stations,
		"maintenance_stations": geo.get("to_stations"),
		"garages": geo.get("garages"),
		"trunk_paths": geo.get("trunk"),
		"branch_paths": geo.get("branch_edges"),
		"local_paths": geo.get("voronoi_edges"),
		"clusters": {
			"centroids": geo.get("cluster_centroids"),
			"hulls": geo.get("cluster_hulls"),
		},
		"flight_levels": geo.get("flight_levels") or [],
	}
	# Keep raw separately (not JSON-serializable; avoid breaking /api/infrastructure)
	STATE["infrastructure_raw"] = raw

	# Build graph for routing.
	STATE["infrastructure_build_stage"] = "build_infrastructure_graph"
	STATE["infrastructure_build_stage_ts"] = datetime.utcnow().isoformat()
	infra_graph = build_infrastructure_graph(geo)

	trunk_n = _fc_count(geo.get("trunk") or {})
	branch_n = _fc_count(geo.get("branch_edges") or {})
	local_n = _fc_count(geo.get("local_edges") or {})
	voro_n = _fc_count(geo.get("voronoi_edges") or {})
	voro_echelon_n = _fc_count_voronoi_by_echelon(geo)
	graph_nodes = int(infra_graph.number_of_nodes()) if infra_graph is not None else 0
	graph_edges = int(infra_graph.number_of_edges()) if infra_graph is not None else 0
	STATE["infrastructure_graph_counts"] = {
		"trunk_edges": trunk_n,
		"branch_edges": branch_n,
		"local_edges": local_n,
		"voronoi_edges": voro_n,
		"voronoi_edges_by_echelon": voro_echelon_n,
		"graph_nodes": graph_nodes,
		"graph_edges": graph_edges,
		"layer_counts": dict((infra_graph.graph or {}).get("layer_counts") or {}) if infra_graph is not None else {},
	}
	if graph_edges == 0 and _geo_has_nonempty_path_layers(geo):
		msg = (
			f"infrastructure_graph has zero edges but GeoJSON path layers are non-empty "
			f"(trunk={trunk_n} branch={branch_n} local={local_n} voronoi={voro_n} voronoi_by_echelon={voro_echelon_n})"
		)
		logger.error(msg)
		STATE["infrastructure_graph_invalid"] = True
		STATE["infrastructure_graph_invalid_reason"] = msg

	# Update STATE atomically-ish.
	STATE["infrastructure"] = infra
	STATE["infrastructure_geojson"] = geo
	STATE["infrastructure_graph"] = infra_graph
	STATE["infrastructure_version"] = int(STATE.get("infrastructure_version") or 0) + 1
	# reset cluster index cache
	STATE.pop("_infra_cluster_index", None)

	# Backward-compat: legacy chargers list is `STATE["stations"]` as (lat, lon)
	legacy_stations: List[Tuple[float, float]] = []
	for st in charging_stations:
		pos = st.get("pos")
		if isinstance(pos, (list, tuple)) and len(pos) == 2:
			legacy_stations.append((float(pos[0]), float(pos[1])))
	STATE["stations"] = legacy_stations
	STATE["station_queues"] = {
		str(i): {
			"charged_batteries": STATION_CHARGED_BATTERIES_MAX,
			"charging_queue": [],
			"queue": [],
		}
		for i in range(len(legacy_stations))
	}
	# IMPORTANT: refresh charger_nodes after BOTH infrastructure_graph and stations are updated,
	# so charging logic binds to the current city_graph with the latest station list.
	try:
		refresh_charger_nodes()
	except Exception:
		logger.exception("refresh_charger_nodes after infra rebuild failed")
	# (Re)spawn drones from infrastructure capacity (no artificial caps).
	# Replace base-inventory spawn (50) once infra charging stations are available.
	try:
		STATE["infrastructure_build_stage"] = "spawn_drones"
		STATE["infrastructure_build_stage_ts"] = datetime.utcnow().isoformat()
		_spawn_drones_from_infra_if_ready(force=force)
	except Exception:
		logger.exception("spawn drones after infra failed")


def _spawn_drones_from_infra_if_ready(*, force: bool = False) -> None:
	infra = STATE.get("infrastructure") or {}
	charging = (infra.get("charging_stations") or []) if isinstance(infra, dict) else []
	if not charging:
		# fallback: at least ensure base drones exist
		ensure_base_drones()
		return
	drones = STATE.get("drones") or {}
	all_idle = all((d or {}).get("status") in (None, "idle") for d in drones.values())
	all_from_base = drones and all((d or {}).get("home_station_id") == "base" for d in drones.values())
	if force or not drones or (all_idle and all_from_base):
		STATE["drones"] = {}
		ensure_base_drones()
		by_type = {"cargo": 0, "operator": 0, "cleaner": 0}
		for _id, d in (STATE.get("drones") or {}).items():
			t = (d or {}).get("type")
			if t in by_type:
				by_type[t] += 1
		logger.info(
			"spawn_drones stations=%s total=%s cargo=%s operator=%s cleaner=%s",
			len(charging),
			len(STATE.get("drones") or {}),
			by_type["cargo"],
			by_type["operator"],
			by_type["cleaner"],
		)


# Utilities
async def resolve_point(coords: Optional[List[float]], address: Optional[str]):
	def _normalize_latlon_pair(pair: Tuple[float, float]) -> Tuple[float, float]:
		lat, lon = float(pair[0]), float(pair[1])
		# Heuristic: choose orientation closer to Volgograd center when city is Volgograd-like
		city = (STATE.get("city") or "").lower()
		if "volgograd" in city or "волгоград" in city:
			c0 = (48.7080, 44.5133)
			d1 = haversine_m((lat, lon), c0)
			d2 = haversine_m((lon, lat), c0)
			if d2 + 10.0 < d1:
				logger.warning("coords look swapped for Volgograd: %s -> swapped", (lat, lon))
				return (lon, lat)
		# Generic bounds swap: if lat outside [-90,90] but lon inside, swap
		if abs(lat) > 90 and abs(lon) <= 90:
			logger.warning("coords lat out of range: swapping %s", (lat, lon))
			return (lon, lat)
		return (lat, lon)

	if coords and len(coords) == 2:
		try:
			return _normalize_latlon_pair((float(coords[0]), float(coords[1])))
		except Exception:
			return tuple(coords)
	if address:
		try:
			city = STATE["city"]
			out = _data_service.address_to_coords(address, city)
			if out and isinstance(out, (list, tuple)) and len(out) == 2:
				return _normalize_latlon_pair((float(out[0]), float(out[1])))
			return out
		except Exception:
			return None
	return None

def classify_order(order: AddOrderRequest) -> str:
	if order.type_hint in ("delivery", "shooting", "work"):
		return order.type_hint
	text = " ".join(filter(None, [order.address_from, order.address_to]))
	text = text.lower() if text else ""
	if any(k in text for k in ["достав", "deliver", "посыл", "parcel"]):
		return "delivery"
	if any(k in text for k in ["съём", "съем", "photo", "video", "aerial"]):
		return "shooting"
	return "work"

def _route_length(coords: List[Tuple[float, float]]) -> float:
	"""Приблизительная длина маршрута в метрах по списку координат."""
	if not coords or len(coords) < 2:
		return 0.0
	return sum(haversine_m(tuple(coords[i]), tuple(coords[i + 1])) for i in range(len(coords) - 1))


def _point_in_polygon(point: Tuple[float, float], polygon: List[Tuple[float, float]]) -> bool:
	"""Проверка: точка (lat, lon) внутри полигона (ray casting)."""
	if not polygon or len(polygon) < 3:
		return False
	lat, lon = point
	n = len(polygon)
	inside = False
	j = n - 1
	for i in range(n):
		lat_i, lon_i = polygon[i]
		lat_j, lon_j = polygon[j]
		if ((lon_i > lon) != (lon_j > lon)) and (lat < (lat_j - lat_i) * (lon - lon_i) / (lon_j - lon_i + 1e-20) + lat_i):
			inside = not inside
		j = i
	return inside


# Базовый шаг сетки облёта внутри зоны (м). Для городского осмотра держим плотнее.
OPERATOR_AREA_GRID_STEP_M = 30.0


def _lawnmower_waypoints_inside_polygon(polygon: List[Tuple[float, float]], step_m: float = OPERATOR_AREA_GRID_STEP_M) -> List[Tuple[float, float]]:
	"""
	Генерирует точки облёта внутри полигона (лаунмower): сетка с шагом step_m,
	только точки внутри полигона, порядок «змейкой» по строкам (lat).
	"""
	if not polygon or len(polygon) < 3:
		return []
	lats = [p[0] for p in polygon]
	lons = [p[1] for p in polygon]
	lat_min, lat_max = min(lats), max(lats)
	lon_min, lon_max = min(lons), max(lons)
	# Приближение: 1° широты ≈ 111 км, 1° долготы ≈ 111*cos(lat) км
	lat_mid = (lat_min + lat_max) / 2.0
	deg_per_m_lat = 1.0 / (111_000.0)
	deg_per_m_lon = 1.0 / (111_000.0 * max(0.01, math.cos(math.radians(lat_mid))))
	step_lat = step_m * deg_per_m_lat
	step_lon = step_m * deg_per_m_lon
	if step_lat <= 0 or step_lon <= 0:
		return [tuple(p) for p in polygon]
	# Сетка по широте (ряды), по долготе (столбцы)
	rows = []
	lat = lat_min
	while lat <= lat_max:
		row = []
		lon = lon_min
		while lon <= lon_max:
			pt = (lat, lon)
			if _point_in_polygon(pt, polygon):
				row.append(pt)
			lon += step_lon
		if row:
			rows.append(row)
		lat += step_lat
	# Змейка: чётные ряды по возрастанию lon, нечётные — по убыванию
	out = []
	for i, row in enumerate(rows):
		if i % 2 == 1:
			row = list(reversed(row))
		out.extend(row)
	return out


def _polygon_bbox_dims_m(polygon: List[Tuple[float, float]]) -> Tuple[float, float]:
	if not polygon:
		return (0.0, 0.0)
	lats = [p[0] for p in polygon]
	lons = [p[1] for p in polygon]
	lat_mid = (min(lats) + max(lats)) / 2.0
	h_m = abs(max(lats) - min(lats)) * 111_000.0
	w_m = abs(max(lons) - min(lons)) * 111_000.0 * max(0.01, math.cos(math.radians(lat_mid)))
	return (w_m, h_m)


def _operator_area_grid_step_m(polygon: List[Tuple[float, float]]) -> float:
	"""Адаптивный шаг змейки: целимся в 20..40 м в зависимости от размера зоны."""
	w_m, h_m = _polygon_bbox_dims_m(polygon)
	min_dim = max(1.0, min(w_m, h_m))
	adaptive = min_dim / 8.0
	return float(max(20.0, min(40.0, adaptive)))


def _operator_area_waypoints(order: Dict[str, Any]) -> List[Tuple[float, float]]:
	"""Точки облёта области: облёт внутри выделенной зоны (сетка лаунмower), не только контур."""
	# Для continuation-задач используем уже оставшиеся внутренние точки,
	# чтобы не генерировать всю сетку заново и не терять прогресс.
	if order.get("remaining_waypoints"):
		return [
			tuple(p) for p in (order.get("remaining_waypoints") or [])
			if isinstance(p, (list, tuple)) and len(p) == 2
		]
	if order.get("rest_waypoints"):
		return [
			tuple(p) for p in (order.get("rest_waypoints") or [])
			if isinstance(p, (list, tuple)) and len(p) == 2
		]
	wp = []
	polygon = []
	if order.get("area_polygon"):
		polygon = [tuple(p) for p in order["area_polygon"] if isinstance(p, (list, tuple)) and len(p) == 2]
	if not polygon and order.get("start") and order.get("end"):
		s, e = order["start"], order["end"]
		lat_min, lat_max = min(s[0], e[0]), max(s[0], e[0])
		lon_min, lon_max = min(s[1], e[1]), max(s[1], e[1])
		polygon = [(lat_min, lon_min), (lat_max, lon_min), (lat_max, lon_max), (lat_min, lon_max)]
	if len(polygon) >= 3:
		step_m = _operator_area_grid_step_m(polygon)
		wp = _lawnmower_waypoints_inside_polygon(polygon, step_m)
	if not wp:
		# Fallback: только вершины контура (как раньше)
		wp = list(polygon) if polygon else []
	return wp


def _operator_area_root_order_id(order: Dict[str, Any]) -> str:
	return str(order.get("root_order_id") or order.get("id") or "")


def _segment_intersects_no_fly_zone(a: Tuple[float, float], b: Tuple[float, float], samples: int = 12) -> bool:
	"""Проверка прямого сегмента на пересечение no-fly зон (дискретная аппроксимация)."""
	if is_point_in_any_zone(a) or is_point_in_any_zone(b):
		return True
	for i in range(1, max(2, samples)):
		t = i / float(samples)
		p = (a[0] + (b[0] - a[0]) * t, a[1] + (b[1] - a[1]) * t)
		if is_point_in_any_zone(p):
			return True
	return False


def _zone_detour_margin_deg(zone: Dict[str, Any], margin_m: float = 45.0) -> Tuple[float, float]:
	try:
		lat_mid = (float(zone["lat_min"]) + float(zone["lat_max"])) * 0.5
	except Exception:
		lat_mid = 0.0
	lat_margin = float(margin_m) / 111_000.0
	lon_margin = float(margin_m) / (111_000.0 * max(0.05, math.cos(math.radians(lat_mid))))
	return lat_margin, lon_margin


def _inflated_zone_corners(zone: Dict[str, Any], margin_m: float = 45.0) -> List[Tuple[float, float]]:
	r = _rect_from_zone(zone)
	if not r:
		return []
	lat_a, lat_b, lon_a, lon_b = r
	lat_min, lat_max = min(lat_a, lat_b), max(lat_a, lat_b)
	lon_min, lon_max = min(lon_a, lon_b), max(lon_a, lon_b)
	dlat, dlon = _zone_detour_margin_deg(zone, margin_m=margin_m)
	# Clockwise: NW, NE, SE, SW. Points are outside the real no-fly rectangle.
	return [
		(lat_max + dlat, lon_min - dlon),
		(lat_max + dlat, lon_max + dlon),
		(lat_min - dlat, lon_max + dlon),
		(lat_min - dlat, lon_min - dlon),
	]


def _point_in_zone(point: Tuple[float, float], zone: Dict[str, Any]) -> bool:
	r = _rect_from_zone(zone)
	if not r:
		return False
	lat_a, lat_b, lon_a, lon_b = r
	lat_min, lat_max = min(lat_a, lat_b), max(lat_a, lat_b)
	lon_min, lon_max = min(lon_a, lon_b), max(lon_a, lon_b)
	lat, lon = float(point[0]), float(point[1])
	return lat_min <= lat <= lat_max and lon_min <= lon <= lon_max


def _segment_intersects_zone(
	a: Tuple[float, float],
	b: Tuple[float, float],
	zone: Dict[str, Any],
	samples: int = 18,
) -> bool:
	if _point_in_zone(a, zone) or _point_in_zone(b, zone):
		return True
	for i in range(1, max(2, int(samples))):
		t = i / float(samples)
		p = (a[0] + (b[0] - a[0]) * t, a[1] + (b[1] - a[1]) * t)
		if _point_in_zone(p, zone):
			return True
	return False


def _first_zone_intersecting_segment(a: Tuple[float, float], b: Tuple[float, float]) -> Optional[Dict[str, Any]]:
	for z in STATE.get("no_fly_zones") or []:
		if not isinstance(z, dict):
			continue
		if _segment_intersects_zone(a, b, z, samples=18):
			return z
	return None


def _path_crosses_no_fly(points: List[Tuple[float, float]]) -> bool:
	if not points or len(points) < 2:
		return False
	for i in range(len(points) - 1):
		if _segment_intersects_no_fly_zone(tuple(points[i]), tuple(points[i + 1]), samples=18):
			return True
	return False


def _detour_segment_around_zone(
	a: Tuple[float, float],
	b: Tuple[float, float],
	zone: Dict[str, Any],
) -> Optional[List[Tuple[float, float]]]:
	corners = _inflated_zone_corners(zone)
	if len(corners) != 4:
		return None
	candidates: List[List[Tuple[float, float]]] = []
	# Try one-corner shortcuts first, then arcs along the inflated rectangle.
	for c in corners:
		candidates.append([tuple(a), c, tuple(b)])
	for i in range(4):
		for j in range(4):
			if i == j:
				continue
			if (i + 1) % 4 == j or (j + 1) % 4 == i:
				candidates.append([tuple(a), corners[i], corners[j], tuple(b)])
			else:
				cw = [tuple(a), corners[i], corners[(i + 1) % 4], corners[j], tuple(b)]
				ccw = [tuple(a), corners[i], corners[(i - 1) % 4], corners[j], tuple(b)]
				candidates.extend([cw, ccw])
	valid = [p for p in candidates if not _path_crosses_no_fly(p)]
	if not valid:
		return None
	valid.sort(key=_route_length)
	return valid[0]


def _avoid_no_fly_in_coords(coords: List[Tuple[float, float]], max_passes: int = 4) -> List[Tuple[float, float]]:
	"""Insert short off-network detour points around rectangular no-fly zones."""
	out = _to_coord_list(coords)
	if len(out) < 2 or not (STATE.get("no_fly_zones") or []):
		return out
	for _ in range(max(1, int(max_passes))):
		changed = False
		next_coords: List[Tuple[float, float]] = [out[0]]
		for i in range(len(out) - 1):
			a = tuple(out[i])
			b = tuple(out[i + 1])
			if _segment_intersects_no_fly_zone(a, b, samples=18):
				z = _first_zone_intersecting_segment(a, b)
				detour = _detour_segment_around_zone(a, b, z) if z else None
				if detour and len(detour) > 2:
					for p in detour[1:]:
						if next_coords[-1] != p:
							next_coords.append(p)
					changed = True
					continue
			if next_coords[-1] != b:
				next_coords.append(b)
		out = next_coords
		if not changed:
			break
	return out


def _free_flight_path_between(
	start: Tuple[float, float],
	end: Tuple[float, float],
	max_range_m: Optional[float] = None,
) -> Tuple[List[Tuple[float, float]], float]:
	"""Direct/free-flight path with no-fly detours, without snapping to infrastructure."""
	coords = _avoid_no_fly_in_coords([tuple(start), tuple(end)])
	if not coords or len(coords) < 2:
		return [], 0.0
	if _path_crosses_no_fly(coords):
		return [], 0.0
	length = float(_route_length(coords))
	if max_range_m is not None and length > float(max_range_m):
		return [], 0.0
	return coords, length


def _apply_no_fly_detours_to_result(result: Dict[str, Any], drone_type: str) -> Dict[str, Any]:
	if not result or not result.get("ok") or not (STATE.get("no_fly_zones") or []):
		return result
	out = dict(result)
	legs = out.get("segments")
	if isinstance(legs, list) and legs:
		new_legs = []
		full: List[Tuple[float, float]] = []
		pickup_count = out.get("pickup_waypoint_count")
		for leg in legs:
			leg2 = dict(leg or {})
			coords = _to_coord_list(leg2.get("coords") or [])
			if coords:
				detoured = _avoid_no_fly_in_coords(coords)
				leg2["coords"] = detoured
				leg2["length"] = float(_route_length(detoured))
				full = _concat_coords(full, detoured)
				if leg2.get("type") == "to_pickup":
					pickup_count = len(full)
			new_legs.append(leg2)
		if len(full) > 1:
			out["segments"] = new_legs
			out["coords"] = full
			out["route_length"] = float(sum(float((l or {}).get("length") or 0.0) for l in new_legs))
			out["route_segments"] = _route_segments_from_coords(full, mode=MODE_EMPTY, drone_type=drone_type)
			if pickup_count is not None:
				out["pickup_waypoint_count"] = int(pickup_count)
			return out

	coords = _to_coord_list(out.get("coords") or [])
	if len(coords) > 1:
		pickup_count = out.get("pickup_waypoint_count")
		if isinstance(pickup_count, int) and 1 < pickup_count < len(coords):
			to_pickup = _avoid_no_fly_in_coords(coords[:pickup_count])
			after_pickup = _avoid_no_fly_in_coords([coords[pickup_count - 1]] + coords[pickup_count:])
			full = _concat_coords(to_pickup, after_pickup)
			out["pickup_waypoint_count"] = len(to_pickup)
		else:
			full = _avoid_no_fly_in_coords(coords)
		out["coords"] = full
		out["route_length"] = float(_route_length(full))
		out["route_segments"] = _route_segments_from_coords(full, mode=MODE_EMPTY, drone_type=drone_type)
	return out


def _remaining_route_intersects_no_fly_zone(route: List[Tuple[float, float]], start_idx: int) -> bool:
	if not route:
		return False
	idx = max(0, int(start_idx))
	for i in range(idx, len(route) - 1):
		try:
			if _segment_intersects_no_fly_zone(tuple(route[i]), tuple(route[i + 1])):
				return True
		except Exception:
			continue
	return False


def _build_operator_area_flight_path(start_pos: Tuple[float, float], polygon: List[Tuple[float, float]], waypoints: List[Tuple[float, float]]) -> List[Tuple[float, float]]:
	"""
	Маршрут осмотра operator-area в координатах полёта (без road graph):
	точки только внутри полигона; сегменты, пересекающие no-fly, пропускаются локально.
	"""
	if not waypoints:
		return []
	waypoints = [tuple(w) for w in waypoints if isinstance(w, (list, tuple)) and len(w) == 2]
	if len(waypoints) > 1:
		try:
			if haversine_m(tuple(start_pos), tuple(waypoints[-1])) < haversine_m(tuple(start_pos), tuple(waypoints[0])):
				waypoints = list(reversed(waypoints))
		except Exception:
			pass
	coords: List[Tuple[float, float]] = []
	prev = tuple(start_pos)
	for wp in waypoints:
		wp = tuple(wp)
		if polygon and not _point_in_polygon(wp, polygon):
			continue
		if _segment_intersects_no_fly_zone(prev, wp):
			# Локально пропускаем недопустимую точку, не переводя всю миссию на дорожный граф.
			continue
		if not coords or coords[-1] != wp:
			coords.append(wp)
			prev = wp
	return coords


def _can_operator_continue_with_reserve(battery_pct: float, segment_m: float, drone_type: str = "operator") -> bool:
	"""Проверка, что после следующего сегмента у дрона останется минимум 20% батареи."""
	after = _routing_service.compute_battery_after(segment_m, battery_pct, MODE_EMPTY, drone_type)
	return after >= 20.0


def _split_operator_area_by_battery(start_pos: Tuple[float, float], battery_pct: float, area_path: List[Tuple[float, float]]) -> Tuple[List[Tuple[float, float]], List[Tuple[float, float]], float]:
	"""
	Делит area-path на доступную сейчас часть и остаток по правилу резерва 20%.
	Возвращает (current_part, remaining_part, battery_after_current_part).
	"""
	current: List[Tuple[float, float]] = []
	if not area_path:
		return (current, [], battery_pct)
	prev = tuple(start_pos)
	bat = float(battery_pct)
	for i, wp in enumerate(area_path):
		dist_m = haversine_m(prev, tuple(wp))
		if not _can_operator_continue_with_reserve(bat, dist_m, "operator"):
			return (current, [tuple(x) for x in area_path[i:]], bat)
		bat = _routing_service.compute_battery_after(dist_m, bat, MODE_EMPTY, "operator")
		current.append(tuple(wp))
		prev = tuple(wp)
	return (current, [], bat)


def _build_operator_area_route(
	drone_pos: Tuple[float, float],
	battery_pct: float,
	polygon: List[Tuple[float, float]],
	coverage_waypoints: List[Tuple[float, float]],
	drone_type: str = "operator",
) -> Dict[str, Any]:
	"""
	Смешанный маршрут operator-area:
	A) approach по graph до входа в зону,
	B) coverage free-flight внутри полигона,
	C) exit по graph до зарядки.
	"""
	if not coverage_waypoints:
		return {"ok": False, "reason": "NO_PATH", "details": "empty coverage"}
	area_path = _build_operator_area_flight_path(drone_pos, polygon, coverage_waypoints)
	if not area_path:
		return {"ok": False, "reason": "NO_PATH", "details": "no valid coverage path"}

	entry = tuple(area_path[0])
	_, approach_coords_raw, approach_len = plan_route_for(tuple(drone_pos), entry, drone_type, battery_pct, reserve_pct=PLAN_RESERVE_PCT)
	approach_coords = _to_coord_list(approach_coords_raw or [tuple(drone_pos)])
	max_approach_m = _routing_service.max_reachable_distance(battery_pct, MODE_EMPTY, drone_type, reserve_pct=PLAN_RESERVE_PCT)
	free_approach_coords, free_approach_len = _free_flight_path_between(tuple(drone_pos), entry, max_approach_m)
	if free_approach_coords and (
		not approach_coords
		or len(approach_coords) < 2
		or free_approach_len <= float(approach_len or 0.0) * FREE_FLIGHT_PREFER_RATIO
	):
		approach_coords = free_approach_coords
		approach_len = free_approach_len
	if not approach_coords:
		approach_coords = [tuple(drone_pos)]
	approach_last = tuple(approach_coords[-1])
	approach_bat = _routing_service.compute_battery_after(approach_len, battery_pct, MODE_EMPTY, drone_type)

	current_cov, remaining_cov, bat_after_cov = _split_operator_area_by_battery(approach_last, approach_bat, area_path)
	if not current_cov:
		return {
			"ok": True,
			"approach_coords": approach_coords,
			"coverage_coords": [],
			"exit_coords": [],
			"combined_coords": approach_coords,
			"approach_length": float(approach_len),
			"coverage_length": 0.0,
			"exit_length": 0.0,
			"remaining_waypoints": area_path,
			"handover_point": approach_last,
			"coverage_start_idx": len(approach_coords),
			"coverage_end_idx": len(approach_coords),
		}

	coverage_len = _route_length([approach_last] + current_cov)
	last_cov = tuple(current_cov[-1])
	exit_coords, exit_len = _best_escape_graph_path(last_cov, bat_after_cov, drone_type)
	combined = _concat_coords(approach_coords, current_cov)
	combined = _concat_coords(combined, exit_coords)
	coverage_start_idx = max(0, len(approach_coords) - 1)
	coverage_end_idx = coverage_start_idx + len(current_cov)
	return {
		"ok": True,
		"approach_coords": approach_coords,
		"coverage_coords": current_cov,
		"exit_coords": exit_coords,
		"combined_coords": combined,
		"approach_length": float(approach_len),
		"coverage_length": float(coverage_len),
		"exit_length": float(exit_len),
		"remaining_waypoints": remaining_cov,
		"handover_point": last_cov,
		"coverage_start_idx": coverage_start_idx,
		"coverage_end_idx": coverage_end_idx,
	}


def _advance_area_order_progress(order: Dict[str, Any], flown_points: List[Tuple[float, float]]) -> None:
	"""Обновляет прогресс area-миссии: completed/remaining для текущего заказа."""
	remaining = [tuple(w) for w in (order.get("remaining_waypoints") or _operator_area_waypoints(order))]
	if not remaining:
		return
	n = min(len(flown_points), len(remaining))
	order["completed_waypoints_count"] = int(order.get("completed_waypoints_count", 0)) + n
	order["remaining_waypoints"] = [tuple(w) for w in remaining[n:]]


def _to_coord_list(coords: List[Any]) -> List[Tuple[float, float]]:
	out: List[Tuple[float, float]] = []
	for p in coords or []:
		if isinstance(p, (list, tuple)) and len(p) == 2:
			out.append((float(p[0]), float(p[1])))
	return out


def _best_escape_graph_path(start: Tuple[float, float], battery_pct: float, drone_type: str = "operator") -> Tuple[List[Tuple[float, float]], float]:
	"""Выход до ближайшей зарядки: free-flight can beat graph if it is clearly shorter."""
	chargers: List[Tuple[float, float]] = []
	if STATE.get("base"):
		chargers.append(tuple(STATE["base"]))
	chargers += [tuple(s) for s in STATE.get("stations", []) if isinstance(s, (list, tuple)) and len(s) == 2]
	best_coords: List[Tuple[float, float]] = []
	best_len = float("inf")
	max_escape_m = _routing_service.max_reachable_distance(battery_pct, MODE_EMPTY, drone_type, reserve_pct=PLAN_RESERVE_PCT)
	for c in chargers:
		_, coords, length = plan_route_for(tuple(start), tuple(c), drone_type, battery_pct, reserve_pct=PLAN_RESERVE_PCT)
		if coords and length < best_len:
			best_len = float(length)
			best_coords = _to_coord_list(coords)
		free_coords, free_len = _free_flight_path_between(tuple(start), tuple(c), max_escape_m)
		if free_coords and (not best_coords or free_len <= best_len * FREE_FLIGHT_PREFER_RATIO):
			best_len = float(free_len)
			best_coords = free_coords
	if best_coords:
		return (best_coords, best_len)
	return ([], 0.0)


def _concat_coords(a: List[Tuple[float, float]], b: List[Tuple[float, float]]) -> List[Tuple[float, float]]:
	if not a:
		return list(b or [])
	if not b:
		return list(a or [])
	if a[-1] == b[0]:
		return list(a) + list(b[1:])
	return list(a) + list(b)


def _get_active_order_for_drone(drone_id: str) -> Optional[Dict[str, Any]]:
	for o in STATE.get("orders", []):
		if o.get("drone_id") == drone_id and o.get("status") in ("assigned", "in_progress", "waiting_continuation"):
			return o
	return None


def _remove_drone_from_charge_queues(drone_id: str) -> None:
	"""Гарантированно убирает drone_id из всех списков очередей зарядки (база и станции)."""
	def _purge_list(lst: Any) -> Any:
		"""
		Remove drone_id from list-like structures.
		Supports: [drone_id, ...], [{"drone_id":...}, ...], [{"id":...}, ...], [(drone_id, ...), ...]
		Leaves non-list values intact.
		"""
		if not isinstance(lst, list):
			return lst
		out = []
		for item in lst:
			try:
				if item == drone_id:
					continue
				if isinstance(item, dict) and (item.get("drone_id") == drone_id or item.get("id") == drone_id):
					continue
				if isinstance(item, (list, tuple)) and item and item[0] == drone_id:
					continue
			except Exception:
				pass
			out.append(item)
		return out

	bq = STATE.setdefault("base_queue", {"charging": [], "queue": [], "capacity": 2})
	for key in ("charging", "queue", "charging_queue"):
		bq[key] = _purge_list(bq.get(key) or [])
	STATE["base_queue"] = bq
	sqs = STATE.setdefault("station_queues", {})
	for _key, sq in list(sqs.items()):
		if not isinstance(sq, dict):
			continue
		for key2 in ("charging", "queue", "charging_queue"):
			sq[key2] = _purge_list(sq.get(key2) or [])
		sqs[_key] = sq
	STATE["station_queues"] = sqs


def _try_close_order_if_no_flight_after_charge(drone_id: str, drone: Dict[str, Any]) -> bool:
	"""
	После зарядки replan мог не построить маршрут; для operator area без оставшихся точек
	закрываем заказ, чтобы не зависать in_progress.
	"""
	order = _get_active_order_for_drone(drone_id)
	if not order:
		return False
	required_type = order.get("drone_type") or map_order_to_drone_type(order.get("type", "delivery"))
	if required_type != "operator" or not order.get("area_polygon"):
		return False
	if order.get("remaining_waypoints"):
		return False
	oid = order.get("id")
	# Order завершается после зарядки / передаётся в continuation — см. mark_order_completed_if_any.
	mark_order_completed_if_any(drone_id)
	o2 = _get_active_order_for_drone(drone_id)
	return o2 is None or o2.get("id") != oid


def _force_exit_charging_if_complete(drone_id: str, drone: Dict[str, Any]) -> None:
	"""
	Исправление инконсистентности: battery >= CHARGE_COMPLETE_PCT, status == charging.
	Дрон выходит из charging, покидает очереди; далее resume миссии или idle + завершение заказа.
	"""
	if drone.get("status") != "charging":
		return
	if float(drone.get("battery", 0.0)) < CHARGE_COMPLETE_PCT:
		return
	_remove_drone_from_charge_queues(drone_id)
	drone["battery"] = 100.0
	_resume_after_charge_or_hold(drone_id, drone)
	if drone.get("status") == "charging":
		rt = drone.get("route") or []
		ti = int(drone.get("target_idx", 0))
		if rt and ti < len(rt):
			drone["status"] = "enroute"
		else:
			mark_order_completed_if_any(drone_id)
			drone["status"] = "idle"


def _mark_order_in_progress_if_started(drone_id: str, drone: Dict[str, Any]) -> None:
	"""Переводим заказ в in_progress, когда дрон реально движется по маршруту миссии."""
	order = _get_active_order_for_drone(drone_id)
	if not order:
		return
	route = drone.get("route") or []
	if not route:
		return
	if int(drone.get("target_idx", 0)) >= len(route):
		return
	if drone.get("status") in ("enroute", "return_charge", "return_base"):
		if order.get("status") == "assigned":
			order["status"] = "in_progress"


def _sanitize_active_drone_state(drone_id: str, drone: Dict[str, Any]) -> None:
	"""
	Инвариант state-machine:
	если есть active_order и непустой route, дрон не должен оставаться idle/holding.
	"""
	order = _get_active_order_for_drone(drone_id)
	if drone.get("active_order_id") and not order:
		try:
			_remove_drone_from_charge_queues(drone_id)
		except Exception:
			pass
		for key in (
			"active_order_id",
			"saved_order_id",
			"saved_target_idx",
			"loaded_after_waypoint_count",
			"waypoints_completed",
			"force_charge_after_route",
		):
			drone.pop(key, None)
		drone["route"] = []
		drone["route_segments"] = []
		drone["resume_route"] = []
		drone["resume_route_segments"] = []
		drone["target_idx"] = 0
		if drone.get("status") != "charging":
			drone["status"] = "idle"
		return
	route = drone.get("route") or []
	idx = int(drone.get("target_idx", 0))
	if order and route and idx < len(route) and drone.get("status") in ("idle", "holding"):
		drone["status"] = "enroute"
	# Дрон в holding без маршрута после неудачного replan — пробуем восстановить миссию или закрыть заказ.
	if drone.get("status") == "holding" and order and (not route or idx >= len(route)):
		if _restore_route_after_charging(drone):
			pass
		elif _try_close_order_if_no_flight_after_charge(drone_id, drone):
			pass
	# Защита: battery >= порога завершения зарядки, но статус всё ещё charging — выходим из зависшего состояния.
	_force_exit_charging_if_complete(drone_id, drone)


def _find_existing_continuation(parent_order: Dict[str, Any], remaining_waypoints: List[Tuple[float, float]]) -> Optional[Dict[str, Any]]:
	"""Защита от дублей continuation: ищем уже существующий order с тем же mission/остатком."""
	mission_id = str(parent_order.get("mission_id") or _operator_area_root_order_id(parent_order))
	fingerprint = f"{mission_id}:{len(remaining_waypoints)}:{tuple(remaining_waypoints[:1] or [])}:{tuple(remaining_waypoints[-1:] or [])}"
	for o in STATE.get("orders", []):
		if o.get("status") not in ("queued", "assigned", "in_progress", "waiting_continuation"):
			continue
		if str(o.get("continuation_fingerprint") or "") == fingerprint:
			return o
	return None


def _new_operator_area_continuation(order: Dict[str, Any], handover_point: Tuple[float, float], remaining_waypoints: List[Tuple[float, float]]) -> Dict[str, Any]:
	root_id = _operator_area_root_order_id(order)
	history = list(order.get("handover_history") or [])
	history.append(tuple(handover_point))
	cont_idx = int(order.get("continuation_index", 0)) + 1
	mission_id = str(order.get("mission_id") or root_id)
	fingerprint = f"{mission_id}:{len(remaining_waypoints)}:{tuple(remaining_waypoints[:1] or [])}:{tuple(remaining_waypoints[-1:] or [])}"
	return {
		"id": f"ord_cont_{root_id}_{cont_idx}_{len(STATE.get('orders', []))}",
		"type": order.get("type", "shooting"),
		"drone_type": "operator",
		"priority": order.get("priority", 5),
		"start": tuple(handover_point),
		"end": tuple(remaining_waypoints[-1]) if remaining_waypoints else tuple(handover_point),
		"battery_level": 100.0,
		"status": "queued",
		"area_polygon": order.get("area_polygon"),
		"mission_mode": "operator_area",
		"remaining_waypoints": [tuple(w) for w in remaining_waypoints],
		"completed_waypoints_count": int(order.get("completed_waypoints_count", 0)),
		"handover_history": history,
		"handover_point": tuple(handover_point),
		"root_order_id": root_id,
		"continuation_index": cont_idx,
		"is_area_continuation": True,
		"parent_order_id": order.get("id"),
		"continuation_of": order.get("id"),
		"mission_id": mission_id,
		"continuation_fingerprint": fingerprint,
		"continuation_spawned": False,
	}


def plan_operator_area_trip(
	drone: Dict[str, Any], order: Dict[str, Any]
) -> Tuple[Dict[str, Any], Optional[Dict[str, Any]]]:
	"""
	Планирование облёта области операторским дроном. Если на одном аккумуляторе не облететь,
	возвращает маршрут до точки «передачи» и заказ-продолжение для второго дрона.
	Возвращает (result, continuation_order). result как у plan_order_trip; continuation_order или None.
	"""
	city = STATE.get("city")
	if not city:
		return ({"ok": False, "reason": "no city", "details": "no selected city"}, None)
	_routing_service.set_battery_mode(STATE.get("battery_mode", "reality"))
	waypoints = _operator_area_waypoints(order)
	if not waypoints:
		return ({"ok": False, "reason": "no area", "details": "no area_polygon or bounds"}, None)
	battery_pct = float(drone.get("battery", 100.0))
	pos = tuple(drone.get("pos", order.get("start", (0, 0))))
	polygon = [tuple(p) for p in (order.get("area_polygon") or []) if isinstance(p, (list, tuple)) and len(p) == 2]
	continuation_order = None
	route_info = _build_operator_area_route(pos, battery_pct, polygon, waypoints, "operator")
	if not route_info.get("ok"):
		return ({"ok": False, "reason": route_info.get("reason", "NO_PATH"), "details": route_info.get("details", "area route failed")}, None)
	coverage_coords = _to_coord_list(route_info.get("coverage_coords") or [])
	remaining_part = _to_coord_list(route_info.get("remaining_waypoints") or [])
	if not coverage_coords and remaining_part:
		# Даже coverage не начат: оставляем continuation в ожидании другого оператора.
		existing = _find_existing_continuation(order, remaining_part)
		continuation_order = existing or _new_operator_area_continuation(order, tuple(route_info.get("handover_point") or pos), remaining_part)
		return ({
			"ok": True,
			"coords": _to_coord_list(route_info.get("approach_coords") or [pos]),
			"route_length": float(route_info.get("approach_length", 0.0)),
			"pickup_waypoint_count": 0,
			"area_completed_waypoints_count": 0,
			"area_total_waypoints_count": len(_to_coord_list(_build_operator_area_flight_path(pos, polygon, waypoints))),
			"chargers_used": [],
			"battery_plan": [{"at": "start", "battery": battery_pct}],
			"segments": [{"type": "approach_graph", "coords": _to_coord_list(route_info.get("approach_coords") or []), "length": float(route_info.get("approach_length", 0.0)), "chargers": []}],
			"mission_mode": "operator_area",
			"force_charge_after_route": True,
			"coverage_start_idx": int(route_info.get("coverage_start_idx", 0)),
			"coverage_end_idx": int(route_info.get("coverage_end_idx", 0)),
		}, continuation_order)

	coords = _to_coord_list(route_info.get("combined_coords") or [])
	length = float(route_info.get("approach_length", 0.0)) + float(route_info.get("coverage_length", 0.0)) + float(route_info.get("exit_length", 0.0))
	if remaining_part:
		# Передача operator area-задачи: фиксируем handover и оставшиеся внутренние точки.
		handover = tuple(route_info.get("handover_point") or coverage_coords[-1])
		existing = _find_existing_continuation(order, remaining_part)
		continuation_order = existing or _new_operator_area_continuation(order, handover, remaining_part)
	result = {
		"ok": True,
		"coords": coords,
		"route_length": length,
		"pickup_waypoint_count": len(coords),
		"area_completed_waypoints_count": len(coverage_coords),
		"area_total_waypoints_count": len(coverage_coords) + len(remaining_part),
		"chargers_used": [],
		"battery_plan": [{"at": "start", "battery": battery_pct}],
		# Комбинированный маршрут: до зоны по graph, внутри coverage змейкой, далее выход по graph.
		"segments": [
			{"type": "approach_graph", "coords": _to_coord_list(route_info.get("approach_coords") or []), "length": float(route_info.get("approach_length", 0.0)), "chargers": []},
			{"type": "coverage_area", "coords": coverage_coords, "length": float(route_info.get("coverage_length", 0.0)), "chargers": []},
			{"type": "exit_graph", "coords": _to_coord_list(route_info.get("exit_coords") or []), "length": float(route_info.get("exit_length", 0.0)), "chargers": []},
		],
		"mission_mode": "operator_area",
		"force_charge_after_route": bool(continuation_order),
		"coverage_start_idx": int(route_info.get("coverage_start_idx", 0)),
		"coverage_end_idx": int(route_info.get("coverage_end_idx", 0)),
	}
	return (result, continuation_order)


def plan_operator_point_trip(
	drone_pos: Tuple[float, float],
	point: Tuple[float, float],
	drone_type: str,
	battery_pct: float,
) -> Dict[str, Any]:
	"""
	Маршрут операторского/сервисного дрона до точки: дрон → точка → зарядка.
	Оба сегмента в режиме MODE_EMPTY, с возможными остановками на зарядку.
	"""
	G = get_active_routing_graph()
	city = STATE.get("city")
	if not city or G is None:
		return {"ok": False, "reason": "no graph", "details": "no city graph"}
	_routing_service.set_battery_mode(STATE.get("battery_mode", "reality"))
	_routing_service.city_graphs[city] = G
	charger_nodes = limited_charger_nodes(drone_pos)
	plan_reserve = PLAN_RESERVE_PCT_TEST if STATE.get("battery_mode") == "test" else PLAN_RESERVE_PCT
	start_node = _routing_service._find_nearest_node(G, drone_pos)
	point_node = _routing_service._find_nearest_node(G, point)
	if not start_node or not point_node:
		return {"ok": False, "reason": NO_PATH_TO_PICKUP, "details": "nearest node not found"}
	# Сегмент 1: дрон → точка (с зарядками по пути)
	path_a, coords_a, len_a, chargers_a = _routing_service.plan_with_chargers(
		G, start_node, point_node, battery_pct, MODE_EMPTY, drone_type, reserve_pct=plan_reserve, charger_nodes=charger_nodes,
		max_segment_battery_pct=MAX_SEGMENT_BATTERY_PCT, max_battery_pct_to_reach_charger=MAX_BATTERY_PCT_TO_REACH_CHARGER,
	)
	if not path_a or not coords_a:
		return {"ok": False, "reason": NO_FEASIBLE_CHARGING_CHAIN_TO_PICKUP, "details": "no path to point"}
	battery_after_a = _routing_service.compute_battery_after(len_a, battery_pct, MODE_EMPTY, drone_type)
	if chargers_a:
		battery_after_a = 100.0
	# Сегмент 2: точка → зарядка (escape)
	base_node = charger_nodes.get("base")
	station_nodes = charger_nodes.get("stations") or []
	path_c, coords_c, len_c, chargers_c = None, None, 0.0, []
	best_c = None
	best_len_c = float("inf")
	for name, goal_n in [("base", base_node)] + [(f"station_{i}", sn) for i, sn in enumerate(station_nodes) if sn]:
		if goal_n is None or goal_n not in G.nodes:
			continue
		p_c, co_c, l_c, ch_c = _routing_service.plan_with_chargers(
			G, point_node, goal_n, battery_after_a, MODE_EMPTY, drone_type, reserve_pct=plan_reserve, charger_nodes=charger_nodes,
			max_segment_battery_pct=MAX_SEGMENT_BATTERY_PCT, max_battery_pct_to_reach_charger=MAX_BATTERY_PCT_TO_REACH_CHARGER,
		)
		if p_c and co_c and l_c < best_len_c:
			best_len_c = l_c
			best_c = (p_c, co_c, l_c, ch_c)
	if best_c:
		path_c, coords_c, len_c, chargers_c = best_c
	full_coords = list(coords_a)
	if coords_c:
		full_coords.extend(coords_c[1:])
	total_length = len_a + len_c
	chargers_used = list(dict.fromkeys(chargers_a + chargers_c))
	battery_after_escape = 100.0 if chargers_c else _routing_service.compute_battery_after(len_c, battery_after_a, MODE_EMPTY, drone_type)
	route_segments = _route_segments_from_coords(full_coords, mode="empty", drone_type=drone_type)
	return {
		"ok": True,
		"coords": full_coords,
		"route_segments": route_segments,
		"segments": [
			{"type": "to_point", "coords": coords_a, "length": len_a, "chargers": chargers_a},
			{"type": "escape", "coords": coords_c or [], "length": len_c, "chargers": chargers_c},
		],
		"chargers_used": chargers_used,
		"battery_plan": [
			{"at": "start", "battery": battery_pct},
			{"at": "after_point", "battery": battery_after_a},
			{"at": "after_escape", "battery": battery_after_escape},
		],
		"pickup_waypoint_count": len(coords_a),
		"route_length": total_length,
	}


def plan_operator_continuation_trip(drone: Dict[str, Any], order: Dict[str, Any]) -> Dict[str, Any]:
	"""Continuation операторской area-задачи: перелёт к точке передачи и дальнейшая area-фаза."""
	remaining = _operator_area_waypoints(order)
	if not remaining:
		return {"ok": False, "reason": "NO_PATH", "details": "no remaining waypoints"}
	handover = order.get("handover_point")
	if not handover:
		handover = remaining[0]
	handover = tuple(handover) if isinstance(handover, (list, tuple)) and len(handover) == 2 else remaining[0]
	if not remaining or tuple(remaining[0]) != tuple(handover):
		remaining = [tuple(handover)] + [tuple(w) for w in remaining]
	# Передаём в общий area-планировщик только хвост после handover.
	# Это обеспечивает многошаговую передачу без отдельной одноразовой логики.
	tmp_order = dict(order)
	tmp_order["remaining_waypoints"] = list(remaining)
	tmp_order["start"] = handover
	return plan_operator_area_trip(drone, tmp_order)[0]


async def rebuild_graph_with_zones():
	city = STATE.get("city")
	if not city:
		return
	try:
		city_data = _data_service.get_city_data(city)
		city_data['no_fly_zones'] = list(STATE["no_fly_zones"]) or city_data.get('no_fly_zones', [])
		drone_type = STATE.get("drone_type") or "cargo"
		city_graph = _graph_service.build_city_graph(city_data, drone_type)
		_routing_service.city_graphs[city] = city_graph
		STATE["city_graph"] = city_graph
		refresh_charger_nodes()
		await persist_state()
	except Exception:
		logger.exception("Failed to rebuild graph with zones")

async def assign_orders():
	# Очередь заказов: назначаем только на свободный дрон нужной категории; приоритет — по полю priority (больше = раньше).
	city = STATE.get("city")
	G = STATE.get("city_graph")
	if not city or G is None:
		return
	queue = [o for o in STATE["orders"] if o.get("status") == "queued"]
	if queue:
		logger.info("assign_orders start queued=%s", len(queue))
	else:
		logger.debug("assign_orders start queued=0")
	# Сортируем по приоритету (убывание), при равном — по порядку в списке (FIFO)
	order_indices = {o.get("id"): i for i, o in enumerate(STATE["orders"])}
	queue = sorted(queue, key=lambda o: (-o.get("priority", 0), order_indices.get(o.get("id"), 0)))
	for order in queue:
		required_type = order.get("drone_type") or map_order_to_drone_type(order.get("type", "delivery"))
		drone_id = pick_drone_for_order(order, required_type)
		if drone_id is None:
			order["last_error"] = "NO_VALID_DRONE"
			continue
		drone = STATE["drones"][drone_id]
		continuation = None
		# Грузовой: три фазы (забор → доставка → зарядка). Операторский/сервисный: точка или область.
		if required_type == "cargo":
			result = await asyncio.to_thread(
				plan_order_trip,
				drone["pos"],
				order["start"],
				order["end"],
				drone["type"],
				drone["battery"],
			)
		elif required_type == "operator" and (order.get("remaining_waypoints") or (order.get("handover_point") and order.get("rest_waypoints"))):
			tmp_order = dict(order)
			remaining = _operator_area_waypoints(order)
			handover = order.get("handover_point")
			if handover and (not remaining or tuple(remaining[0]) != tuple(handover)):
				tmp_order["remaining_waypoints"] = [tuple(handover)] + [tuple(w) for w in remaining]
			result, continuation = await asyncio.to_thread(plan_operator_area_trip, drone, tmp_order)
			if result and result.get("ok") and continuation and not any(o.get("id") == continuation.get("id") for o in STATE.get("orders", [])):
				# Многошаговая передача area-задачи: каждый этап может создать следующий continuation.
				STATE["orders"].append(continuation)
				order["continuation_spawned"] = True
				order["status"] = "waiting_continuation"
			logger.info("assign_orders: area continuation order %s -> drone %s", order.get("id"), drone_id)
		elif required_type == "operator" and order.get("area_polygon"):
			order["mission_mode"] = "operator_area"
			order["mission_id"] = str(order.get("mission_id") or order.get("id"))
			if not order.get("area_waypoints"):
				order["area_waypoints"] = [tuple(w) for w in _operator_area_waypoints(order)]
			if not order.get("remaining_waypoints"):
				order["remaining_waypoints"] = list(order.get("area_waypoints") or [])
			result, continuation = await asyncio.to_thread(plan_operator_area_trip, drone, order)
			if result and result.get("ok") and continuation and not any(o.get("id") == continuation.get("id") for o in STATE.get("orders", [])):
				STATE["orders"].append(continuation)
				order["continuation_spawned"] = True
				order["status"] = "waiting_continuation"
				logger.info("assign_orders: created continuation order %s (handover at %s), first drone %s", continuation.get("id"), continuation.get("handover_point"), drone_id)
			if not result or not result.get("ok"):
				result = result or {"ok": False, "reason": "NO_PATH", "details": "operator area plan failed"}
		else:
			# Операторская точка или сервисный: дрон → точка → зарядка (отдельное планирование)
			result = await asyncio.to_thread(
				plan_operator_point_trip,
				drone["pos"],
				order["end"],
				drone["type"],
				drone["battery"],
			)
		if not result.get("ok"):
			logger.info(
				"assign_orders: order %s not assigned to %s reason=%s details=%s",
				order.get("id"), drone_id, result.get("reason"), result.get("details")
			)
			continue
		result = _apply_no_fly_detours_to_result(result, str(drone.get("type") or required_type))
		full_coords = result.get("coords") or []
		segs = result.get("route_segments") or []
		route_len = float(result.get("route_length") or 0.0)
		valid_reason = "ok"
		if not full_coords or len(full_coords) <= 1:
			valid_reason = "invalid_waypoints"
		elif route_len <= 1.0:
			valid_reason = "invalid_route_len"
		elif haversine_m(tuple(order["start"]), tuple(order["end"])) > 30.0 and int(result.get("pickup_waypoint_count") or 0) <= 1:
			valid_reason = "invalid_pickup_waypoints"
		# Ensure segments exist for UI/telemetry
		if valid_reason == "ok" and (not isinstance(segs, list) or len(segs) == 0) and len(full_coords) >= 2:
			dt = str(drone.get("type") or "cargo")
			segs = _route_segments_from_coords([tuple(p) for p in full_coords], mode="empty", drone_type=dt)
			result["route_segments"] = segs
		if valid_reason != "ok":
			logger.info(
				"assign_orders route validation: order_id=%s drone_id=%s route_len=%.1f waypoints=%s segments=%s valid=false reason=%s",
				order.get("id"),
				drone_id,
				route_len,
				len(full_coords) if isinstance(full_coords, list) else 0,
				len(segs) if isinstance(segs, list) else 0,
				valid_reason,
			)
			order["status"] = "queued"
			order["last_plan_error"] = str(valid_reason)
			continue
		logger.info(
			"assign_orders route validation: order_id=%s drone_id=%s route_len=%.1f waypoints=%s segments=%s valid=true",
			order.get("id"),
			drone_id,
			route_len,
			len(full_coords) if isinstance(full_coords, list) else 0,
			len(segs) if isinstance(segs, list) else 0,
		)
		rd0 = result.get("route_diagnostics")
		if isinstance(rd0, dict) and rd0:
			_mode = "strict_layer" if rd0.get("strict_layer") else "infra_weighted"
			logger.info(
				"assign_orders: drone_type=%s trunk_ratio=%.3f local_ratio=%.3f off_network_ratio=%.3f mode=%s",
				str(drone.get("type") or required_type),
				float(rd0.get("trunk_ratio") or 0.0),
				float(rd0.get("local_ratio") or rd0.get("local_voronoi_ratio") or 0.0),
				float(rd0.get("off_network_ratio") or 0.0),
				_mode,
			)
		full_coords = result["coords"]
		apply_midroute_charging(drone, full_coords, full_segments=result.get("route_segments"))
		if result.get("route_diagnostics"):
			drone["route_diagnostics"] = dict(result["route_diagnostics"])
		else:
			drone.pop("route_diagnostics", None)
		chargers_used = result.get("chargers_used", [])
		pickup_wp = result.get("pickup_waypoint_count", len(full_coords))
		logger.info(
			"assign_orders: order %s -> %s, route_length=%.0fm, chargers_used=%s (%s), pickup_waypoints=%s",
			order.get("id"), drone_id, result["route_length"], chargers_used, len(chargers_used), pickup_wp
		)
		order["status"] = "assigned"
		order["drone_id"] = drone_id
		order["route_length"] = result["route_length"]
		order["pickup_waypoint_count"] = pickup_wp
		if required_type == "operator" and (order.get("area_polygon") or order.get("remaining_waypoints")):
			order["mission_mode"] = "operator_area"
			_advance_area_order_progress(order, [tuple(p) for p in (result.get("coords") or [])])
			total_wp = int(result.get("area_total_waypoints_count", 0)) or len(_operator_area_waypoints(order))
			order["total_waypoints_count"] = total_wp
			if not order.get("area_waypoints"):
				order["area_waypoints"] = [tuple(w) for w in _operator_area_waypoints(order)]
			if continuation:
				order["remaining_waypoints"] = list(continuation.get("remaining_waypoints") or [])
				order["handover_history"] = list(continuation.get("handover_history") or order.get("handover_history") or [])
				order["has_continuation"] = True
				order["root_order_id"] = _operator_area_root_order_id(order)
				order["continuation_spawned"] = True
			else:
				order["remaining_waypoints"] = []
				order["has_continuation"] = False
				order["continuation_spawned"] = False
		order["chargers_used"] = result.get("chargers_used", [])
		order["battery_plan"] = result.get("battery_plan", [])
		order["segments"] = result.get("segments", [])
		# Route segments for UI (do not embed into infrastructure geojson)
		drone["route_segments"] = list(drone.get("route_segments") or [])
		drone["loaded_after_waypoint_count"] = pickup_wp
		drone["waypoints_completed"] = 0
		drone["active_order_id"] = order.get("id")
		drone["mission_mode"] = result.get("mission_mode")
		drone["force_charge_after_route"] = bool(result.get("force_charge_after_route"))

def _estimate_speed_mps() -> float:
	"""Скорость дрона (м/с) для оценки ETA с учётом ветра."""
	wind = float(STATE.get("weather", {}).get("wind_mps", 3.0))
	return max(5.0, 15.0 - 0.3 * wind)


def _order_completion_time_seconds(result: Dict[str, Any]) -> float:
	"""Оценка времени выполнения заказа в секундах: полёт + остановки на зарядку."""
	if not result or not result.get("ok"):
		return float("inf")
	route_m = float(result.get("route_length", 0))
	# Reject "zero route" unless explicitly allowed
	if route_m <= 1.0:
		return float("inf")
	chargers = result.get("chargers_used") or []
	speed = _estimate_speed_mps()
	flight_sec = route_m / speed if speed > 0 else 0
	charge_sec = len(chargers) * CHARGE_TICKS_ESTIMATE_PER_STOP
	return flight_sec + charge_sec


def _run_plan_for_order(drone_or_base_pos: Tuple[float, float], battery_pct: float, order: Dict[str, Any], required_type: str) -> Optional[Dict[str, Any]]:
	"""Запускает планировщик для заказа с заданной позиции и заряда. Не изменяет STATE (кроме графа)."""
	if required_type == "cargo":
		return plan_order_trip(
			drone_or_base_pos, order["start"], order["end"], required_type, battery_pct
		)
	if required_type == "operator" and (order.get("remaining_waypoints") or (order.get("handover_point") and order.get("rest_waypoints"))):
		# continuation — нужен объект дрона с pos и battery
		drone = {"pos": drone_or_base_pos, "battery": battery_pct, "type": "operator"}
		return plan_operator_continuation_trip(drone, order)
	if required_type == "operator" and order.get("area_polygon"):
		drone = {"pos": drone_or_base_pos, "battery": battery_pct, "type": "operator"}
		result, _ = plan_operator_area_trip(drone, order)
		return result
	# operator point / cleaner
	point = order.get("end") or order.get("start")
	if not point:
		return None
	point = tuple(point) if isinstance(point, (list, tuple)) and len(point) == 2 else None
	if not point:
		return None
	return plan_operator_point_trip(drone_or_base_pos, point, required_type, battery_pct)


def estimate_order_completion_time_seconds(drone: Dict[str, Any], order: Dict[str, Any], required_type: str) -> Tuple[float, Optional[Dict[str, Any]]]:
	"""Оценка времени (сек) до завершения заказа этим дроном (с учётом зарядки). Возвращает (секунды, result или None)."""
	pos = drone.get("pos")
	if not pos:
		return (float("inf"), None)
	pos = tuple(pos) if isinstance(pos, (list, tuple)) and len(pos) == 2 else None
	if not pos:
		return (float("inf"), None)
	battery = float(drone.get("battery", 100.0))
	result = _run_plan_for_order(pos, battery, order, required_type)
	if not result or not result.get("ok"):
		return (float("inf"), None)
	result = _apply_no_fly_detours_to_result(result, required_type)
	return (_order_completion_time_seconds(result), result)


def estimate_new_drone_completion_time_seconds(order: Dict[str, Any], required_type: str) -> Tuple[float, Optional[Dict[str, Any]]]:
	"""Оценка времени (сек) до завершения заказа «новым» дроном с базы (100% заряд)."""
	base = STATE.get("base")
	if not base:
		return (float("inf"), None)
	base = tuple(base) if isinstance(base, (list, tuple)) and len(base) == 2 else None
	if not base:
		return (float("inf"), None)
	result = _run_plan_for_order(base, 100.0, order, required_type)
	if not result or not result.get("ok"):
		return (float("inf"), None)
	result = _apply_no_fly_detours_to_result(result, required_type)
	return (_order_completion_time_seconds(result), result)


def _operator_area_assignment_targets(order: Dict[str, Any]) -> List[Tuple[float, float]]:
	"""Points that represent where an operator area mission should be picked up."""
	targets: List[Tuple[float, float]] = []
	for key in ("handover_point",):
		p = order.get(key)
		if isinstance(p, (list, tuple)) and len(p) == 2:
			targets.append((float(p[0]), float(p[1])))
	for p in order.get("remaining_waypoints") or []:
		if isinstance(p, (list, tuple)) and len(p) == 2:
			targets.append((float(p[0]), float(p[1])))
	for p in order.get("area_waypoints") or []:
		if isinstance(p, (list, tuple)) and len(p) == 2:
			targets.append((float(p[0]), float(p[1])))
	for p in order.get("area_polygon") or []:
		if isinstance(p, (list, tuple)) and len(p) == 2:
			targets.append((float(p[0]), float(p[1])))
	if order.get("area_polygon") and not targets:
		for p in _operator_area_waypoints(order):
			targets.append((float(p[0]), float(p[1])))
	if targets:
		try:
			targets.append(_centroid(targets))
		except Exception:
			pass
	return targets


def _assignment_distance_to_order(
	order: Dict[str, Any],
	pos: Tuple[float, float],
	required_type: str,
) -> Tuple[float, Tuple[float, float]]:
	"""Distance used only for selecting the nearest suitable drone."""
	targets: List[Tuple[float, float]] = []
	if required_type == "operator" and (order.get("area_polygon") or order.get("remaining_waypoints")):
		targets = _operator_area_assignment_targets(order)
	elif required_type in ("operator", "cleaner"):
		# Point operator/service jobs are performed at destination. When the UI
		# does not provide "from", add_order fills start with base for validity;
		# that must not make us choose the drone nearest to base.
		end = order.get("end")
		if isinstance(end, (list, tuple)) and len(end) == 2:
			targets = [(float(end[0]), float(end[1]))]
	if not targets:
		order_start = order.get("handover_point")
		if not order_start and order.get("remaining_waypoints"):
			order_start = (order.get("remaining_waypoints") or [None])[0]
		if not order_start:
			order_start = order.get("start") or order.get("end")
		if isinstance(order_start, (list, tuple)) and len(order_start) == 2:
			targets = [(float(order_start[0]), float(order_start[1]))]
	if not targets:
		return float("inf"), tuple(pos)
	best = min(targets, key=lambda p: haversine_m(tuple(pos), tuple(p)))
	return float(haversine_m(tuple(pos), tuple(best))), tuple(best)


def _station_type_for_drone(drone: Dict[str, Any]) -> Optional[str]:
	"""Best-effort station type for an already spawned drone."""
	for key in ("current_station_type", "home_station_type"):
		val = drone.get(key)
		if val:
			return str(val)
	infra = STATE.get("infrastructure") or {}
	stations = infra.get("charging_stations") or []
	ids = [drone.get("current_station_id"), drone.get("home_station_id")]
	for sid in ids:
		if not sid:
			continue
		sid_s = str(sid)
		for st in stations:
			if str((st or {}).get("id")) == sid_s:
				stype = (st or {}).get("station_type")
				return str(stype) if stype else None
	return None


def pick_drone_for_order(order: Dict[str, Any], required_drone_type: str) -> Optional[str]:
	"""Выбор ближайшего свободного дрона к стартовой/передаточной точке заказа."""
	best_drone_id = None
	best_distance = float("inf")
	best_time = float("inf")
	debug_rows = []

	idle_candidates = []
	for drone_id, d in STATE["drones"].items():
		if d.get("type") != required_drone_type:
			continue
		if d.get("status") not in (None, "idle"):
			continue
		if not d.get("pos"):
			continue
		pos = tuple(d.get("pos"))
		dd, target_for_debug = _assignment_distance_to_order(order, pos, required_drone_type)
		if not math.isfinite(dd):
			continue
		idle_candidates.append((float(dd), drone_id, d, pos, target_for_debug))

	idle_candidates.sort(key=lambda x: x[0])
	idle_candidates = idle_candidates[:MAX_DRONE_ASSIGNMENT_CANDIDATES]

	for dd, drone_id, d, pos, target_for_debug in idle_candidates:
		time_sec, res = estimate_order_completion_time_seconds(d, order, required_drone_type)
		route_len = float(res.get("route_length", 0.0)) if res else 0.0
		valid = (time_sec != float("inf") and route_len > 1.0)
		reason = "ok" if valid else ("no_route" if not res or not res.get("ok") else "zero_or_inf")
		debug_rows.append({
			"drone_id": drone_id,
			"drone_pos": pos,
			"pickup": tuple(target_for_debug),
			"distance_direct_m": float(dd),
			"route_len_m": float(route_len),
			"eta_s": float(time_sec) if time_sec != float("inf") else None,
			"valid": bool(valid),
			"reason": reason,
		})
		if valid:
			best_distance = float(dd)
			best_time = time_sec
			best_drone_id = drone_id
			break

	# Для continuation/handover задач не спавним новый дрон автоматически:
	# если свободных нет — заказ ждёт в очереди.
	is_continuation = bool(order.get("remaining_waypoints") or order.get("handover_point") or order.get("is_area_continuation"))
	if is_continuation and best_drone_id is None:
		return None

	# Вариант «новый дрон с базы» — сравниваем ETA
	inv = STATE.get("inventory") or {}
	has_inventory = (inv.get(required_drone_type, 0) or 0) > 0
	time_new = float("inf")
	if has_inventory:
		if best_drone_id is None:
			time_new, _ = estimate_new_drone_completion_time_seconds(order, required_drone_type)
			existing_best = best_time
			if time_new < best_time:
				new_id = spawn_drone_from_inventory(required_drone_type, order_id=str(order.get("id")), caller="pick_drone_for_order")
				if new_id:
					logger.info(
						"pick_drone_for_order: order=%s -> new drone %s (ETA %.0fs < existing best %.0fs)",
						order.get("id"), new_id, time_new, existing_best,
					)
					return new_id

	STATE["last_route_debug"] = {
		"ts": datetime.utcnow().isoformat(),
		"order_id": order.get("id"),
		"required_type": required_drone_type,
		"candidates": sorted(debug_rows, key=lambda r: (r["valid"] is False, r["eta_s"] if r["eta_s"] is not None else 1e18, r["distance_direct_m"]))[:30],
	}
	logger.info("pick_drone candidates count=%s valid=%s", len(debug_rows), sum(1 for r in debug_rows if r.get("valid")))

	if best_drone_id is not None:
		logger.info(
			"pick_drone_for_order: order=%s -> nearest drone %s (distance %.0fm, ETA %.0fs, new_from_base=%.0fs)",
			order.get("id"), best_drone_id, best_distance, best_time, time_new if has_inventory else float("inf"),
		)
		return best_drone_id
	return None


def map_order_to_drone_type(order_type: str) -> str:
	if order_type == "delivery":
		return "cargo"
	if order_type == "shooting":
		return "operator"
	return "cleaner"


def plan_route_for(
	start: Tuple[float, float],
	end: Tuple[float, float],
	drone_type: str,
	battery_level: float,
	waypoints: Optional[List[Tuple[float, float]]] = None,
	reserve_pct: Optional[float] = None,
):
	"""reserve_pct: при None используется RESERVE_PCT; при 0 — весь заряд на путь (для экстренного вылета на зарядку)."""
	city = STATE.get("city")
	city_G = STATE.get("city_graph")
	if not city or city_G is None:
		return None, None, 0.0
	_routing_service.city_graphs[city] = city_G
	res = reserve_pct if reserve_pct is not None else RESERVE_PCT
	max_range = _routing_service.max_reachable_distance(battery_level, MODE_EMPTY, drone_type, reserve_pct=res)
	# 1) If infra is ready, try infra routing FIRST (only for simple A->B without waypoints).
	if is_infrastructure_routing_ready() and not waypoints:
		_pn, coords_infra, length_infra, segs, _diag = plan_infrastructure_preferred_route(
			start, end, drone_type, mode="empty", prefer_network=True
		)
		if coords_infra and len(coords_infra) > 1 and float(length_infra) > 1.0 and segs:
			coords_infra = _avoid_no_fly_in_coords(_to_coord_list(coords_infra))
			length_infra = _route_length(coords_infra)
			logger.info("plan_route_for: mode=%s len=%.1f pts=%s", "infra_route", float(length_infra), len(coords_infra))
			return None, coords_infra, float(length_infra)

	# 2) City door-to-door fallback (MUST use city_graph, never infra_graph).
	path, coords, length = _routing_service.plan_direct_path(city_G, start, end, max_range, waypoints=waypoints)
	if path and coords and len(coords) > 1 and float(length) > 1.0:
		coords = _avoid_no_fly_in_coords(_to_coord_list(coords))
		length = _route_length(coords)
		logger.info("plan_route_for: mode=%s len=%.1f pts=%s", "city_fallback", float(length), len(coords))
		return path, coords, length

	# 3) City node-to-node last resort
	start_node = _routing_service._find_nearest_node(city_G, start)
	end_node = _routing_service._find_nearest_node(city_G, end)
	if not start_node or not end_node:
		return None, None, 0.0
	path = _routing_service._find_safe_path(city_G, start_node, end_node, max_range)
	if not path:
		return None, None, 0.0
	coords = [city_G.nodes[n]['pos'] for n in path]
	coords = _avoid_no_fly_in_coords(_to_coord_list(coords))
	length = _routing_service._calculate_path_length(city_G, path)
	length = _route_length(coords)
	if coords and len(coords) > 1 and float(length) > 1.0:
		logger.info("plan_route_for: mode=%s len=%.1f pts=%s", "city_fallback", float(length), len(coords))
	return path, coords, length


def _plan_order_trip_via_charger_first(
	G, drone_pos: Tuple[float, float], pickup: Tuple[float, float], dropoff: Tuple[float, float],
	drone_type: str, battery_pct: float, charger_nodes: Dict, pickup_node, dropoff_node, start_node,
) -> Optional[Dict[str, Any]]:
	"""Строит маршрут: дрон → ближайшая станция зарядки → забор → доставка → зарядка. Для грузового с запасом 20% после доставки."""
	base_node = charger_nodes.get("base")
	station_nodes = charger_nodes.get("stations") or []
	candidates = []
	if base_node and base_node in G.nodes:
		candidates.append(("base", base_node))
	for i, sn in enumerate(station_nodes):
		if sn and sn in G.nodes:
			candidates.append((f"station_{i}", sn))
	if not candidates:
		return None
	# Ближайшая зарядка от текущей позиции
	best_name, best_node = None, None
	best_len = float("inf")
	for name, node in candidates:
		p, co, l, _ = _routing_service.plan_with_chargers(
			G, start_node, node, battery_pct, MODE_EMPTY, drone_type, reserve_pct=CHARGER_ARRIVAL_MIN_PCT, charger_nodes=charger_nodes,
			max_segment_battery_pct=MAX_SEGMENT_BATTERY_PCT, max_battery_pct_to_reach_charger=MAX_BATTERY_PCT_TO_REACH_CHARGER,
		)
		if p and co and l < best_len:
			best_len = l
			best_name, best_node = name, node
	if not best_node:
		return None
	# Сегмент до зарядки
	path_to_ch, coords_to_ch, len_to_ch, ch_to = _routing_service.plan_with_chargers(
		G, start_node, best_node, battery_pct, MODE_EMPTY, drone_type, reserve_pct=CHARGER_ARRIVAL_MIN_PCT, charger_nodes=charger_nodes,
		max_segment_battery_pct=MAX_SEGMENT_BATTERY_PCT, max_battery_pct_to_reach_charger=MAX_BATTERY_PCT_TO_REACH_CHARGER,
	)
	if not path_to_ch or not coords_to_ch:
		return None
	plan_reserve = CARGO_RESERVE_AFTER_DELIVERY_PCT
	# От зарядки (100%) до забора
	path_a, coords_a, len_a, chargers_a = _routing_service.plan_with_chargers(
		G, best_node, pickup_node, 100.0, MODE_EMPTY, drone_type, reserve_pct=plan_reserve, charger_nodes=charger_nodes,
		max_segment_battery_pct=MAX_SEGMENT_BATTERY_PCT, max_battery_pct_to_reach_charger=MAX_BATTERY_PCT_TO_REACH_CHARGER,
	)
	if not path_a or not coords_a:
		return None
	battery_after_a = 100.0 if chargers_a else _routing_service.compute_battery_after(len_a, 100.0, MODE_EMPTY, drone_type)
	# Забор → доставка
	path_b, coords_b, len_b, chargers_b = _routing_service.plan_with_chargers(
		G, pickup_node, dropoff_node, battery_after_a, MODE_LOADED, drone_type, reserve_pct=plan_reserve, charger_nodes=charger_nodes,
		max_segment_battery_pct=MAX_SEGMENT_BATTERY_PCT, max_battery_pct_to_reach_charger=MAX_BATTERY_PCT_TO_REACH_CHARGER,
	)
	if not path_b or not coords_b:
		return None
	battery_after_b = _routing_service.compute_battery_after(len_b, battery_after_a, MODE_LOADED, drone_type)
	if chargers_b:
		battery_after_b = 100.0
	if battery_after_b < CARGO_RESERVE_AFTER_DELIVERY_PCT:
		return None
	# Доставка → зарядка (escape)
	path_c, coords_c, len_c, chargers_c = None, None, 0.0, []
	station_list = charger_nodes.get("stations") or []
	for name, goal_n in [("base", charger_nodes.get("base"))] + [(f"station_{i}", sn) for i, sn in enumerate(station_list) if sn]:
		if goal_n is None or goal_n not in G.nodes:
			continue
		p_c, co_c, l_c, ch_c = _routing_service.plan_with_chargers(
			G, dropoff_node, goal_n, battery_after_b, MODE_EMPTY, drone_type, reserve_pct=plan_reserve, charger_nodes=charger_nodes,
			max_segment_battery_pct=MAX_SEGMENT_BATTERY_PCT, max_battery_pct_to_reach_charger=MAX_BATTERY_PCT_TO_REACH_CHARGER,
		)
		if p_c and co_c and l_c < (len_c if path_c else float("inf")):
			path_c, coords_c, len_c, chargers_c = p_c, co_c, l_c, ch_c
	if not path_c or not coords_c:
		return None
	full_coords = list(coords_to_ch)
	if coords_a:
		full_coords.extend(coords_a[1:])
	if coords_b:
		full_coords.extend(coords_b[1:])
	if coords_c:
		full_coords.extend(coords_c[1:])
	pickup_waypoint_count = len(coords_to_ch) + len(coords_a) - 1
	total_length = len_to_ch + len_a + len_b + len_c
	chargers_used = list(dict.fromkeys(ch_to + chargers_a + chargers_b + chargers_c))
	return {
		"ok": True,
		"coords": full_coords,
		"segments": [
			{"type": "to_charger", "coords": coords_to_ch, "length": len_to_ch, "chargers": ch_to},
			{"type": "to_pickup", "coords": coords_a, "length": len_a, "chargers": chargers_a},
			{"type": "to_dropoff", "coords": coords_b, "length": len_b, "chargers": chargers_b},
			{"type": "escape", "coords": coords_c, "length": len_c, "chargers": chargers_c},
		],
		"chargers_used": chargers_used,
		"battery_plan": [
			{"at": "start", "battery": battery_pct},
			{"at": "after_charger", "battery": 100.0},
			{"at": "after_pickup", "battery": battery_after_a},
			{"at": "after_dropoff", "battery": battery_after_b},
			{"at": "after_escape", "battery": 100.0},
		],
		"pickup_waypoint_count": pickup_waypoint_count,
		"route_length": total_length,
	}


def plan_order_trip(
	drone_pos: Tuple[float, float],
	pickup: Tuple[float, float],
	dropoff: Tuple[float, float],
	drone_type: str,
	battery_pct: float,
) -> Dict[str, Any]:
	"""
	Three-phase: A (empty) drone->pickup, B (loaded) pickup->dropoff, C (empty) dropoff->charger.
	Каждая фаза может проходить через несколько станций зарядки: если прямого пути не хватает по заряду,
	строится маршрут до станции, затем от неё до цели; при необходимости добавляются следующие станции.
	Returns {ok, coords, segments, chargers_used, battery_plan, pickup_waypoint_count, route_length}
	or {ok: False, reason, details}.
	"""
	G = STATE.get("city_graph")
	city = STATE.get("city")
	if not city or G is None:
		return {"ok": False, "reason": NO_PATH_TO_PICKUP, "details": "no city graph"}

	logger.info(
		"plan_order_trip: graphs city_graph=%s infra_ready=%s",
		"ok" if (G is not None and getattr(G, "number_of_edges", lambda: 0)() > 0) else "missing",
		bool(is_infrastructure_routing_ready()),
	)

	# Три плеча: leg1 start→pickup (empty), leg2 pickup→dropoff (loaded), leg3 escape (empty). Сначала infra по каждому плечу.
	if is_infrastructure_routing_ready():
		try:
			plan_reserve = PLAN_RESERVE_PCT_TEST if STATE.get("battery_mode") == "test" else PLAN_RESERVE_PCT
			if drone_type == "cargo":
				plan_reserve = max(plan_reserve, CARGO_RESERVE_AFTER_DELIVERY_PCT)

			leg2_air_m = float(haversine_m(tuple(pickup), tuple(dropoff)))
			long_kw: Dict[str, Any] = {}
			if leg2_air_m > 1000.0:
				long_kw = {
					"long_leg_quality_threshold_m": 1000.0,
					"long_leg_min_infra_ratio": 0.7,
					"long_leg_max_off_network_ratio": 0.3,
				}

			def _log_leg_diag(leg: str, mode_s: str, diag: Dict[str, Any]) -> None:
				d = _enrich_route_diagnostics(dict(diag or {}))
				logger.info(
					"plan_order_trip leg=%s mode=%s trunk_ratio=%.3f infra_ratio=%.3f off_network_ratio=%.3f",
					leg,
					mode_s,
					float(d.get("trunk_ratio") or 0.0),
					float(d.get("infra_ratio") or 0.0),
					float(d.get("off_network_ratio") or 0.0),
				)

			def _city_leg_direct(
				start_c: Tuple[float, float],
				end_c: Tuple[float, float],
				max_m: float,
				seg_mode: str,
			) -> Tuple[Optional[List[Tuple[float, float]]], float, List[Dict[str, Any]], Dict[str, Any], str]:
				_pa, ca, la = _routing_service.plan_direct_path(G, start_c, end_c, max_m)
				if not ca or len(ca) < 2 or float(la) <= 1.0:
					return None, 0.0, [], {}, "city_direct_unavailable"
				coords_list = [(float(p[0]), float(p[1])) for p in ca]
				segs = _route_segments_from_coords(coords_list, mode=seg_mode, drone_type=drone_type)
				diag = _enrich_route_diagnostics(
					_infra_route_diagnostics_from_segments(segs, start_end_m=float(haversine_m(tuple(start_c), tuple(end_c))))
				)
				return coords_list, float(la), segs, diag, "city_direct"

			def _free_flight_leg(
				start_c: Tuple[float, float],
				end_c: Tuple[float, float],
				max_m: float,
				seg_mode: str,
			) -> Tuple[Optional[List[Tuple[float, float]]], float, List[Dict[str, Any]], Dict[str, Any], str]:
				coords_list, length_m = _free_flight_path_between(tuple(start_c), tuple(end_c), max_m)
				if not coords_list or len(coords_list) < 2:
					return None, 0.0, [], {}, "free_flight_unavailable"
				if length_m <= 1.0:
					return None, 0.0, [], {}, "free_flight_range"
				segs = _off_network_segments_from_coords(coords_list, mode=seg_mode)
				diag = _enrich_route_diagnostics(
					_infra_route_diagnostics_from_segments(segs, start_end_m=float(haversine_m(tuple(start_c), tuple(end_c))))
				)
				diag["free_flight"] = True
				return coords_list, length_m, segs, diag, "free_flight"

			def _use_free_over_network(
				free_coords: Optional[List[Tuple[float, float]]],
				free_len: float,
				network_coords: Optional[List[Tuple[float, float]]],
				network_len: float,
			) -> bool:
				if not free_coords or len(free_coords) < 2 or float(free_len) <= 1.0:
					return False
				if not network_coords or len(network_coords) < 2 or float(network_len) <= 1.0:
					return True
				return float(free_len) <= float(network_len) * FREE_FLIGHT_PREFER_RATIO

			max_a = _routing_service.max_reachable_distance(battery_pct, MODE_EMPTY, drone_type, reserve_pct=plan_reserve)
			_p1, c1, l1, s1, d1 = plan_infrastructure_preferred_route(
				drone_pos, pickup, drone_type, mode=MODE_EMPTY, prefer_network=True
			)
			fb1 = False
			fb1_reason = ""
			free_c1, free_l1, free_s1, free_d1, free_r1 = _free_flight_leg(drone_pos, pickup, max_a, MODE_EMPTY)
			infra1_ok = bool(c1 and len(c1) > 1 and s1 and float(l1) > 1.0 and float(l1) <= float(max_a))
			if _use_free_over_network(free_c1, free_l1, c1 if infra1_ok else None, l1):
				c1, l1, s1, d1, fb1_reason = free_c1, free_l1, free_s1, free_d1, free_r1
				fb1 = True
				_p1 = None
			elif not infra1_ok:
				c1, l1, s1, d1, fb1_reason = _city_leg_direct(drone_pos, pickup, max_a, MODE_EMPTY)
				fb1 = bool(c1)
				_p1 = None
			if not c1:
				raise ValueError("leg1 to_pickup impossible")

			battery_after_a_i = _routing_service.compute_battery_after(float(l1), battery_pct, MODE_EMPTY, drone_type)
			_log_leg_diag("to_pickup", MODE_EMPTY, d1)

			max_b = _routing_service.max_reachable_distance(battery_after_a_i, MODE_LOADED, drone_type, reserve_pct=plan_reserve)
			_p2, c2, l2, s2, d2 = plan_infrastructure_preferred_route(
				pickup, dropoff, drone_type, mode=MODE_LOADED, prefer_network=True, **long_kw
			)
			fb2 = False
			fb2_reason = ""
			free_c2, free_l2, free_s2, free_d2, free_r2 = _free_flight_leg(pickup, dropoff, max_b, MODE_LOADED)
			infra2_ok = bool(c2 and len(c2) > 1 and s2 and float(l2) > 1.0 and float(l2) <= float(max_b))
			if _use_free_over_network(free_c2, free_l2, c2 if infra2_ok else None, l2):
				c2, l2, s2, d2, fb2_reason = free_c2, free_l2, free_s2, free_d2, free_r2
				fb2 = True
				_p2 = None
			elif not infra2_ok:
				c2, l2, s2, d2, fb2_reason = _city_leg_direct(pickup, dropoff, max_b, MODE_LOADED)
				fb2 = bool(c2)
				_p2 = None
			if not c2:
				raise ValueError("leg2 pickup_to_dropoff impossible")
			if fb2:
				logger.warning(
					"plan_order_trip leg=pickup_to_dropoff used fallback route, reason=%s",
					fb2_reason,
				)

			_log_leg_diag("pickup_to_dropoff", MODE_LOADED, d2)

			battery_after_b_i = _routing_service.compute_battery_after(float(l2), battery_after_a_i, MODE_LOADED, drone_type)
			if drone_type == "cargo" and float(battery_after_b_i) + 1e-9 < float(CARGO_RESERVE_AFTER_DELIVERY_PCT):
				raise ValueError("infra legs leave insufficient battery after delivery")

			coords_c: List[Tuple[float, float]] = []
			segs_c: List[Dict[str, Any]] = []
			len_c = 0.0
			chargers_used: List[str] = []
			battery_after_escape = float(battery_after_b_i)
			fb3 = False
			d3: Dict[str, Any] = {}
			escape_charger_label = ""

			max_c = _routing_service.max_reachable_distance(float(battery_after_b_i), MODE_EMPTY, drone_type, reserve_pct=plan_reserve)
			charger_nodes = limited_charger_nodes(dropoff)
			station_nodes = charger_nodes.get("stations") or []
			base_node = charger_nodes.get("base")
			dropoff_node = _routing_service._find_nearest_node(G, dropoff)

			best_infra_escape: Optional[Tuple[float, List[Tuple[float, float]], List[Dict[str, Any]], Dict[str, Any], str]] = None
			cand_pts: List[Tuple[str, Tuple[float, float]]] = []
			if STATE.get("base"):
				bc = tuple(STATE["base"])
				if len(bc) >= 2:
					cand_pts.append(("base", (float(bc[0]), float(bc[1]))))
			for i, s in enumerate(STATE.get("stations") or []):
				if isinstance(s, (list, tuple)) and len(s) >= 2:
					cand_pts.append((f"station_{i}", (float(s[0]), float(s[1]))))

			for ch_name, goal_pt in cand_pts:
				_pe, co_e, le, sg_e, dg_e = plan_infrastructure_preferred_route(
					dropoff, goal_pt, drone_type, mode=MODE_EMPTY, prefer_network=True
				)
				if co_e and len(co_e) > 1 and sg_e and float(le) > 1.0 and float(le) <= float(max_c):
					bat_try = _routing_service.compute_battery_after(float(le), float(battery_after_b_i), MODE_EMPTY, drone_type)
					if bat_try + 1e-9 >= 0.0:
						if best_infra_escape is None or float(le) < float(best_infra_escape[0]):
							best_infra_escape = (float(le), list(co_e), list(sg_e), dict(dg_e or {}), ch_name)

			if best_infra_escape is not None:
				len_c, coords_c, segs_c, d3, escape_charger_label = best_infra_escape
				chargers_used = [escape_charger_label] if escape_charger_label else []
				battery_after_escape = _routing_service.compute_battery_after(float(len_c), float(battery_after_b_i), MODE_EMPTY, drone_type)
			else:
				best_c = None
				best_len_c = float("inf")
				for name, goal_n in [("base", base_node)] + [(f"station_{i}", sn) for i, sn in enumerate(station_nodes) if sn]:
					if goal_n is None or goal_n not in G.nodes or dropoff_node is None:
						continue
					p_c, co_c, l_c, ch_c = _routing_service.plan_with_chargers(
						G, dropoff_node, goal_n, float(battery_after_b_i), MODE_EMPTY, drone_type,
						reserve_pct=plan_reserve, charger_nodes=charger_nodes,
						max_segment_battery_pct=MAX_SEGMENT_BATTERY_PCT,
						max_battery_pct_to_reach_charger=MAX_BATTERY_PCT_TO_REACH_CHARGER,
					)
					if p_c and co_c and float(l_c) < best_len_c:
						best_len_c = float(l_c)
						best_c = (name, co_c, ch_c)
				if best_c:
					escape_charger_label, coords_c, ch_c = best_c[0], list(best_c[1]), best_c[2]
					len_c = float(best_len_c)
					chargers_used = list(ch_c or [])
					segs_c = _route_segments_from_coords(
						[(float(p[0]), float(p[1])) for p in coords_c], mode=MODE_EMPTY, drone_type=drone_type
					)
					d3 = _enrich_route_diagnostics(
						_infra_route_diagnostics_from_segments(segs_c, start_end_m=float(haversine_m(tuple(dropoff), tuple(coords_c[-1]))))
					)
					fb3 = True
					if chargers_used:
						battery_after_escape = 100.0
					else:
						battery_after_escape = _routing_service.compute_battery_after(len_c, float(battery_after_b_i), MODE_EMPTY, drone_type)

			full_coords = concat_coords_without_duplicate_endpoints(c1, c2, coords_c if coords_c else None)

			route_segments: List[Dict[str, Any]] = []
			for seg in s1 or []:
				sg = dict(seg)
				sg["mode"] = MODE_EMPTY
				route_segments.append(sg)
			for seg in s2 or []:
				sg = dict(seg)
				sg["mode"] = MODE_LOADED
				route_segments.append(sg)
			for seg in segs_c or []:
				sg = dict(seg)
				sg["mode"] = MODE_EMPTY
				route_segments.append(sg)

			leg_diags_for_total: List[Dict[str, Any]] = [dict(d1 or {}), dict(d2 or {})]
			escape_leg_payload: Dict[str, Any] = {}
			if coords_c:
				leg_diags_for_total.append(dict(d3 or {}))
				escape_leg_payload = {
					**(d3 or {}),
					"used_fallback": bool(fb3),
					"fallback_reason": ("city_chargers" if fb3 else None),
					"charger_target": escape_charger_label or None,
				}
			route_diagnostics: Dict[str, Any] = {
				"legs": {
					"to_pickup": {**(d1 or {}), "used_fallback": bool(fb1), "fallback_reason": (fb1_reason if fb1 else None)},
					"pickup_to_dropoff": {**(d2 or {}), "used_fallback": bool(fb2), "fallback_reason": (fb2_reason if fb2 else None)},
					"escape": escape_leg_payload,
				},
				"total": _aggregate_route_diagnostics_from_legs(leg_diags_for_total),
			}
			tot = route_diagnostics["total"]
			for copy_k in (
				"trunk_m", "branch_m", "local_m", "voronoi_m", "off_network_m", "total_m",
				"trunk_ratio", "infra_ratio", "off_network_ratio", "local_ratio", "local_voronoi_ratio",
				"trunk_pct", "local_voronoi_pct", "off_network_pct",
			):
				if copy_k in tot:
					route_diagnostics[copy_k] = tot[copy_k]
			if bool((d1 or {}).get("strict_layer")) and bool((d2 or {}).get("strict_layer")) and (not coords_c or bool((d3 or {}).get("strict_layer"))):
				route_diagnostics["strict_layer"] = True

			total_length = float(l1) + float(l2) + float(len_c)
			logger.info(
				"plan_order_trip total trunk_ratio=%.3f infra_ratio=%.3f off_network_ratio=%.3f route_len=%.1f pts=%s",
				float(tot.get("trunk_ratio") or 0.0),
				float(tot.get("infra_ratio") or 0.0),
				float(tot.get("off_network_ratio") or 0.0),
				float(total_length),
				len(full_coords),
			)

			if total_length > 1.0 and len(full_coords) > 1:
				return {
					"ok": True,
					"coords": full_coords,
					"route_segments": route_segments,
					"route_diagnostics": route_diagnostics,
					"segments": (
						[
							{"type": "to_pickup", "coords": c1, "length": float(l1), "chargers": []},
							{"type": "to_dropoff", "coords": c2, "length": float(l2), "chargers": []},
						]
						+ ([{"type": "escape", "coords": coords_c, "length": float(len_c), "chargers": chargers_used}] if coords_c else [])
					),
					"chargers_used": chargers_used,
					"battery_plan": [
						{"at": "start", "battery": float(battery_pct)},
						{"at": "after_pickup", "battery": float(battery_after_a_i)},
						{"at": "after_dropoff", "battery": float(battery_after_b_i)},
						{"at": "after_escape", "battery": float(battery_after_escape)},
					],
					"pickup_waypoint_count": len(c1),
					"route_length": float(total_length),
				}
		except Exception:
			# infra route must never break orders; fall back below
			logger.exception("plan_order_trip: infra_route failed, falling back to city_graph")

	# Primary (while infra is unstable): door-to-door city_graph plan.
	try:
		max_a = _routing_service.max_reachable_distance(battery_pct, MODE_EMPTY, drone_type, reserve_pct=PLAN_RESERVE_PCT)
		_pa, ca, la = _routing_service.plan_direct_path(G, drone_pos, pickup, max_a)
		if ca and len(ca) > 1:
			battery_after_a = _routing_service.compute_battery_after(la, battery_pct, MODE_EMPTY, drone_type)
			max_b = _routing_service.max_reachable_distance(battery_after_a, MODE_LOADED, drone_type, reserve_pct=PLAN_RESERVE_PCT)
			_pb, cb, lb = _routing_service.plan_direct_path(G, pickup, dropoff, max_b)
			if cb and len(cb) > 1:
				full_coords = list(ca) + list(cb[1:])
				route_segments = _route_segments_from_coords(
					[tuple(p) for p in full_coords], mode="empty", drone_type=drone_type
				)
				total_length = float(la) + float(lb)
				logger.info("plan_order_trip: mode=%s len=%.1f pts=%s segs=%s", "city_fallback", total_length, len(full_coords), len(route_segments))
				return {
					"ok": True,
					"coords": full_coords,
					"route_segments": route_segments,
					"segments": [],
					"chargers_used": [],
					"battery_plan": [],
					"pickup_waypoint_count": len(ca),
					"route_length": total_length,
				}
	except Exception:
		pass
	# Guard: if pickup/dropoff are different but planning degenerates to zero, reject later.
	try:
		if haversine_m(tuple(pickup), tuple(dropoff)) > 30.0:
			pass
	except Exception:
		pass
	# Всегда синхронизируем режим батареи из STATE, чтобы тест/реальность применялись при планировании
	_routing_service.set_battery_mode(STATE.get("battery_mode", "reality"))
	_routing_service.city_graphs[city] = G
	charger_nodes = limited_charger_nodes(drone_pos)
	# В тесте — большой запас; для грузового — после доставки оставляем 20%
	plan_reserve = PLAN_RESERVE_PCT_TEST if STATE.get("battery_mode") == "test" else PLAN_RESERVE_PCT
	if drone_type == "cargo":
		plan_reserve = max(plan_reserve, CARGO_RESERVE_AFTER_DELIVERY_PCT)

	pickup_node = _routing_service._find_nearest_node(G, pickup)
	dropoff_node = _routing_service._find_nearest_node(G, dropoff)
	start_node = _routing_service._find_nearest_node(G, drone_pos)
	if not start_node or not pickup_node or not dropoff_node:
		logger.warning("plan_order_trip: missing node start=%s pickup=%s dropoff=%s", start_node, pickup_node, dropoff_node)
		return {"ok": False, "reason": NO_PATH_TO_PICKUP, "details": "nearest node not found"}

	# While infrastructure is building (and legacy stations are empty), allow a pure city-graph plan
	# without multi-charger constraints. This keeps orders flying instead of getting stuck queued.
	try:
		if STATE.get("infrastructure_build_status") != "ready" and not (charger_nodes.get("stations") or []):
			max_a = _routing_service.max_reachable_distance(battery_pct, MODE_EMPTY, drone_type, reserve_pct=plan_reserve)
			_pa, ca, la = _routing_service.plan_direct_path(G, drone_pos, pickup, max_a)
			if not ca or len(ca) < 2:
				return {"ok": False, "reason": NO_PATH_TO_PICKUP, "details": "city_graph direct path to pickup failed"}
			battery_after_a_direct = _routing_service.compute_battery_after(la, battery_pct, MODE_EMPTY, drone_type)
			max_b = _routing_service.max_reachable_distance(battery_after_a_direct, MODE_LOADED, drone_type, reserve_pct=plan_reserve)
			_pb, cb, lb = _routing_service.plan_direct_path(G, pickup, dropoff, max_b)
			if not cb or len(cb) < 2:
				return {"ok": False, "reason": NO_FEASIBLE_CHAIN_PICKUP_TO_DROPOFF_LOADED, "details": "city_graph direct path pickup->dropoff failed"}
			full_coords = list(ca) + list(cb[1:])
			route_segments = _route_segments_from_coords(
				[tuple(p) for p in full_coords], mode="empty", drone_type=drone_type
			)
			total_length = float(la) + float(lb)
			return {
				"ok": True,
				"coords": full_coords,
				"route_segments": route_segments,
				"segments": [],
				"chargers_used": [],
				"battery_plan": [],
				"pickup_waypoint_count": len(ca),
				"route_length": total_length,
			}
	except Exception:
		pass

	# Попытка без предварительной зарядки; для грузового проверяем, что после доставки остаётся >= 20%
	logger.info("plan_order_trip: stage=%s graph=%s", "A_to_pickup", "city_graph")
	path_a, coords_a, len_a, chargers_a = _routing_service.plan_with_chargers(
		G, start_node, pickup_node, battery_pct, MODE_EMPTY, drone_type, reserve_pct=plan_reserve, charger_nodes=charger_nodes,
		max_segment_battery_pct=MAX_SEGMENT_BATTERY_PCT, max_battery_pct_to_reach_charger=MAX_BATTERY_PCT_TO_REACH_CHARGER,
	)
	if not path_a or not coords_a:
		# Не хватает заряда до точки забора — строим маршрут через станцию зарядки
		if drone_type == "cargo" and (charger_nodes.get("base") or (charger_nodes.get("stations") or [])):
			result = _plan_order_trip_via_charger_first(G, drone_pos, pickup, dropoff, drone_type, battery_pct, charger_nodes, pickup_node, dropoff_node, start_node)
			if result:
				logger.info("plan_order_trip: route via charger first (battery=%.1f%% -> pickup)", battery_pct)
				return result
		logger.info("plan_order_trip: no feasible chain to pickup battery=%.1f", battery_pct)
		return {"ok": False, "reason": NO_FEASIBLE_CHARGING_CHAIN_TO_PICKUP, "details": "no path to pickup"}

	battery_after_a = _routing_service.compute_battery_after(len_a, battery_pct, MODE_EMPTY, drone_type)
	if chargers_a:
		battery_after_a = 100.0

	# Stage B: pickup -> dropoff (loaded); для грузового после доставки должно остаться >= 20%
	logger.info("plan_order_trip: stage=%s graph=%s", "B_to_dropoff", "city_graph")
	path_b, coords_b, len_b, chargers_b = _routing_service.plan_with_chargers(
		G, pickup_node, dropoff_node, battery_after_a, MODE_LOADED, drone_type, reserve_pct=plan_reserve, charger_nodes=charger_nodes,
		max_segment_battery_pct=MAX_SEGMENT_BATTERY_PCT, max_battery_pct_to_reach_charger=MAX_BATTERY_PCT_TO_REACH_CHARGER,
	)
	if not path_b or not coords_b:
		if drone_type == "cargo" and (charger_nodes.get("base") or (charger_nodes.get("stations") or [])):
			result = _plan_order_trip_via_charger_first(G, drone_pos, pickup, dropoff, drone_type, battery_pct, charger_nodes, pickup_node, dropoff_node, start_node)
			if result:
				logger.info("plan_order_trip: route via charger first (pickup->dropoff loaded infeasible)")
				return result
		logger.info("plan_order_trip: no feasible chain pickup->dropoff loaded")
		return {"ok": False, "reason": NO_FEASIBLE_CHAIN_PICKUP_TO_DROPOFF_LOADED, "details": "loaded segment infeasible"}

	battery_after_b = _routing_service.compute_battery_after(len_b, battery_after_a, MODE_LOADED, drone_type)
	if chargers_b:
		battery_after_b = 100.0
	# Грузовой: после доставки должно оставаться >= 20%; иначе маршрут через зарядку в начале
	if drone_type == "cargo" and battery_after_b < CARGO_RESERVE_AFTER_DELIVERY_PCT:
		result = _plan_order_trip_via_charger_first(G, drone_pos, pickup, dropoff, drone_type, battery_pct, charger_nodes, pickup_node, dropoff_node, start_node)
		if result:
			logger.info("plan_order_trip: route via charger first (battery after dropoff %.1f%% < 20%%)", battery_after_b)
			return result
		return {"ok": False, "reason": NO_FEASIBLE_CHAIN_PICKUP_TO_DROPOFF_LOADED, "details": "insufficient battery after delivery (need 20%% reserve)"}

	# Stage C: dropoff -> any charger (escape)
	logger.info("plan_order_trip: stage=%s graph=%s", "C_escape", "city_graph")
	path_c, coords_c, len_c, chargers_c = None, None, 0.0, []
	base_node = charger_nodes.get("base")
	station_nodes = charger_nodes.get("stations") or []
	best_c = None
	best_len_c = float("inf")
	for name, goal_n in [("base", base_node)] + [(f"station_{i}", sn) for i, sn in enumerate(station_nodes) if sn]:
		if goal_n is None or goal_n not in G.nodes:
			continue
		p_c, co_c, l_c, ch_c = _routing_service.plan_with_chargers(
			G, dropoff_node, goal_n, battery_after_b, MODE_EMPTY, drone_type, reserve_pct=plan_reserve, charger_nodes=charger_nodes,
			max_segment_battery_pct=MAX_SEGMENT_BATTERY_PCT, max_battery_pct_to_reach_charger=MAX_BATTERY_PCT_TO_REACH_CHARGER,
		)
		if p_c and co_c and l_c < best_len_c:
			best_len_c = l_c
			best_c = (p_c, co_c, l_c, ch_c)
	if not best_c:
		# If there are no stations configured yet (infra is building), do not block the order.
		# We'll allow finishing at dropoff without an "escape to charger" segment.
		if not any(station_nodes):
			path_c, coords_c, len_c, chargers_c = [], [], 0.0, []
		else:
			logger.info("plan_order_trip: no escape after dropoff battery_after_b=%.1f", battery_after_b)
			return {"ok": False, "reason": NO_ESCAPE_AFTER_DROPOFF, "details": "no reachable charger after dropoff"}
	else:
		path_c, coords_c, len_c, chargers_c = best_c

	full_coords = list(coords_a)
	if coords_b:
		full_coords.extend(coords_b[1:])
	if coords_c:
		full_coords.extend(coords_c[1:])
	pickup_waypoint_count = len(coords_a)

	# Build route_segments with mode switch after pickup
	route_segments: List[Dict[str, Any]] = []
	try:
		segs_all = _route_segments_from_coords(full_coords, mode="empty", drone_type=drone_type)
		# segments index i corresponds to coords[i] -> coords[i+1]
		for si, seg in enumerate(segs_all):
			# before pickup: empty; from pickup onwards: loaded until dropoff; then empty for escape
			# pickup point is coords[pickup_waypoint_count-1]
			# dropoff point is end of coords_b (coords_a + coords_b[1:]) => idx_dropoff = pickup_waypoint_count + (len(coords_b)-1) - 1
			dropoff_coord_idx = max(0, pickup_waypoint_count + max(0, len(coords_b or []) - 1) - 1)
			mode_here = "empty"
			if si >= max(0, pickup_waypoint_count - 1) and si < dropoff_coord_idx:
				mode_here = "loaded"
			seg["mode"] = mode_here
			route_segments.append(seg)
	except Exception:
		route_segments = _route_segments_from_coords(full_coords, mode="empty", drone_type=drone_type)

	segments = [
		{"type": "to_pickup", "coords": coords_a, "length": len_a, "chargers": chargers_a},
		{"type": "to_dropoff", "coords": coords_b, "length": len_b, "chargers": chargers_b},
		{"type": "escape", "coords": coords_c, "length": len_c, "chargers": chargers_c},
	]
	chargers_used = list(dict.fromkeys(chargers_a + chargers_b + chargers_c))
	battery_after_escape = 100.0 if chargers_c else _routing_service.compute_battery_after(len_c, battery_after_b, MODE_EMPTY, drone_type)
	battery_plan = [
		{"at": "start", "battery": battery_pct},
		{"at": "after_pickup", "battery": battery_after_a},
		{"at": "after_dropoff", "battery": battery_after_b},
		{"at": "after_escape", "battery": battery_after_escape},
	]
	total_length = len_a + len_b + len_c
	if total_length <= 1.0 and haversine_m(tuple(pickup), tuple(dropoff)) > 30.0:
		return {"ok": False, "reason": "ZERO_ROUTE", "details": "route_length=0 for distinct pickup/dropoff"}

	return {
		"ok": True,
		"coords": full_coords,
		"route_segments": route_segments,
		"segments": segments,
		"chargers_used": chargers_used,
		"battery_plan": battery_plan,
		"pickup_waypoint_count": pickup_waypoint_count,
		"route_length": total_length,
	}


def plan_via_base_if_needed(current: Tuple[float, float], end: Tuple[float, float], drone_type: str, battery_level: float):
	"""Single segment with optional charging; uses meta-graph (plan_with_chargers)."""
	G = STATE.get("city_graph")
	city = STATE.get("city")
	if not city or G is None:
		return None, None, 0.0
	_routing_service.city_graphs[city] = G
	start_node = _routing_service._find_nearest_node(G, current)
	end_node = _routing_service._find_nearest_node(G, end)
	if not start_node or not end_node:
		return None, None, 0.0
	charger_nodes = STATE.get("charger_nodes") or {"base": None, "stations": []}
	path, coords, length, _ = _routing_service.plan_with_chargers(
		G, start_node, end_node, battery_level, MODE_EMPTY, drone_type, reserve_pct=RESERVE_PCT, charger_nodes=charger_nodes
	)
	return (path, coords, length) if coords else (None, None, 0.0)


def can_escape_after(point: Tuple[float, float], drone_type: str) -> bool:
	"""True if from point we can reach some charger with reserve (plan_with_chargers)."""
	G = STATE.get("city_graph")
	if G is None:
		return True
	# While infrastructure is building (and legacy stations may be empty),
	# do not hard-block orders by "escape" feasibility.
	if STATE.get("infrastructure_build_status") in ("building", "idle", "error"):
		return True
	charger_nodes = STATE.get("charger_nodes") or {"base": None, "stations": []}
	point_node = _routing_service._find_nearest_node(G, point)
	# If we cannot bind to the graph, don't hard-block the order — let fallback routing decide.
	if not point_node:
		return True
	base_node = charger_nodes.get("base")
	stations = charger_nodes.get("stations") or []
	if not base_node and not any(stations):
		# No chargers configured yet (infra building and legacy stations empty) — do not block.
		return True
	for goal in [base_node] + [s for s in stations if s]:
		if goal is None or goal not in G.nodes:
			continue
		_, coords, _, _ = _routing_service.plan_with_chargers(
			G, point_node, goal, 80.0, MODE_EMPTY, drone_type, reserve_pct=RESERVE_PCT, charger_nodes=charger_nodes
		)
		if coords:
			return True
	return False


def simulate_step():
	# move drones along their routes, drain battery, reroute if blocked and avoid collisions
	_routing_service.set_battery_mode(STATE.get("battery_mode", "reality"))
	city = STATE.get("city")
	G = get_active_routing_graph()
	if not city or G is None:
		return
	for drone_id, drone in STATE["drones"].items():
		_sanitize_active_drone_state(drone_id, drone)
		if drone.get("status") == "avoidance":
			_avoidance_step(drone_id, drone)
			continue
		# Сначала: при заряде <=20% любой дрон обязан лететь на зарядку (жёсткое правило)
		if drone.get("battery", 100.0) <= FLY_TO_CHARGER_AT_PCT:
			if drone.get("status") in ("idle", "holding"):
				_save_route_for_return_if_on_order(drone_id, drone)
				drone["status"] = "low_battery"
				maybe_route_to_base_or_station(drone)
				logger.info("simulate_step: drone %s battery=%.1f%% -> routing to charger (idle/holding)", drone_id, drone.get("battery"))
			elif drone.get("status") == "enroute":
				_save_route_for_return_if_on_order(drone_id, drone)
				if (drone.get("route") or []) and drone.get("target_idx", 0) < len(drone.get("route", [])):
					# Уже едет по маршруту заказа — переключаем на зарядку
					drone["status"] = "low_battery"
					maybe_route_to_base_or_station(drone)
					logger.info("simulate_step: drone %s battery=%.1f%% -> routing to charger (was enroute), saved route for resume", drone_id, drone.get("battery"))
		# Движение по маршруту: не только enroute, но и поездка на зарядку
		if drone.get("status") not in ("enroute", "return_charge", "return_base", "low_battery"):
			continue
		_mark_order_in_progress_if_started(drone_id, drone)
		route = drone.get("route") or []
		idx = drone.get("target_idx", 0)
		# Update current echelon from route_segments (preferred), fallback to edge inference
		try:
			segs = drone.get("route_segments") or []
			if segs and idx > 0 and (idx - 1) < len(segs):
				drone["current_echelon"] = segs[idx - 1].get("echelon")
		except Exception:
			pass
		if not route or idx >= len(route):
			mark_order_completed_if_any(drone_id)
			if drone.get("status") != "charging":
				drone["status"] = "idle"
			# При заряде <=20% после завершения — сразу на зарядку
			if drone.get("battery", 100.0) <= FLY_TO_CHARGER_AT_PCT:
				maybe_route_to_base_or_station(drone)
			continue
		try:
			zone_version = int(STATE.get("zone_version", 0) or 0)
			seen_zone_version = int(drone.get("_zone_version_seen", zone_version) or 0)
			if zone_version != seen_zone_version and _remaining_route_intersects_no_fly_zone(route, max(0, idx - 1)):
				if _replan_active_route_for_zones(drone_id, drone):
					continue
				drone["_zone_version_seen"] = zone_version
		except Exception:
			pass
		current = drone["pos"]
		target = route[idx]
		# Periodic movement debug (throttled per drone)
		try:
			now = time.time()
			last = float(drone.get("_last_move_log_ts") or 0.0)
			if now - last >= 5.0:
				drone["_last_move_log_ts"] = now
				logger.info(
					"simulate_step: drone=%s status=%s target_idx=%s/%s pos=%s target=%s dist=%.1fm",
					drone_id,
					drone.get("status"),
					int(idx),
					int(len(route) if isinstance(route, list) else 0),
					tuple(current) if isinstance(current, (list, tuple)) and len(current) == 2 else current,
					tuple(target) if isinstance(target, (list, tuple)) and len(target) == 2 else target,
					float(haversine_m(tuple(current), tuple(target))) if isinstance(current, (list, tuple)) and isinstance(target, (list, tuple)) else -1.0,
				)
		except Exception:
			pass
		# Echelon hint for UI: infer from infrastructure edge (if available)
		try:
			infra_G = STATE.get("infrastructure_graph")
			if infra_G is not None and getattr(infra_G, "number_of_edges", lambda: 0)() > 0:
				n1 = _routing_service._find_nearest_node(infra_G, tuple(current))
				n2 = _routing_service._find_nearest_node(infra_G, tuple(target))
				if n1 is not None and n2 is not None and infra_G.has_edge(n1, n2):
					ed = infra_G.get_edge_data(n1, n2) or {}
					drone["current_echelon"] = ed.get("flight_level")
				else:
					drone["current_echelon"] = drone.get("current_echelon")
		except Exception:
			pass
		# Для operator_area миссии не перепрокладываем через road graph:
		# там маршрут уже задан внутренними координатами зоны осмотра.
		if (
			(is_point_in_any_zone(target) or _segment_intersects_no_fly_zone(tuple(current), tuple(target)))
			and drone.get("mission_mode") != "operator_area"
		):
			# attempt reroute
			if _replan_active_route_for_zones(drone_id, drone):
				continue
			else:
				drone["status"] = "holding"
				continue
		elif is_point_in_any_zone(target) and drone.get("mission_mode") == "operator_area":
			# Для operator_area не уводим маршрут на дороги: пропускаем заблокированную внутреннюю точку.
			drone["target_idx"] = idx + 1
			continue
		# Base speed affected by wind (simple model)
		wind = float(STATE.get("weather",{}).get("wind_mps", 3.0))
		speed_mps = max(5.0, 15.0 - 0.3 * wind)
		dist = haversine_m(current, target)
		if dist < speed_mps:
			# Arrive this tick
			drone["pos"] = target
			at_charger = is_at_any_station(target) or (STATE.get("base") and haversine_m(target, tuple(STATE["base"])) < STATION_NEAR_METERS)
			charging_route_status = drone.get("status") in ("return_charge", "return_base", "low_battery")
			# Останавливаемся на промежуточной зарядке только когда дрон явно летит на зарядку.
			# В обычных заказах станции часто являются узлами инфраструктуры, их надо проходить транзитом.
			if at_charger and idx + 1 < len(route) and charging_route_status:
				drone["resume_route"] = list(route[idx + 1:])
				drone["route"] = list(route[: idx + 1])
				drone["target_idx"] = idx + 1
				drone["waypoints_completed"] = drone.get("waypoints_completed", 0) + 1
				assign_to_charger_queue(drone_id)
				drone["status"] = "charging"
			else:
				drone["target_idx"] = idx + 1
				drone["waypoints_completed"] = drone.get("waypoints_completed", 0) + 1
				if drone["target_idx"] >= len(route):
					if drone.get("force_charge_after_route"):
						drone["force_charge_after_route"] = False
						# Завершаем текущий этап area-миссии перед уходом на зарядку,
						# чтобы continuation мог подхватиться другим свободным оператором.
						mark_order_completed_if_any(drone_id)
						drone["status"] = "low_battery"
						maybe_route_to_base_or_station(drone)
						continue
					# Для маршрутов "на зарядку" не завершаем задачу в idle, пока не встанем в очередь зарядки.
					if at_charger or drone.get("status") in ("return_charge", "return_base", "low_battery"):
						if assign_to_charger_queue(drone_id):
							drone["status"] = "charging"
						else:
							# Если конечная точка получилась не рядом со станцией — перепрокладываем.
							maybe_route_to_base_or_station(drone)
					else:
						mark_order_completed_if_any(drone_id)
						if drone.get("status") != "charging":
							drone["status"] = "idle"
				else:
					# Сохраняем статус «на зарядку», если дрон едет к станции
					if drone.get("status") not in ("return_charge", "return_base", "low_battery"):
						drone["status"] = "enroute"
			battery_drain(drone, dist)
		else:
			# Move fractionally and apply basic collision avoidance
			next_pos = move_towards(current, target, speed_mps / dist)
			if will_collide(drone_id, next_pos):
				drone["status"] = "avoidance"
				drone["avoidance_ticks"] = drone.get("avoidance_ticks", 0) + 1
			else:
				drone["pos"] = next_pos
				drone["avoidance_ticks"] = 0
			battery_drain(drone, speed_mps)
		# link quality estimation vs. nearest base or last strong point
		compute_link_quality(drone)
		# low battery: если ещё не едем на зарядку — сохраняем маршрут заказа и строим маршрут до станции
		if drone["battery"] <= FLY_TO_CHARGER_AT_PCT and drone.get("status") not in ("return_charge", "return_base", "low_battery"):
			if drone.get("status") == "enroute":
				_save_route_for_return_if_on_order(drone_id, drone)
			drone["status"] = "low_battery"
			maybe_route_to_base_or_station(drone)
			logger.info("simulate_step: drone %s battery=%.1f%% -> routing to charger (in-route check)", drone_id, drone.get("battery"))

		# Update ETA approximation (seconds) and readable remaining distance
		try:
			remaining = 0.0
			if drone.get("route") and drone.get("target_idx", 0) < len(drone["route"]):
				pos = drone.get("pos")
				tidx = drone.get("target_idx", 0)
				# distance to current target
				remaining += haversine_m(pos, drone["route"][tidx])
				for i in range(tidx, len(drone["route"]) - 1):
					remaining += haversine_m(drone["route"][i], drone["route"][i+1])
			drone["remaining_m"] = remaining
			drone["eta_s"] = int(remaining / max(0.1, speed_mps)) if remaining > 0 else 0
		except Exception:
			pass

	# charging progression & queue handling
	progress_charging()



def _avoidance_step(drone_id: str, drone: Dict[str, Any]) -> None:
	"""Выход из мёртвой блокировки «обход препятствия»: через 2 тика делаем боковой сдвиг."""
	ticks = drone.get("avoidance_ticks", 0) + 1
	drone["avoidance_ticks"] = ticks
	if ticks < 2:
		return
	route = drone.get("route") or []
	idx = drone.get("target_idx", 0)
	if not route or idx >= len(route):
		drone["status"] = "idle"
		drone["avoidance_ticks"] = 0
		return
	current = drone["pos"]
	target = route[idx]
	dist = haversine_m(current, target)
	if dist < 1.0:
		drone["avoidance_ticks"] = 0
		return
	# Не смещаем дрон в искусственную точку вне графа, чтобы не "слетал" с карты.
	# Делаем короткое удержание и продолжаем движение по исходному маршруту.
	drone["status"] = "enroute"
	drone["avoidance_ticks"] = 0


def will_collide(drone_id: str, next_pos: Tuple[float,float]) -> bool:
	# Separation is represented by route layers/echelons in this simulation.
	# A nearby drone must not create a fake obstacle: it caused endless
	# "avoidance" loops when drones met on infrastructure nodes or stations.
	return False


def _is_drone_loaded(drone: Dict[str, Any]) -> bool:
	"""Дрон загружен, если уже проехал не менее loaded_after_waypoint_count точек (едет к точке доставки)."""
	after = drone.get("loaded_after_waypoint_count", 0)
	completed = drone.get("waypoints_completed", 0)
	return completed >= after


def battery_drain(drone: Dict[str, Any], distance_m: float):
	# Single source of truth: RoutingService._get_drone_params. drain в %: distance_m / m_per_pct (без *100)
	p = _routing_service._get_drone_params(drone.get("type", "cargo"))
	loaded = _is_drone_loaded(drone)
	m_per_pct = p["loaded_m_per_pct"] if loaded else p["empty_m_per_pct"]
	drain = distance_m / m_per_pct if m_per_pct > 0 else 0.0
	drone["battery"] = max(0.0, drone["battery"] - drain)
	# temperature/mock telemetry drift
	drone["temp_c"] = float(drone.get("temp_c", 35.0)) + (0.02 * (distance_m/10.0))
	# record history sparsely
	try:
		hist = drone.get("history") or []
		pos = drone.get("pos")
		if pos and (not hist or haversine_m(tuple(hist[-1]), tuple(pos)) > 5.0):
			hist.append(tuple(pos))
			# cap history length
			if len(hist) > 1000:
				hist = hist[-1000:]
			drone["history"] = hist
	except Exception:
		pass


def is_point_in_any_zone(point: Tuple[float, float]) -> bool:
	lat, lon = point
	for z in STATE["no_fly_zones"]:
		lat_min = min(z["lat_min"], z["lat_max"]) ; lat_max = max(z["lat_min"], z["lat_max"])
		lon_min = min(z["lon_min"], z["lon_max"]) ; lon_max = max(z["lon_min"], z["lon_max"])
		if lat_min <= lat <= lat_max and lon_min <= lon <= lon_max:
			return True
	return False

def is_at_any_station(point: Tuple[float, float]) -> bool:
	try:
		for s in STATE.get("stations", []):
			if haversine_m(point, tuple(s)) < STATION_NEAR_METERS:
				return True
		return False
	except Exception:
		return False

def nearest_station_index(point: Tuple[float,float]) -> Optional[int]:
    try:
        best_i = None
        best_d = float('inf')
        for i, s in enumerate(STATE.get("stations", [])):
            d = haversine_m(point, tuple(s))
            if d < best_d:
                best_d = d
                best_i = i
        return best_i
    except Exception:
        return None

def maybe_route_to_base(drone: Dict[str, Any]):
	base = STATE.get("base")
	if not base:
		return
	try:
		battery = max(0.5, float(drone.get("battery", 10.0)))
		reserve = 0.0 if battery <= 15.0 else RESERVE_PCT
		_, coords, _ = plan_route_for(
			tuple(drone.get("pos", base)), tuple(base), drone["type"], battery, reserve_pct=reserve
		)
		if coords:
			drone["route"] = coords
			drone["target_idx"] = 0
			if drone["battery"] <= 5.0:
				drone["status"] = "low_battery"
			else:
				drone["status"] = "return_base"
	except Exception:
		pass

def _save_route_for_return_if_on_order(drone_id: str, drone: Dict[str, Any]) -> None:
	"""Перед уходом на зарядку сохраняем маршрут заказа, чтобы после зарядки вернуть дрон к заданию."""
	if drone.get("saved_route_for_charge") is not None:
		return
	for o in STATE.get("orders", []):
		if o.get("drone_id") == drone_id and o.get("status") in ("assigned", "in_progress", "waiting_continuation"):
			drone["saved_order_id"] = o.get("id")
			drone["active_order_id"] = o.get("id")
			route = drone.get("route") or []
			if route:
				drone["saved_route_for_charge"] = list(route)
				drone["saved_target_idx"] = int(drone.get("target_idx", 0))
			return


def _restore_route_after_charging(drone: Dict[str, Any]) -> bool:
	"""После зарядки восстанавливаем маршрут заказа, если он был сохранён. Возвращает True, если восстановили."""
	# Выход из charging: маршрут восстановлен — статус станет enroute в конце (исполняемое состояние, не «зарядка»).
	if drone.get("saved_route_for_charge") is not None:
		saved = drone.get("saved_route_for_charge") or []
		drone["route"] = list(saved)
		drone["route_segments"] = (
			_route_segments_from_coords(drone["route"], mode="empty", drone_type=str(drone.get("type") or "cargo"))
			if saved
			else []
		)
		sidx = int(drone.get("saved_target_idx", 0))
		drone["target_idx"] = max(0, min(sidx, max(0, len(saved) - 1)))
		drone["status"] = "enroute"
		order_id = drone.pop("saved_order_id", None)
		if order_id:
			drone["active_order_id"] = order_id
		drone.pop("saved_route_for_charge", None)
		drone.pop("saved_target_idx", None)
		logger.info("_restore_route_after_charging: restored route for order %s (%s waypoints from idx %s)", order_id, len(saved), drone.get("target_idx"))
		return True

	# Fallback: если маршрут не сохранился (например, был пустой), пересобираем маршрут по заказу.
	order_id = drone.get("saved_order_id")
	if not order_id:
		return False
	order = next((o for o in STATE.get("orders", []) if o.get("id") == order_id), None)
	if not order or order.get("status") not in ("assigned", "in_progress", "waiting_continuation"):
		# If order is gone or not assigned, don't replan.
		return False

	try:
		required_type = order.get("drone_type") or map_order_to_drone_type(order.get("type", "delivery"))
		pos = tuple(drone.get("pos", (0.0, 0.0)))
		if required_type == "cargo":
			# Для cargo после промежуточной зарядки возвращаемся к текущей фазе заказа:
			# если груз уже "на борту", не летим заново на pickup, а продолжаем к dropoff.
			if _is_drone_loaded(drone):
				G = STATE.get("city_graph")
				charger_nodes = STATE.get("charger_nodes") or {"base": None, "stations": []}
				start_node = _routing_service._find_nearest_node(G, pos) if G else None
				drop_node = _routing_service._find_nearest_node(G, tuple(order["end"])) if G else None
				if G and start_node and drop_node:
					p1, c1, l1, ch1 = _routing_service.plan_with_chargers(
						G, start_node, drop_node, 100.0, MODE_LOADED, "cargo", reserve_pct=PLAN_RESERVE_PCT, charger_nodes=charger_nodes,
						max_segment_battery_pct=MAX_SEGMENT_BATTERY_PCT, max_battery_pct_to_reach_charger=MAX_BATTERY_PCT_TO_REACH_CHARGER,
					)
					if c1:
						bat_after = 100.0 if ch1 else _routing_service.compute_battery_after(l1, 100.0, MODE_LOADED, "cargo")
						exit_coords, exit_len = _best_escape_graph_path(tuple(c1[-1]), bat_after, "cargo")
						full = _to_coord_list(c1)
						full = _concat_coords(full, exit_coords)
						res = {
							"ok": True,
							"coords": full,
							"route_length": float(l1 + exit_len),
							"pickup_waypoint_count": 0,
							"segments": [
								{"type": "to_dropoff", "coords": _to_coord_list(c1), "length": float(l1), "chargers": ch1},
								{"type": "escape", "coords": exit_coords, "length": float(exit_len), "chargers": []},
							],
						}
					else:
						res = plan_order_trip(pos, order["start"], order["end"], "cargo", 100.0)
				else:
					res = plan_order_trip(pos, order["start"], order["end"], "cargo", 100.0)
			else:
				res = plan_order_trip(pos, order["start"], order["end"], "cargo", 100.0)
		elif required_type == "operator" and (order.get("remaining_waypoints") or (order.get("handover_point") and order.get("rest_waypoints"))):
			res = plan_operator_continuation_trip({"pos": pos, "battery": 100.0, "type": "operator"}, order)
		elif required_type == "operator" and order.get("area_polygon"):
			res, _cont = plan_operator_area_trip({"pos": pos, "battery": 100.0, "type": "operator"}, order)
		elif required_type == "operator":
			res = plan_operator_point_trip(pos, order["end"], "operator", 100.0)
		elif required_type == "cleaner":
			res = plan_operator_point_trip(pos, order["end"], "cleaner", 100.0)
		else:
			res = None
	except Exception:
		logger.exception("_restore_route_after_charging: replanning failed")
		return False

	if not res or not res.get("ok") or not res.get("coords"):
		rt = order.get("drone_type") or map_order_to_drone_type(order.get("type", "delivery"))
		if rt == "operator" and order.get("area_polygon") and not (order.get("remaining_waypoints") or []):
			logger.warning(
				"_restore_route_after_charging: replan empty for operator area order %s (no remaining waypoints)",
				order.get("id"),
			)
		return False

	res = _apply_no_fly_detours_to_result(res, str(drone.get("type") or order.get("drone_type") or "cargo"))
	drone["route"] = list(res["coords"])
	drone["route_segments"] = list(
		res.get("route_segments")
		or _route_segments_from_coords(drone["route"], mode="empty", drone_type=str(drone.get("type") or "cargo"))
	)
	if res.get("route_diagnostics"):
		drone["route_diagnostics"] = dict(res["route_diagnostics"])
	else:
		drone.pop("route_diagnostics", None)
	drone["resume_route_segments"] = []
	drone["target_idx"] = 0
	# После восстановления миссии обязательно возвращаем дрона в исполняемый статус.
	drone["status"] = "enroute"
	drone["active_order_id"] = order_id
	drone.pop("saved_order_id", None)
	logger.info("_restore_route_after_charging: replanned route for order %s (%s waypoints)", order_id, len(drone["route"]))
	return True


def _apply_replanned_route(drone_id: str, drone: Dict[str, Any], order: Dict[str, Any], res: Dict[str, Any]) -> bool:
	if not res or not res.get("ok") or not res.get("coords"):
		return False
	res = _apply_no_fly_detours_to_result(res, str(drone.get("type") or order.get("drone_type") or "cargo"))
	coords = _to_coord_list(res.get("coords") or [])
	if not coords:
		return False
	drone["route"] = coords
	drone["route_segments"] = list(
		res.get("route_segments")
		or _route_segments_from_coords(coords, mode="empty", drone_type=str(drone.get("type") or "cargo"))
	)
	drone["resume_route"] = []
	drone["resume_route_segments"] = []
	cur = drone.get("pos")
	start_idx = 0
	try:
		if cur and len(coords) > 1 and haversine_m(tuple(cur), tuple(coords[0])) < 3.0:
			start_idx = 1
	except Exception:
		start_idx = 0
	drone["target_idx"] = start_idx
	drone["status"] = "enroute"
	drone["active_order_id"] = order.get("id")
	if res.get("route_diagnostics"):
		drone["route_diagnostics"] = dict(res["route_diagnostics"])
	else:
		drone.pop("route_diagnostics", None)
	return True


def _replan_active_route_for_zones(drone_id: str, drone: Dict[str, Any]) -> bool:
	order = _get_active_order_for_drone(drone_id)
	if not order:
		return False
	try:
		required_type = order.get("drone_type") or map_order_to_drone_type(order.get("type", "delivery"))
		pos = tuple(drone.get("pos", (0.0, 0.0)))
		battery = float(drone.get("battery", 100.0))
		if required_type == "cargo":
			if _is_drone_loaded(drone):
				res = plan_order_trip(pos, pos, tuple(order["end"]), "cargo", battery)
				if res and res.get("ok"):
					res["pickup_waypoint_count"] = 0
			else:
				res = plan_order_trip(pos, tuple(order["start"]), tuple(order["end"]), "cargo", battery)
		elif required_type == "operator" and (order.get("remaining_waypoints") or (order.get("handover_point") and order.get("rest_waypoints"))):
			res = plan_operator_continuation_trip({"pos": pos, "battery": battery, "type": "operator"}, order)
		elif required_type == "operator" and order.get("area_polygon"):
			res, _cont = plan_operator_area_trip({"pos": pos, "battery": battery, "type": "operator"}, order)
		elif required_type == "operator":
			res = plan_operator_point_trip(pos, tuple(order["end"]), "operator", battery)
		elif required_type == "cleaner":
			res = plan_operator_point_trip(pos, tuple(order["end"]), "cleaner", battery)
		else:
			return False
	except Exception:
		logger.exception("_replan_active_route_for_zones: failed drone=%s", drone_id)
		return False
	if _apply_replanned_route(drone_id, drone, order, res):
		drone["_zone_version_seen"] = STATE.get("zone_version", 0)
		logger.info("_replan_active_route_for_zones: drone=%s order=%s route_len=%.1f", drone_id, order.get("id"), float(res.get("route_length", 0.0)))
		return True
	return False


def _has_assigned_order_for_drone(drone_id: str) -> bool:
	for o in STATE.get("orders", []):
		if o.get("drone_id") == drone_id and o.get("status") in ("assigned", "in_progress", "waiting_continuation"):
			return True
	return False


def _resume_after_charge_or_hold(drone_id: str, drone: Dict[str, Any]) -> None:
	# Дрон выходит из зарядки: сначала убираем из очередей (иначе UI и логика видят «в зарядке» при battery=100).
	_remove_drone_from_charge_queues(drone_id)
	# Восстановление маршрута после промежуточной зарядки:
	# 1) сначала заранее сохранённый хвост маршрута,
	# 2) затем пересборка по сохранённому order_id,
	# 3) если задание ещё назначено, не уходим в idle до явного завершения.
	resume = drone.get("resume_route") or []
	if resume:
		drone["route"] = list(resume)
		drone["resume_route"] = []
		seg_resume = drone.get("resume_route_segments") or []
		drone["route_segments"] = (
			list(seg_resume)
			if seg_resume
			else _route_segments_from_coords(drone["route"], mode="empty", drone_type=str(drone.get("type") or "cargo"))
		)
		drone["resume_route_segments"] = []
		drone["target_idx"] = 0
		# После зарядки при восстановленном маршруте переводим в активный исполняемый статус.
		drone["status"] = "enroute"
		if drone.get("saved_order_id"):
			drone["active_order_id"] = drone.get("saved_order_id")
		return
	if _restore_route_after_charging(drone):
		return
	# Нет resume_route и не удалось replan: operator area без хвоста — закрываем order после зарядки.
	if _try_close_order_if_no_flight_after_charge(drone_id, drone):
		if not _get_active_order_for_drone(drone_id):
			drone["status"] = "idle"
		return
	if _has_assigned_order_for_drone(drone_id):
		# Есть активный заказ, но route пока не восстановлен — удерживаем, не уходим в idle.
		drone["status"] = "holding"
		return
	drone.pop("active_order_id", None)
	drone["status"] = "idle"


def maybe_route_to_base_or_station(drone: Dict[str, Any]):
	"""Строит маршрут до ближайшей зарядки. При низком заряде — через другие станции (plan_with_chargers)."""
	chargers: List[Tuple[float, float]] = []
	if STATE.get("base"):
		chargers.append(tuple(STATE["base"]))
	chargers += [tuple(s) for s in STATE.get("stations", []) if isinstance(s, (list, tuple)) and len(s) == 2]
	if not chargers:
		return
	pos = tuple(drone.get("pos", chargers[0]))
	best = None
	bestd = float("inf")
	for c in chargers:
		d = haversine_m(pos, c)
		if d < bestd:
			bestd = d
			best = c
	if not best:
		return
	battery = max(0.5, float(drone.get("battery", 10.0)))
	reserve = 0.0 if battery <= FLY_TO_CHARGER_AT_PCT else RESERVE_PCT
	_, coords, _ = plan_route_for(pos, best, drone["type"], battery, reserve_pct=reserve)
	# Если прямой путь недостижим — строим маршрут через станции зарядки (plan_with_chargers)
	if not coords:
		G = get_active_routing_graph()
		charger_nodes = limited_charger_nodes(pos)
		if G and charger_nodes.get("base") is not None or (charger_nodes.get("stations") or []):
			start_node = _routing_service._find_nearest_node(G, pos)
			goal_node = None
			base_coords = STATE.get("base")
			if base_coords and tuple(base_coords) == best:
				goal_node = charger_nodes.get("base")
			else:
				for i, s in enumerate(STATE.get("stations") or []):
					if isinstance(s, (list, tuple)) and len(s) == 2 and tuple(s) == best:
						st = charger_nodes.get("stations") or []
						if i < len(st):
							goal_node = st[i]
						break
			if start_node and goal_node and goal_node in G.nodes:
				path_p, coords_c, _len, _ch = _routing_service.plan_with_chargers(
					G, start_node, goal_node, battery, MODE_EMPTY, drone["type"], reserve_pct=reserve,
					charger_nodes=charger_nodes, max_segment_battery_pct=MAX_SEGMENT_BATTERY_PCT,
					max_battery_pct_to_reach_charger=MAX_BATTERY_PCT_TO_REACH_CHARGER,
				)
				if path_p and coords_c:
					coords = coords_c
					logger.info("maybe_route_to_base_or_station: route to charger via plan_with_chargers (battery=%.1f%%)", battery)
		if not coords and G and best:
			max_range = _routing_service.max_reachable_distance(battery, MODE_EMPTY, drone["type"], reserve_pct=reserve)
			_, coords, _ = _routing_service.plan_direct_path(G, pos, best, max_range * 2.0)
	if coords:
		drone["route"] = coords
		drone["target_idx"] = 0
		drone["status"] = "return_charge"

def assign_to_charger_queue(drone_id: str) -> bool:
    d = STATE["drones"].get(drone_id)
    if not d:
        return False
    pos = tuple(d.get("pos", (0,0)))
    # База: зарядка на месте (как раньше)
    base_close = STATE.get("base") and haversine_m(pos, tuple(STATE["base"])) < STATION_NEAR_METERS
    if base_close:
        q = STATE.get("base_queue") or {"charging": [], "queue": [], "capacity": 2}
        if drone_id not in q["charging"] and drone_id not in q["queue"]:
            if len(q["charging"]) < q.get("capacity", 2):
                q["charging"].append(drone_id)
            else:
                q["queue"].append(drone_id)
        STATE["base_queue"] = q
        return True
    # Станции зарядки: смена аккумулятора (запас 20 заряженных)
    idx = nearest_station_index(pos)
    if idx is None:
        return False
    # Ставим в очередь только если дрон действительно рядом со станцией.
    station_pos = None
    stations = STATE.get("stations") or []
    if 0 <= idx < len(stations):
        station_pos = tuple(stations[idx])
    if not station_pos or haversine_m(pos, station_pos) >= STATION_NEAR_METERS:
        return False
    key = str(idx)
    sq = (STATE.get("station_queues") or {}).get(key)
    if not sq:
        sq = {"charged_batteries": STATION_CHARGED_BATTERIES_MAX, "charging_queue": [], "queue": []}
    if "charged_batteries" in sq:
        if drone_id in sq.get("queue", []):
            return True
        if sq.get("charged_batteries", 0) > 0:
            sq["charged_batteries"] -= 1
            sq.setdefault("charging_queue", []).append(STATION_BATTERY_CHARGE_TICKS)
            d["battery"] = 100.0
            _resume_after_charge_or_hold(drone_id, d)
        else:
            sq.setdefault("queue", []).append(drone_id)
            d["status"] = "charging"
    else:
        if drone_id not in sq.get("charging", []) and drone_id not in sq.get("queue", []):
            if len(sq.get("charging", [])) < sq.get("capacity", 2):
                sq.setdefault("charging", []).append(drone_id)
            else:
                sq.setdefault("queue", []).append(drone_id)
    STATE.setdefault("station_queues", {})[key] = sq
    return True

def progress_charging():
    # base
    bq = STATE.get("base_queue") or {"charging": [], "queue": [], "capacity": 2}
    done = []
    for did in list(bq.get("charging", [])):
        d = STATE["drones"].get(did)
        if not d:
            continue
        d["battery"] = min(100.0, float(d.get("battery", 0.0)) + 4.0)  # 4% per tick
        if d["battery"] >= CHARGE_COMPLETE_PCT:
            d["battery"] = 100.0
            done.append(did)
            _resume_after_charge_or_hold(did, d)
    for did in done:
        if did in bq["charging"]:
            bq["charging"].remove(did)
    # promote from queue
    while len(bq["charging"]) < bq.get("capacity", 2) and bq["queue"]:
        bq["charging"].append(bq["queue"].pop(0))
    STATE["base_queue"] = bq
    # Станции зарядки: смена аккумуляторов (тик зарядки в очереди, затем выдача ожидающим)
    sqs = STATE.get("station_queues") or {}
    for key, sq in sqs.items():
        if "charged_batteries" in sq:
            cq = sq.get("charging_queue") or []
            new_cq = []
            for t in cq:
                t -= 1
                if t <= 0:
                    sq["charged_batteries"] = min(STATION_CHARGED_BATTERIES_MAX, sq.get("charged_batteries", 0) + 1)
                else:
                    new_cq.append(t)
            sq["charging_queue"] = new_cq
            served = []
            for did in list(sq.get("queue", [])):
                if sq.get("charged_batteries", 0) <= 0:
                    break
                d = STATE["drones"].get(did)
                if not d:
                    served.append(did)
                    continue
                sq["charged_batteries"] -= 1
                sq.setdefault("charging_queue", []).append(STATION_BATTERY_CHARGE_TICKS)
                d["battery"] = 100.0
                _resume_after_charge_or_hold(did, d)
                served.append(did)
            for did in served:
                if did in sq.get("queue", []):
                    sq["queue"].remove(did)
            # Пока нет готовых аккумуляторов — дроны в очереди заряжаются постепенно (как на базе), чтобы не застревать на 19%
            for did in list(sq.get("queue", [])):
                d = STATE["drones"].get(did)
                if not d:
                    continue
                d["battery"] = min(100.0, float(d.get("battery", 0.0)) + 4.0)
                if d["battery"] >= CHARGE_COMPLETE_PCT:
                    d["battery"] = 100.0
                    if did in sq.get("queue", []):
                        sq["queue"].remove(did)
                    _resume_after_charge_or_hold(did, d)
        else:
            done = []
            for did in list(sq.get("charging", [])):
                d = STATE["drones"].get(did)
                if not d:
                    continue
                d["battery"] = min(100.0, float(d.get("battery", 0.0)) + 4.0)
                if d["battery"] >= CHARGE_COMPLETE_PCT:
                    d["battery"] = 100.0
                    done.append(did)
                    _resume_after_charge_or_hold(did, d)
            for did in done:
                if did in sq.get("charging", []):
                    sq["charging"].remove(did)
            while len(sq.get("charging", [])) < sq.get("capacity", 2) and sq.get("queue", []):
                sq["charging"].append(sq["queue"].pop(0))
        sqs[key] = sq
    STATE["station_queues"] = sqs
    # Нормализация: battery >= порога, status charging — недопустимо оставаться в charging (очередь/статус/маршрут).
    for _did, _d in list((STATE.get("drones") or {}).items()):
        _force_exit_charging_if_complete(_did, _d)

def _area_chain_has_pending(root_order_id: str, exclude_order_id: Optional[str] = None) -> bool:
	for o in STATE.get("orders", []):
		if o.get("id") == exclude_order_id:
			continue
		if _operator_area_root_order_id(o) != root_order_id:
			continue
		if o.get("status") in ("queued", "assigned", "in_progress", "waiting_continuation"):
			return True
	return False


def _mark_area_root_completed_if_ready(root_order_id: str) -> None:
	root = next((o for o in STATE.get("orders", []) if o.get("id") == root_order_id), None)
	if not root:
		return
	if _area_chain_has_pending(root_order_id, exclude_order_id=root_order_id):
		root["status"] = "in_progress"
		return
	remaining = root.get("remaining_waypoints") or []
	if remaining:
		root["status"] = "in_progress"
		return
	root["status"] = "completed"
	root.pop("drone_id", None)


def mark_order_completed_if_any(drone_id: str):
	# Финализация заказа: выполняем только при реальном окончании миссии, не при промежуточной зарядке.
	drone = STATE.get("drones", {}).get(drone_id)
	if not drone:
		return
	route = drone.get("route") or []
	if route and int(drone.get("target_idx", 0)) < len(route):
		return
	# Во время зарядки не завершаем заказ (промежуточная остановка), кроме случая «зарядка завершена»:
	# battery >= порога и дрон снимаем с charging-очередей — иначе in_progress зависает навсегда.
	if drone.get("status") == "charging":
		if float(drone.get("battery", 0.0)) < CHARGE_COMPLETE_PCT:
			return
		_remove_drone_from_charge_queues(drone_id)
	elif drone.get("status") in ("return_charge", "return_base", "low_battery"):
		return

	order = _get_active_order_for_drone(drone_id)
	if not order:
		if drone.get("status") == "charging" and float(drone.get("battery", 0.0)) >= CHARGE_COMPLETE_PCT:
			drone["status"] = "idle"
		return

	required_type = order.get("drone_type") or map_order_to_drone_type(order.get("type", "delivery"))
	if required_type == "operator" and (order.get("area_polygon") or order.get("remaining_waypoints")):
		# area mission completed только при полном исчерпании remaining_waypoints и отсутствии continuation.
		root_id = _operator_area_root_order_id(order)
		rem = order.get("remaining_waypoints") or []
		# После зарядки (battery полная) пустой route при ненулевом remaining — не handover, а восстановление миссии.
		if rem and float(drone.get("battery", 0.0)) >= CHARGE_COMPLETE_PCT and not (drone.get("route") or []):
			if _restore_route_after_charging(drone):
				return
			drone["status"] = "holding"
			return
		if order.get("has_continuation") or rem:
			order["status"] = "waiting_continuation"
			order.pop("drone_id", None)
		else:
			order["status"] = "completed"
			order.pop("drone_id", None)
		_mark_area_root_completed_if_ready(root_id)
	else:
		# cargo/operator-point/cleaner завершаем только в конце основного маршрута миссии.
		order["status"] = "completed"
		order.pop("drone_id", None)

	# Чистим активный контекст дрона после завершения/передачи.
	drone.pop("active_order_id", None)
	drone.pop("mission_mode", None)
	drone["force_charge_after_route"] = False
	# После завершения/передачи заказа дрон не должен оставаться в «зарядке» или holding без заказа.
	if not _get_active_order_for_drone(drone_id) and drone.get("status") in ("charging", "holding"):
		drone["status"] = "idle"

	# Доп. защита: закрываем возможные дубликаты этого же order id в активных статусах.
	for o in STATE.get("orders", []):
		if o.get("id") != order.get("id"):
			continue
		if o is order:
			continue
		if o.get("status") in ("assigned", "in_progress", "waiting_continuation") and o.get("drone_id") == drone_id:
			o["status"] = "cancelled"
			o.pop("drone_id", None)

# Telemetry helpers
def compute_link_quality(drone: Dict[str, Any]):
    base = STATE.get("base")
    if not base:
        drone["link_quality"] = 0.7  # default medium
        return
    try:
        d = haversine_m(tuple(drone.get("pos", base)), tuple(base))
        # simple path loss model: quality 1.0 within 300m, decays to 0.1 at 5km, never below 0.05
        if d <= 300:
            q = 1.0
        elif d >= 5000:
            q = 0.1
        else:
            q = max(0.05, 1.0 - (d-300)/(5000-300))
        # adjust for wind
        wind = float(STATE.get("weather",{}).get("wind_mps", 3.0))
        q *= max(0.4, 1.0 - wind*0.02)
        drone["link_quality"] = round(float(q), 2)
    except Exception:
        drone["link_quality"] = 0.5

def ensure_base_drones():
	"""
	Bootstrap drones for simulation/UI.
	- Preferred: place drones on charging stations from infrastructure with capacity-driven distribution.
	- Fallback: legacy behaviour — spawn from base inventory.
	"""
	infra = STATE.get("infrastructure") or {}
	charging = (infra.get("charging_stations") or []) if isinstance(infra, dict) else []
	drones = STATE.get("drones") or {}
	# If drones exist but they were spawned from base inventory, replace them once infra stations are available.
	if drones and charging:
		all_idle = all((d or {}).get("status") in (None, "idle") for d in drones.values())
		all_from_base = all((d or {}).get("home_station_id") == "base" for d in drones.values())
		if all_idle and all_from_base:
			STATE["drones"] = {}
			drones = {}
		else:
			return
	# If there are already drones (non-base), do not auto-spawn duplicates
	if drones:
		return

	def _split_capacity(total: int) -> Tuple[int, int, int]:
		t = max(0, int(total))
		cargo = int(round(t * 0.75))
		operator = int(round(t * 0.15))
		cleaner = t - cargo - operator
		if cleaner < 0:
			deficit = -cleaner
			cut_cargo = min(deficit, cargo)
			cargo -= cut_cargo
			deficit -= cut_cargo
			if deficit > 0:
				cut_operator = min(deficit, operator)
				operator -= cut_operator
				deficit -= cut_operator
			cleaner = 0
		return cargo, operator, cleaner

	# Infrastructure-driven spawn (charging stations)
	if charging:
		for st in charging:
			try:
				pos = st.get("pos")
				if not (isinstance(pos, (list, tuple)) and len(pos) == 2):
					continue
				st_id = st.get("id")
				# Prefer explicit precomputed totals if present
				t_total = st.get("cluster_drones_total")
				t_cargo = st.get("cluster_drones_cargo")
				t_op = st.get("cluster_drones_monitoring")
				t_srv = st.get("cluster_drones_service")

				if t_total is not None and t_cargo is not None and t_op is not None and t_srv is not None:
					n_cargo = max(0, int(round(float(t_cargo))))
					n_operator = max(0, int(round(float(t_op))))
					n_cleaner = max(0, int(round(float(t_srv))))
				else:
					cap = st.get("capacity")
					try:
						cap_i = int(round(float(cap))) if cap is not None else 2
					except Exception:
						cap_i = 2
					n_cargo, n_operator, n_cleaner = _split_capacity(cap_i)

				for _ in range(n_cargo):
					spawn_drone("cargo", pos=tuple(pos), battery=100.0, home_station_id=st_id, current_station_id=st_id, home_station_type=st.get("station_type"), current_station_type=st.get("station_type"))
				for _ in range(n_operator):
					spawn_drone("operator", pos=tuple(pos), battery=100.0, home_station_id=st_id, current_station_id=st_id, home_station_type=st.get("station_type"), current_station_type=st.get("station_type"))
				for _ in range(n_cleaner):
					spawn_drone("cleaner", pos=tuple(pos), battery=100.0, home_station_id=st_id, current_station_id=st_id, home_station_type=st.get("station_type"), current_station_type=st.get("station_type"))
			except Exception:
				continue
		return

	# Legacy fallback: base + inventory
	base = STATE.get("base")
	inv = STATE.get("inventory") or {}
	if not base or not isinstance(inv, dict):
		return
	total = sum(max(0, int(v)) for v in inv.values())
	if total <= 0:
		return
	for typ, cnt in inv.items():
		try:
			n = max(0, int(cnt))
		except Exception:
			n = 0
		for _ in range(n):
			spawn_drone(typ, pos=tuple(base), battery=100.0, home_station_id="base", current_station_id="base")

def _next_drone_id(drone_type: str) -> str:
    """Генерирует имя дрона по типу: грузовой1, операторский1, мойщик1 и т.д."""
    type_names = {"cargo": "грузовой", "operator": "операторский", "cleaner": "мойщик"}
    name_ru = type_names.get(drone_type, "дрон")
    numbers = []
    for k in STATE.get("drones", {}):
        if k == name_ru or k.startswith(name_ru):
            suf = k[len(name_ru):].strip()
            if suf.isdigit():
                numbers.append(int(suf))
    next_num = max(numbers, default=0) + 1
    return f"{name_ru}{next_num}"


def spawn_drone(
	drone_type: str,
	pos: Tuple[float, float],
	battery: float = 100.0,
	home_station_id: Optional[str] = None,
	current_station_id: Optional[str] = None,
	home_station_type: Optional[str] = None,
	current_station_type: Optional[str] = None,
) -> str:
	did = _next_drone_id(drone_type)
	STATE["drones"][did] = {
		"pos": pos,
		"type": drone_type,
		"battery": float(battery),
		"route": [],
		"target_idx": 0,
		"status": "idle",
		"home_station_id": home_station_id,
		"current_station_id": current_station_id,
		"home_station_type": home_station_type,
		"current_station_type": current_station_type,
		"current_echelon": None,
	}
	return did

def spawn_drone_from_inventory(pref_type: str, order_id: Optional[str] = None, caller: str = "unknown") -> Optional[str]:
    base = STATE.get("base")
    inv = STATE.get("inventory") or {}
    if not base:
        return None
    # Запрещаем спавн без реального запаса inventory нужного типа.
    cnt_raw = inv.get(pref_type, 0)
    try:
        cnt = int(cnt_raw)
    except Exception:
        cnt = 0
    if cnt <= 0:
        logger.info(
            "spawn_drone_from_inventory: blocked type=%s order=%s caller=%s inventory_before=%s",
            pref_type, order_id, caller, cnt_raw,
        )
        return None
    before = cnt
    did = spawn_drone(pref_type, pos=tuple(base), battery=100.0)
    inv[pref_type] = cnt - 1
    STATE["inventory"] = inv
    logger.info(
        "spawn_drone_from_inventory: spawned drone=%s type=%s order=%s caller=%s inventory_before=%s inventory_after=%s",
        did, pref_type, order_id, caller, before, inv.get(pref_type, 0),
    )
    return did

# Assign a full route. Charging is handled by simulate_step only at the final charger
# or when the drone is explicitly routed to charge because of low battery.
def apply_midroute_charging(
	drone: Dict[str, Any],
	full_coords: List[Tuple[float, float]],
	full_segments: Optional[List[Dict[str, Any]]] = None,
):
    try:
        _dt_seg = str(drone.get("type") or "cargo")
        if not isinstance(full_coords, list) or len(full_coords) < 2:
            drone["route"] = list(full_coords or [])
            drone["resume_route"] = []
            drone["route_segments"] = []
            drone["resume_route_segments"] = []
            drone["target_idx"] = 0
            drone["status"] = "enroute" if full_coords else "idle"
            return
        drone["route"] = list(full_coords)
        drone["resume_route"] = []
        drone["route_segments"] = (
            list(full_segments)
            if full_segments
            else _route_segments_from_coords(drone["route"], mode="empty", drone_type=_dt_seg)
        )
        drone["resume_route_segments"] = []
        cur = drone.get("pos")
        start_idx = 0
        try:
            if cur and len(drone["route"]) > 1 and haversine_m(tuple(cur), tuple(drone["route"][0])) < 3.0:
                start_idx = 1
        except Exception:
            start_idx = 0
        drone["target_idx"] = start_idx
        drone["status"] = "enroute"
    except Exception:
        drone["route"] = list(full_coords or [])
        drone["resume_route"] = []
        drone["route_segments"] = (
            _route_segments_from_coords(
                drone["route"], mode="empty", drone_type=str(drone.get("type") or "cargo")
            )
            if drone.get("route")
            else []
        )
        drone["resume_route_segments"] = []
        drone["target_idx"] = 0
        drone["status"] = "enroute" if full_coords else "idle"

# Persistence
async def persist_state():
    if not _redis:
        return
    try:
        data = {
            "city": STATE.get("city"),
            # Orders are runtime-only. Persisting them causes old queued jobs to
            # resurrect after restart and spam routing warnings.
            "orders": [],
            "drones": STATE.get("drones", {}),
            "no_fly_zones": STATE.get("no_fly_zones", []),
            "weather": STATE.get("weather", {}),
            "base": STATE.get("base"),
            "stations": STATE.get("stations", []),
            "station_queues": STATE.get("station_queues", {}),
            "battery_mode": STATE.get("battery_mode", "reality"),
            "drone_type": STATE.get("drone_type"),
            "ts": datetime.utcnow().isoformat(),
        }
        _redis.set("drone_planner:state", json.dumps(data))
    except Exception:
        logger.exception("persist_state failed")

async def restore_state():
    if not _redis:
        return
    try:
        raw = _redis.get("drone_planner:state")
        if not raw:
            return
        data = json.loads(raw)
        STATE["city"] = data.get("city")
        # Do not restore orders from Redis; they are intentionally runtime-only.
        STATE["orders"] = []
        STATE["drones"] = data.get("drones", {})
        STATE["no_fly_zones"] = data.get("no_fly_zones", [])
        STATE["weather"] = data.get("weather", {"wind_mps": 3.0})
        STATE["base"] = tuple(data.get("base")) if data.get("base") else None
        STATE["stations"] = data.get("stations", [])
        if data.get("station_queues"):
            sq = data["station_queues"]
            for k, v in list(sq.items()):
                if isinstance(v, dict) and "charged_batteries" not in v:
                    sq[k] = {"charged_batteries": STATION_CHARGED_BATTERIES_MAX, "charging_queue": [], "queue": []}
            STATE["station_queues"] = sq
        elif STATE["stations"]:
            STATE["station_queues"] = {
                str(i): {"charged_batteries": STATION_CHARGED_BATTERIES_MAX, "charging_queue": [], "queue": []}
                for i in range(len(STATE["stations"]))
            }
        if data.get("battery_mode") in ("reality", "test"):
            STATE["battery_mode"] = data["battery_mode"]
            _routing_service.set_battery_mode(data["battery_mode"])
        if data.get("drone_type") in ("cargo", "operator", "cleaner"):
            STATE["drone_type"] = data["drone_type"]
        if STATE["city"]:
            try:
                await rebuild_graph_with_zones()
            except Exception:
                logger.exception("Failed to rebuild graph on restore")
        refresh_charger_nodes()
    except Exception:
        logger.exception("restore_state failed")


def move_towards(a: Tuple[float, float], b: Tuple[float, float], frac: float) -> Tuple[float, float]:
	frac = max(0.0, min(1.0, frac))
	return (a[0] + (b[0]-a[0]) * frac, a[1] + (b[1]-a[1]) * frac)


def haversine_m(a: Tuple[float, float], b: Tuple[float, float]) -> float:
	R = 6371000.0
	lat1 = math.radians(a[0]) ; lat2 = math.radians(b[0])
	dlat = lat2 - lat1
	dlon = math.radians(b[1] - a[1])
	sa = math.sin(dlat/2.0)**2 + math.cos(lat1)*math.cos(lat2)*math.sin(dlon/2.0)**2
	c = 2.0 * math.atan2(math.sqrt(sa), math.sqrt(1.0-sa))
	return R * c


def normalize_latlon(coord: Any, *, ctx: str = "") -> Optional[Tuple[float, float]]:
	"""
	Normalize coordinate to (lat, lon) for STATE/orders/drones/routes.
	GeoJSON remains (lon, lat) elsewhere.
	"""
	try:
		if coord is None:
			return None
		if isinstance(coord, (list, tuple)) and len(coord) == 2:
			lat, lon = float(coord[0]), float(coord[1])
		else:
			return None
		city = (STATE.get("city") or "").lower()
		if "volgograd" in city or "волгоград" in city:
			c0 = (48.7080, 44.5133)
			d1 = haversine_m((lat, lon), c0)
			d2 = haversine_m((lon, lat), c0)
			if d2 + 10.0 < d1:
				logger.warning("normalize_latlon: swapped for Volgograd ctx=%s in=%s out=%s", ctx, (lat, lon), (lon, lat))
				return (lon, lat)
		if abs(lat) > 90 and abs(lon) <= 90:
			logger.warning("normalize_latlon: lat out of range ctx=%s in=%s out=%s", ctx, (lat, lon), (lon, lat))
			return (lon, lat)
		return (lat, lon)
	except Exception:
		return None

# Run helper for uvicorn: python -m uvicorn api_server:app --reload
