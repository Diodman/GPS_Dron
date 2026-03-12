from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import asyncio
import logging
from typing import Dict, List, Any, Optional, Tuple
from pydantic import BaseModel
import math
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, Response
import os
import base64
import json
from datetime import datetime
from redis import Redis

from data_service import DataService
from graph_service import GraphService
from routing_service import RoutingService, MODE_EMPTY, MODE_LOADED

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
# Скорость по умолчанию для оценки ETA (м/с), если ветер неизвестен
DEFAULT_SPEED_MPS = 12.0

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
app.mount("/static", StaticFiles(directory="static"), name="static")

# Минимальная 1×1 PNG (прозрачный пиксель) — чтобы не было 404 на /favicon.ico
FAVICON_PNG = base64.b64decode(
	"iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8z8BQDwAEhQGAhKmMIQAAAABJRU5ErkJggg=="
)

@app.get("/favicon.ico", include_in_schema=False)
async def favicon():
	return Response(content=FAVICON_PNG, media_type="image/png")

# Services
_data_service = DataService()
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
}

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
	while True:
		try:
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

@app.on_event("startup")
async def on_startup():
	await restore_state()
	# initialize drones at base from inventory if needed
	try:
		ensure_base_drones()
	except Exception:
		logger.exception("ensure_base_drones on startup failed")
	asyncio.create_task(broadcaster_loop())
	asyncio.create_task(scheduler_loop())

@app.on_event("shutdown")
async def on_shutdown():
	await persist_state()

# API endpoints
@app.post("/api/load_city")
async def load_city(body: LoadCityRequest):
	try:
		city_data = _data_service.get_city_data(body.city)
		# inject current API no-fly zones into data prior to build
		city_data['no_fly_zones'] = list(STATE["no_fly_zones"]) or city_data.get('no_fly_zones', [])
		city_graph = _graph_service.build_city_graph(city_data, body.drone_type)
		_routing_service.city_graphs[body.city] = city_graph
		STATE["city"] = body.city
		STATE["city_graph"] = city_graph
		STATE["drone_type"] = body.drone_type
		refresh_charger_nodes()
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
	}

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
                d["route"] = []
                d["status"] = "idle"
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
            o["end"] = new_end
            # if assigned, replan
            did = o.get("drone_id")
            if did and did in STATE["drones"]:
                d = STATE["drones"][did]
                _, coords, _ = plan_route_for(d.get("pos", o["start"]), new_end, d["type"], d["battery"])
                if coords:
                    apply_midroute_charging(d, coords)
            await persist_state()
            return {"ok": True, "order": o}
    return JSONResponse(status_code=404, content={"ok": False, "error": "Order not found"})

@app.get("/")
async def root_page():
	try:
		with open("static/index.html", "r", encoding="utf-8") as f:
			return HTMLResponse(f.read())
	except Exception:
		return HTMLResponse("<h3>Client not found. Ensure static/index.html exists.</h3>", status_code=404)

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
	logger.debug("charger_nodes refreshed: base=%s stations=%s", cn["base"], cn["stations"])


# Utilities
async def resolve_point(coords: Optional[List[float]], address: Optional[str]):
	if coords and len(coords) == 2:
		return tuple(coords)
	if address:
		try:
			city = STATE["city"]
			return _data_service.address_to_coords(address, city)
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


# Шаг сетки облёта внутри зоны (м). Меньше = плотнее маршрут.
OPERATOR_AREA_GRID_STEP_M = 80.0


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


def _operator_area_waypoints(order: Dict[str, Any]) -> List[Tuple[float, float]]:
	"""Точки облёта области: облёт внутри выделенной зоны (сетка лаунмower), не только контур."""
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
		wp = _lawnmower_waypoints_inside_polygon(polygon, OPERATOR_AREA_GRID_STEP_M)
	if not wp:
		# Fallback: только вершины контура (как раньше)
		wp = list(polygon) if polygon else []
	return wp


def plan_operator_area_trip(
	drone: Dict[str, Any], order: Dict[str, Any]
) -> Tuple[Dict[str, Any], Optional[Dict[str, Any]]]:
	"""
	Планирование облёта области операторским дроном. Если на одном аккумуляторе не облететь,
	возвращает маршрут до точки «передачи» и заказ-продолжение для второго дрона.
	Возвращает (result, continuation_order). result как у plan_order_trip; continuation_order или None.
	"""
	G = STATE.get("city_graph")
	city = STATE.get("city")
	if not city or G is None:
		return ({"ok": False, "reason": "no graph", "details": "no city graph"}, None)
	_routing_service.set_battery_mode(STATE.get("battery_mode", "reality"))
	charger_nodes = STATE.get("charger_nodes") or {"base": None, "stations": []}
	waypoints = _operator_area_waypoints(order)
	if not waypoints:
		return ({"ok": False, "reason": "no area", "details": "no area_polygon or bounds"}, None)
	drone_type = "operator"
	battery_pct = float(drone.get("battery", 100.0))
	pos = tuple(drone.get("pos", order.get("start", (0, 0))))
	# Запас для оператора после облёта (как 20% к точке зарядки)
	OPERATOR_RESERVE_PCT = 20.0
	max_range = _routing_service.max_reachable_distance(
		battery_pct, MODE_EMPTY, drone_type, reserve_pct=OPERATOR_RESERVE_PCT
	)
	# Приблизительная длина: pos -> wp0 -> wp1 -> ... -> wpN
	approx_lengths = []
	prev = pos
	for wp in waypoints:
		approx_lengths.append(haversine_m(prev, tuple(wp)))
		prev = tuple(wp)
	# до ближайшей зарядки от последней точки
	base = STATE.get("base")
	stations = STATE.get("stations") or []
	chargers = ([tuple(base)] if base else []) + [tuple(s) for s in stations if isinstance(s, (list, tuple)) and len(s) == 2]
	escape_dist = min(haversine_m(prev, c) for c in chargers) if chargers else 0.0
	total_approx = sum(approx_lengths) + escape_dist
	# Если не хватает заряда (остаток <20%) — находим точку передачи; 80% max_range на облёт, 20% резерв до зарядки
	continuation_order = None
	handover_idx = None
	if total_approx > max_range and len(waypoints) >= 2:
		cum = 0.0
		for i in range(len(waypoints)):
			cum += approx_lengths[i]
			if cum >= max_range * 0.80:
				handover_idx = i
				break
		if handover_idx is None:
			handover_idx = max(0, len(waypoints) - 1)
		logger.info("plan_operator_area_trip: handover at waypoint %s (total_approx=%.0fm > max_range=%.0fm)", handover_idx, total_approx, max_range)
	# Строим маршрут по точкам зоны: каждый сегмент через plan_with_chargers (можно через станции)
	target_waypoints = waypoints[: handover_idx + 1] if handover_idx is not None else waypoints
	if not target_waypoints:
		target_waypoints = [waypoints[0]]
	plan_reserve = PLAN_RESERVE_PCT_TEST if STATE.get("battery_mode") == "test" else PLAN_RESERVE_PCT
	all_coords = []
	length = 0.0
	battery_left = battery_pct
	prev_pt = pos
	for i, wp in enumerate(target_waypoints):
		start_node = _routing_service._find_nearest_node(G, prev_pt)
		end_node = _routing_service._find_nearest_node(G, tuple(wp))
		if not start_node or not end_node:
			if i == 0:
				return ({"ok": False, "reason": "NO_PATH", "details": "no path to area"}, None)
			break
		path_seg, coords_seg, len_seg, ch_seg = _routing_service.plan_with_chargers(
			G, start_node, end_node, battery_left, MODE_EMPTY, drone_type, reserve_pct=OPERATOR_RESERVE_PCT,
			charger_nodes=charger_nodes, max_segment_battery_pct=MAX_SEGMENT_BATTERY_PCT,
			max_battery_pct_to_reach_charger=MAX_BATTERY_PCT_TO_REACH_CHARGER,
		)
		if not coords_seg:
			path_seg, coords_seg, len_seg = plan_route_for(
				prev_pt, tuple(wp), drone_type, battery_left, reserve_pct=OPERATOR_RESERVE_PCT
			)
			ch_seg = []
		if not coords_seg:
			if i == 0:
				return ({"ok": False, "reason": "NO_PATH", "details": "no path to area"}, None)
			break
		if all_coords and coords_seg:
			if all_coords[-1] == coords_seg[0]:
				all_coords.extend(coords_seg[1:])
			else:
				all_coords.extend(coords_seg)
		else:
			all_coords.extend(coords_seg)
		length += len_seg
		battery_left = 100.0 if ch_seg else _routing_service.compute_battery_after(len_seg, battery_left, MODE_EMPTY, drone_type)
		prev_pt = tuple(coords_seg[-1]) if coords_seg else tuple(wp)
	coords = all_coords
	if not coords:
		return ({"ok": False, "reason": "NO_PATH", "details": "no path to area"}, None)
	battery_after = battery_left
	# Сегмент до зарядки
	last_pt = coords[-1] if coords else pos
	base_node = charger_nodes.get("base")
	station_nodes = charger_nodes.get("stations") or []
	plan_reserve = PLAN_RESERVE_PCT_TEST if STATE.get("battery_mode") == "test" else PLAN_RESERVE_PCT
	last_node = _routing_service._find_nearest_node(G, last_pt)
	best_escape = None
	best_escape_len = float("inf")
	for name, goal_n in [("base", base_node)] + [(f"station_{i}", sn) for i, sn in enumerate(station_nodes) if sn]:
		if goal_n is None or goal_n not in G.nodes:
			continue
		p_c, co_c, l_c, _ = _routing_service.plan_with_chargers(
			G, last_node, goal_n, battery_after, MODE_EMPTY, drone_type, reserve_pct=plan_reserve, charger_nodes=charger_nodes,
			max_segment_battery_pct=MAX_SEGMENT_BATTERY_PCT, max_battery_pct_to_reach_charger=MAX_BATTERY_PCT_TO_REACH_CHARGER,
		)
		if p_c and co_c and l_c < best_escape_len:
			best_escape_len = l_c
			best_escape = (p_c, co_c, l_c)
	if best_escape:
		_, coords_escape, len_escape = best_escape
		coords = list(coords) + list(coords_escape[1:]) if coords_escape else coords
		length += len_escape
	result = {
		"ok": True,
		"coords": coords,
		"route_length": length,
		"pickup_waypoint_count": len(coords),
		"chargers_used": [],
		"battery_plan": [],
		"segments": [{"type": "area", "coords": coords, "length": length, "chargers": []}],
	}
	if handover_idx is not None and handover_idx + 1 < len(waypoints):
		handover_point = tuple(waypoints[handover_idx])
		rest_waypoints = waypoints[handover_idx + 1:]
		continuation_order = {
			"id": f"ord_cont_{order.get('id', '')}_{len(STATE['orders'])}",
			"type": order.get("type", "shooting"),
			"drone_type": "operator",
			"priority": order.get("priority", 5),
			"start": STATE.get("base") or handover_point,
			"end": rest_waypoints[-1] if rest_waypoints else handover_point,
			"battery_level": 100.0,
			"status": "queued",
			"handover_point": handover_point,
			"rest_waypoints": rest_waypoints,
			"area_polygon": order.get("area_polygon"),
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
	G = STATE.get("city_graph")
	city = STATE.get("city")
	if not city or G is None:
		return {"ok": False, "reason": "no graph", "details": "no city graph"}
	_routing_service.set_battery_mode(STATE.get("battery_mode", "reality"))
	_routing_service.city_graphs[city] = G
	charger_nodes = STATE.get("charger_nodes") or {"base": None, "stations": []}
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
	return {
		"ok": True,
		"coords": full_coords,
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
	"""Маршрут второго операторского дрона: позиция → handover_point → rest_waypoints → зарядка. С зарядками по пути."""
	G = STATE.get("city_graph")
	if not G:
		return {"ok": False, "reason": "no graph", "details": "no city graph"}
	_routing_service.set_battery_mode(STATE.get("battery_mode", "reality"))
	charger_nodes = STATE.get("charger_nodes") or {"base": None, "stations": []}
	pos = tuple(drone.get("pos", (0, 0)))
	handover = tuple(order["handover_point"]) if isinstance(order["handover_point"], (list, tuple)) else order["handover_point"]
	rest = list(order.get("rest_waypoints", []))
	rest = [tuple(w) if isinstance(w, (list, tuple)) else w for w in rest]
	battery_pct = float(drone.get("battery", 100.0))
	OPERATOR_RESERVE_PCT = 20.0
	plan_reserve = PLAN_RESERVE_PCT_TEST if STATE.get("battery_mode") == "test" else PLAN_RESERVE_PCT
	# Участок до точки передачи (с возможностью зарядки по пути)
	start_node = _routing_service._find_nearest_node(G, pos)
	handover_node = _routing_service._find_nearest_node(G, handover)
	if not start_node or not handover_node:
		return {"ok": False, "reason": "NO_PATH", "details": "no nodes for handover"}
	path1, coords1, len1, ch1 = _routing_service.plan_with_chargers(
		G, start_node, handover_node, battery_pct, MODE_EMPTY, "operator", reserve_pct=OPERATOR_RESERVE_PCT,
		charger_nodes=charger_nodes, max_segment_battery_pct=MAX_SEGMENT_BATTERY_PCT,
		max_battery_pct_to_reach_charger=MAX_BATTERY_PCT_TO_REACH_CHARGER,
	)
	if not coords1:
		path1, coords1, len1 = plan_route_for(pos, handover, "operator", battery_pct, reserve_pct=OPERATOR_RESERVE_PCT)
		ch1 = []
	if not coords1:
		return {"ok": False, "reason": "NO_PATH", "details": "no path to handover"}
	battery_after1 = 100.0 if ch1 else _routing_service.compute_battery_after(len1, battery_pct, MODE_EMPTY, "operator")
	# Участок handover → rest_waypoints по порядку (каждый сегмент через plan_with_chargers)
	coords2, len2 = [], 0.0
	prev_pt = handover
	battery_after2 = battery_after1
	if rest:
		for wp in rest:
			wp = tuple(wp) if isinstance(wp, (list, tuple)) else wp
			a_node = _routing_service._find_nearest_node(G, prev_pt)
			b_node = _routing_service._find_nearest_node(G, wp)
			if not a_node or not b_node:
				break
			p2, c2, l2, ch2 = _routing_service.plan_with_chargers(
				G, a_node, b_node, battery_after2, MODE_EMPTY, "operator", reserve_pct=OPERATOR_RESERVE_PCT,
				charger_nodes=charger_nodes, max_segment_battery_pct=MAX_SEGMENT_BATTERY_PCT,
				max_battery_pct_to_reach_charger=MAX_BATTERY_PCT_TO_REACH_CHARGER,
			)
			if not c2:
				p2, c2, l2 = plan_route_for(prev_pt, wp, "operator", battery_after2, reserve_pct=OPERATOR_RESERVE_PCT)
				ch2 = []
			if not c2:
				break
			if coords2 and c2 and coords2[-1] == c2[0]:
				coords2.extend(c2[1:])
			else:
				coords2.extend(c2)
			len2 += l2
			battery_after2 = 100.0 if ch2 else _routing_service.compute_battery_after(l2, battery_after2, MODE_EMPTY, "operator")
			prev_pt = tuple(c2[-1]) if c2 else wp
		if not coords2 and rest:
			path2, coords2, len2 = plan_route_for(
				handover, rest[-1], "operator", battery_after1,
				waypoints=rest[:-1] if len(rest) > 1 else None,
				reserve_pct=OPERATOR_RESERVE_PCT,
			)
			if coords2:
				battery_after2 = _routing_service.compute_battery_after(len2, battery_after1, MODE_EMPTY, "operator")
		if not coords2:
			return {"ok": False, "reason": "NO_PATH", "details": "no path through rest waypoints"}
		last_pt = coords2[-1] if coords2 else handover
	else:
		last_pt = handover
	last_node = _routing_service._find_nearest_node(G, last_pt)
	base_node = charger_nodes.get("base")
	station_nodes = charger_nodes.get("stations") or []
	best_escape = None
	best_escape_len = float("inf")
	for goal_n in [base_node] + list(station_nodes or []):
		if goal_n is None or goal_n not in G.nodes:
			continue
		p_c, co_c, l_c, _ = _routing_service.plan_with_chargers(
			G, last_node, goal_n, battery_after2, MODE_EMPTY, "operator", reserve_pct=plan_reserve, charger_nodes=charger_nodes,
			max_segment_battery_pct=MAX_SEGMENT_BATTERY_PCT, max_battery_pct_to_reach_charger=MAX_BATTERY_PCT_TO_REACH_CHARGER,
		)
		if p_c and co_c and l_c < best_escape_len:
			best_escape_len = l_c
			best_escape = (co_c, l_c)
	full_coords = list(coords1)
	if coords2:
		full_coords.extend(coords2[1:] if full_coords and coords2 and full_coords[-1] == coords2[0] else coords2)
	if best_escape:
		full_coords.extend(best_escape[0][1:])
	total_len = len1 + len2 + (best_escape[1] if best_escape else 0.0)
	logger.info("plan_operator_continuation_trip: handover=%s, rest_waypoints=%s, total_len=%.0fm", handover, len(rest), total_len)
	return {
		"ok": True,
		"coords": full_coords,
		"route_length": total_len,
		"pickup_waypoint_count": len(coords1),
		"chargers_used": [],
		"battery_plan": [],
		"segments": [],
	}


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
	# Сортируем по приоритету (убывание), при равном — по порядку в списке (FIFO)
	order_indices = {o.get("id"): i for i, o in enumerate(STATE["orders"])}
	queue = sorted(queue, key=lambda o: (-o.get("priority", 0), order_indices.get(o.get("id"), 0)))
	for order in queue:
		required_type = order.get("drone_type") or map_order_to_drone_type(order.get("type", "delivery"))
		drone_id = pick_drone_for_order(order, required_type)
		if drone_id is None:
			continue
		drone = STATE["drones"][drone_id]
		# Грузовой: три фазы (забор → доставка → зарядка). Операторский/сервисный: точка или область.
		if required_type == "cargo":
			result = plan_order_trip(
				drone["pos"], order["start"], order["end"], drone["type"], drone["battery"]
			)
		elif required_type == "operator" and order.get("handover_point") and order.get("rest_waypoints"):
			result = plan_operator_continuation_trip(drone, order)
			logger.info("assign_orders: continuation order %s -> drone %s", order.get("id"), drone_id)
		elif required_type == "operator" and order.get("area_polygon"):
			result, continuation = plan_operator_area_trip(drone, order)
			if result and result.get("ok") and continuation:
				STATE["orders"].append(continuation)
				logger.info("assign_orders: created continuation order %s (handover at %s), first drone %s", continuation.get("id"), continuation.get("handover_point"), drone_id)
			if not result or not result.get("ok"):
				result = result or {"ok": False, "reason": "NO_PATH", "details": "operator area plan failed"}
		else:
			# Операторская точка или сервисный: дрон → точка → зарядка (отдельное планирование)
			result = plan_operator_point_trip(
				drone["pos"], order["end"], drone["type"], drone["battery"]
			)
		if not result.get("ok"):
			logger.info(
				"assign_orders: order %s not assigned to %s reason=%s details=%s",
				order.get("id"), drone_id, result.get("reason"), result.get("details")
			)
			continue
		full_coords = result["coords"]
		apply_midroute_charging(drone, full_coords)
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
		order["chargers_used"] = result.get("chargers_used", [])
		order["battery_plan"] = result.get("battery_plan", [])
		order["segments"] = result.get("segments", [])
		drone["loaded_after_waypoint_count"] = pickup_wp
		drone["waypoints_completed"] = 0

def _estimate_speed_mps() -> float:
	"""Скорость дрона (м/с) для оценки ETA с учётом ветра."""
	wind = float(STATE.get("weather", {}).get("wind_mps", 3.0))
	return max(5.0, 15.0 - 0.3 * wind)


def _order_completion_time_seconds(result: Dict[str, Any]) -> float:
	"""Оценка времени выполнения заказа в секундах: полёт + остановки на зарядку."""
	if not result or not result.get("ok"):
		return float("inf")
	route_m = float(result.get("route_length", 0))
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
	if required_type == "operator" and order.get("handover_point") and order.get("rest_waypoints"):
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
	return (_order_completion_time_seconds(result), result)


def pick_drone_for_order(order: Dict[str, Any], required_drone_type: str) -> Optional[str]:
	"""Выбор дрона с минимальным ETA (полёт + зарядки по пути + выполнение заказа)."""
	order_start = order.get("start") or order.get("end")
	if not order_start:
		return None
	order_start = tuple(order_start) if isinstance(order_start, (list, tuple)) and len(order_start) == 2 else None
	if not order_start:
		return None

	best_drone_id = None
	best_time = float("inf")

	for drone_id, d in STATE["drones"].items():
		if d.get("type") != required_drone_type:
			continue
		if d.get("status") not in (None, "idle"):
			continue
		if not d.get("pos"):
			continue
		time_sec, _ = estimate_order_completion_time_seconds(d, order, required_drone_type)
		if time_sec < best_time:
			best_time = time_sec
			best_drone_id = drone_id

	# Вариант «новый дрон с базы» — сравниваем ETA
	inv = STATE.get("inventory") or {}
	has_inventory = (inv.get(required_drone_type, 0) or 0) > 0
	time_new = float("inf")
	if has_inventory:
		time_new, _ = estimate_new_drone_completion_time_seconds(order, required_drone_type)
		existing_best = best_time
		if time_new < best_time:
			new_id = spawn_drone_from_inventory(required_drone_type)
			if new_id:
				logger.info(
					"pick_drone_for_order: order=%s -> new drone %s (ETA %.0fs < existing best %.0fs)",
					order.get("id"), new_id, time_new, existing_best,
				)
				return new_id

	if best_drone_id is not None:
		logger.info(
			"pick_drone_for_order: order=%s -> drone %s (ETA %.0fs, new_from_base=%.0fs)",
			order.get("id"), best_drone_id, best_time, time_new if has_inventory else float("inf"),
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
	G = STATE.get("city_graph")
	if not city or G is None:
		return None, None, 0.0
	_routing_service.city_graphs[city] = G
	res = reserve_pct if reserve_pct is not None else RESERVE_PCT
	max_range = _routing_service.max_reachable_distance(battery_level, MODE_EMPTY, drone_type, reserve_pct=res)
	path, coords, length = _routing_service.plan_direct_path(G, start, end, max_range, waypoints=waypoints)
	if path:
		return path, coords, length
	# fallback to node-to-node
	start_node = _routing_service._find_nearest_node(G, start)
	end_node = _routing_service._find_nearest_node(G, end)
	if not start_node or not end_node:
		return None, None, 0.0
	path = _routing_service._find_safe_path(G, start_node, end_node, max_range)
	if not path:
		return None, None, 0.0
	coords = [G.nodes[n]['pos'] for n in path]
	length = _routing_service._calculate_path_length(G, path)
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
	# Всегда синхронизируем режим батареи из STATE, чтобы тест/реальность применялись при планировании
	_routing_service.set_battery_mode(STATE.get("battery_mode", "reality"))
	_routing_service.city_graphs[city] = G
	charger_nodes = STATE.get("charger_nodes") or {"base": None, "stations": []}
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

	# Попытка без предварительной зарядки; для грузового проверяем, что после доставки остаётся >= 20%
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
		if not base_node and not any(station_nodes):
			# No chargers defined — plan is still valid, no escape segment
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

	return {
		"ok": True,
		"coords": full_coords,
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
	charger_nodes = STATE.get("charger_nodes") or {"base": None, "stations": []}
	point_node = _routing_service._find_nearest_node(G, point)
	if not point_node:
		return False
	base_node = charger_nodes.get("base")
	stations = charger_nodes.get("stations") or []
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
	G = STATE.get("city_graph")
	if not city or G is None:
		return
	for drone_id, drone in STATE["drones"].items():
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
		route = drone.get("route") or []
		idx = drone.get("target_idx", 0)
		if not route or idx >= len(route):
			drone["status"] = "idle"
			mark_order_completed_if_any(drone_id)
			# При заряде <=20% после завершения — сразу на зарядку
			if drone.get("battery", 100.0) <= FLY_TO_CHARGER_AT_PCT:
				maybe_route_to_base_or_station(drone)
			continue
		current = drone["pos"]
		target = route[idx]
		# Check if target lies now inside any zone; if so reroute from current to end
		if is_point_in_any_zone(target):
			# attempt reroute
			end = route[-1]
			_, coords, _ = plan_via_base_if_needed(current, end, drone["type"], drone["battery"])
			if coords:
				drone["route"] = coords
				drone["target_idx"] = 0
				continue
			else:
				drone["status"] = "holding"
				continue
		# Base speed affected by wind (simple model)
		wind = float(STATE.get("weather",{}).get("wind_mps", 3.0))
		speed_mps = max(5.0, 15.0 - 0.3 * wind)
		dist = haversine_m(current, target)
		if dist < speed_mps:
			# Arrive this tick
			drone["pos"] = target
			at_charger = is_at_any_station(target) or (STATE.get("base") and haversine_m(target, tuple(STATE["base"])) < STATION_NEAR_METERS)
			# Если прилетели на зарядку и впереди ещё точки — останавливаемся на зарядку, потом продолжаем маршрут
			if at_charger and idx + 1 < len(route):
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
					if at_charger:
						assign_to_charger_queue(drone_id)
						drone["status"] = "charging"
					else:
						drone["status"] = "idle"
						mark_order_completed_if_any(drone_id)
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
	# Перпендикуляр к направлению на цель (сдвиг ~5 м вбок)
	dx = target[0] - current[0]
	dy = target[1] - current[1]
	norm = (dx * dx + dy * dy) ** 0.5
	if norm < 1e-9:
		drone["avoidance_ticks"] = 0
		drone["status"] = "enroute"
		return
	# Сдвиг на ~0.00005 градуса (примерно 5 м)
	offset = 0.00005 * (1 if (hash(drone_id) % 2 == 0) else -1)
	side_lat = current[0] - (dy / norm) * offset
	side_lon = current[1] + (dx / norm) * offset
	sidestep = (side_lat, side_lon)
	if not will_collide(drone_id, sidestep):
		drone["pos"] = sidestep
		drone["status"] = "enroute"
	drone["avoidance_ticks"] = 0


def will_collide(drone_id: str, next_pos: Tuple[float,float]) -> bool:
	for other_id, other in STATE["drones"].items():
		if other_id == drone_id:
			continue
		if haversine_m(next_pos, other.get("pos", next_pos)) < 8.0:  # 8 meters bubble
			return True
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
		if o.get("drone_id") == drone_id and o.get("status") == "assigned":
			route = drone.get("route") or []
			if not route:
				return
			drone["saved_route_for_charge"] = list(route)
			drone["saved_target_idx"] = int(drone.get("target_idx", 0))
			drone["saved_order_id"] = o.get("id")
			return


def _restore_route_after_charging(drone: Dict[str, Any]) -> bool:
	"""После зарядки восстанавливаем маршрут заказа, если он был сохранён. Возвращает True, если восстановили."""
	saved = drone.get("saved_route_for_charge")
	if not saved:
		return False
	drone["route"] = list(saved)
	drone["target_idx"] = int(drone.get("saved_target_idx", 0))
	drone["status"] = "enroute"
	order_id = drone.pop("saved_order_id", None)
	drone.pop("saved_route_for_charge", None)
	drone.pop("saved_target_idx", None)
	logger.info("_restore_route_after_charging: restored route for order %s (%s waypoints from idx %s)", order_id, len(saved), drone.get("target_idx"))
	return True


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
		G = STATE.get("city_graph")
		charger_nodes = STATE.get("charger_nodes") or {"base": None, "stations": []}
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

def assign_to_charger_queue(drone_id: str):
    d = STATE["drones"].get(drone_id)
    if not d:
        return
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
        return
    # Станции зарядки: смена аккумулятора (запас 20 заряженных)
    idx = nearest_station_index(pos)
    if idx is None:
        return
    key = str(idx)
    sq = (STATE.get("station_queues") or {}).get(key)
    if not sq:
        sq = {"charged_batteries": STATION_CHARGED_BATTERIES_MAX, "charging_queue": [], "queue": []}
    if "charged_batteries" in sq:
        if drone_id in sq.get("queue", []):
            return
        if sq.get("charged_batteries", 0) > 0:
            sq["charged_batteries"] -= 1
            sq.setdefault("charging_queue", []).append(STATION_BATTERY_CHARGE_TICKS)
            d["battery"] = 100.0
            resume = d.get("resume_route") or []
            if resume:
                d["route"] = list(resume)
                d["resume_route"] = []
                d["target_idx"] = 0
                d["status"] = "enroute"
            elif _restore_route_after_charging(d):
                pass
            else:
                d["status"] = "idle"
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

def progress_charging():
    # base
    bq = STATE.get("base_queue") or {"charging": [], "queue": [], "capacity": 2}
    done = []
    for did in list(bq.get("charging", [])):
        d = STATE["drones"].get(did)
        if not d:
            continue
        d["battery"] = min(100.0, float(d.get("battery", 0.0)) + 4.0)  # 4% per tick
        if d["battery"] >= 100.0:
            done.append(did)
            resume = d.get("resume_route") or []
            if resume:
                d["route"] = list(resume)
                d["resume_route"] = []
                d["target_idx"] = 0
                d["status"] = "enroute"
            elif _restore_route_after_charging(d):
                pass
            else:
                d["status"] = "idle"
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
                resume = d.get("resume_route") or []
                if resume:
                    d["route"] = list(resume)
                    d["resume_route"] = []
                    d["target_idx"] = 0
                    d["status"] = "enroute"
                elif _restore_route_after_charging(d):
                    pass
                else:
                    d["status"] = "idle"
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
                if d["battery"] >= 100.0:
                    if did in sq.get("queue", []):
                        sq["queue"].remove(did)
                    resume = d.get("resume_route") or []
                    if resume:
                        d["route"] = list(resume)
                        d["resume_route"] = []
                        d["target_idx"] = 0
                        d["status"] = "enroute"
                    elif _restore_route_after_charging(d):
                        pass
                    else:
                        d["status"] = "idle"
        else:
            done = []
            for did in list(sq.get("charging", [])):
                d = STATE["drones"].get(did)
                if not d:
                    continue
                d["battery"] = min(100.0, float(d.get("battery", 0.0)) + 4.0)
                if d["battery"] >= 100.0:
                    done.append(did)
                    resume = d.get("resume_route") or []
                    if resume:
                        d["route"] = list(resume)
                        d["resume_route"] = []
                        d["target_idx"] = 0
                        d["status"] = "enroute"
                    elif _restore_route_after_charging(d):
                        pass
                    else:
                        d["status"] = "idle"
            for did in done:
                if did in sq.get("charging", []):
                    sq["charging"].remove(did)
            while len(sq.get("charging", [])) < sq.get("capacity", 2) and sq.get("queue", []):
                sq["charging"].append(sq["queue"].pop(0))
        sqs[key] = sq
    STATE["station_queues"] = sqs

def mark_order_completed_if_any(drone_id: str):
    # If order assigned to this drone becomes completed
    for o in STATE.get("orders", []):
        if o.get("drone_id") == drone_id and o.get("status") == "assigned":
            o["status"] = "completed"
            break

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
    base = STATE.get("base")
    inv = STATE.get("inventory") or {}
    if not base or not isinstance(inv, dict):
        return
    # If there are already drones, do not auto-spawn duplicates
    if STATE.get("drones"):
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
            spawn_drone(typ, pos=tuple(base), battery=100.0)

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


def spawn_drone(drone_type: str, pos: Tuple[float, float], battery: float = 100.0) -> str:
    did = _next_drone_id(drone_type)
    STATE["drones"][did] = {
        "pos": pos,
        "type": drone_type,
        "battery": float(battery),
        "route": [],
        "target_idx": 0,
        "status": "idle",
    }
    return did

def spawn_drone_from_inventory(pref_type: str) -> Optional[str]:
    base = STATE.get("base")
    inv = STATE.get("inventory") or {}
    if not base:
        return None
    # prefer requested type, else any available
    types_to_try = [pref_type] + [t for t in ("cargo","operator","cleaner") if t != pref_type]
    for t in types_to_try:
        cnt = inv.get(t, 0)
        try:
            cnt = int(cnt)
        except Exception:
            cnt = 0
        if cnt > 0:
            did = spawn_drone(t, pos=tuple(base), battery=100.0)
            inv[t] = cnt - 1
            STATE["inventory"] = inv
            return did
    return None

# Split a full route into two: up to first charger (station/base), then remainder to resume after charging
def apply_midroute_charging(drone: Dict[str, Any], full_coords: List[Tuple[float, float]]):
    try:
        if not isinstance(full_coords, list) or len(full_coords) < 2:
            drone["route"] = list(full_coords or [])
            drone["resume_route"] = []
            drone["target_idx"] = 0
            drone["status"] = "enroute" if full_coords else "idle"
            return
        base = STATE.get("base")
        split_idx = None
        for i in range(1, len(full_coords)):
            pt = tuple(full_coords[i])
            at_station = is_at_any_station(pt)
            at_base = bool(base) and haversine_m(pt, tuple(base)) < STATION_NEAR_METERS
            if at_station or at_base:
                if i < len(full_coords) - 1:
                    split_idx = i
                    break
        if split_idx is not None:
            drone["route"] = list(full_coords[:split_idx+1])
            drone["resume_route"] = list(full_coords[split_idx+1:])
            drone["target_idx"] = 0
            drone["status"] = "enroute"
        else:
            drone["route"] = list(full_coords)
            drone["resume_route"] = []
            drone["target_idx"] = 0
            drone["status"] = "enroute"
    except Exception:
        drone["route"] = list(full_coords or [])
        drone["resume_route"] = []
        drone["target_idx"] = 0
        drone["status"] = "enroute" if full_coords else "idle"

# Persistence
async def persist_state():
    if not _redis:
        return
    try:
        data = {
            "city": STATE.get("city"),
            "orders": STATE.get("orders", []),
            "drones": STATE.get("drones", {}),
            "no_fly_zones": STATE.get("no_fly_zones", []),
            "weather": STATE.get("weather", {}),
            "base": STATE.get("base"),
            "inventory": STATE.get("inventory", {}),
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
        STATE["orders"] = data.get("orders", [])
        STATE["drones"] = data.get("drones", {})
        STATE["no_fly_zones"] = data.get("no_fly_zones", [])
        STATE["weather"] = data.get("weather", {"wind_mps": 3.0})
        STATE["base"] = tuple(data.get("base")) if data.get("base") else None
        STATE["inventory"] = data.get("inventory", {})
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

# Run helper for uvicorn: python -m uvicorn api_server:app --reload
