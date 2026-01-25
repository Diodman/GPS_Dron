from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import asyncio
import logging
from typing import Dict, List, Any, Optional, Tuple
from pydantic import BaseModel
import math
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse
import os
import json
from datetime import datetime
from redis import Redis

from data_service import DataService
from graph_service import GraphService
from routing_service import RoutingService

logger = logging.getLogger(__name__)

app = FastAPI(title="Drone Planner API", version="0.2.0")
app.add_middleware(
	CORSMiddleware,
	allow_origins=["*"],
	allow_credentials=True,
	allow_methods=["*"],
	allow_headers=["*"],
)
app.mount("/static", StaticFiles(directory="static"), name="static")

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
    "station_queues": {},  # station_index -> {charging:[], queue:[], capacity:int}
    "base_queue": {"charging": [], "queue": [], "capacity": 2},
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


async def broadcast_state():
	payload = {
		"city": STATE["city"],
		"orders": STATE["orders"],
		"drones": STATE["drones"],
		"histories": {k: v.get("history", []) for k,v in STATE["drones"].items()},
		"no_fly_zones": STATE["no_fly_zones"],
		"stations": STATE.get("stations", []),
		"weather": STATE.get("weather", {}),
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
	z = zone.dict()
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

@app.post("/api/orders")
async def add_order(order: AddOrderRequest):
	# Classify order type
	order_type = classify_order(order)
	start = await resolve_point(order.coords_from, order.address_from)
	end = await resolve_point(order.coords_to, order.address_to)
	if not start or not end:
		return JSONResponse(status_code=400, content={"ok": False, "error": "Invalid start or end"})
	order_id = f"ord_{len(STATE['orders'])+1}"
	req_drone_type = (order.drone_type or "").strip().lower() or map_order_to_drone_type(order_type)
	if req_drone_type not in ("cargo", "operator", "cleaner"):
		req_drone_type = map_order_to_drone_type(order_type)
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
	"""Загрузить погоду из Open-Meteo по координатам (или по базе)."""
	if lat is None or lon is None:
		base = STATE.get("base")
		if base and len(base) == 2:
			lat, lon = float(base[0]), float(base[1])
		else:
			# Волгоград по умолчанию
			lat, lon = 48.7080, 44.5133
	data = await fetch_weather_from_api(lat, lon)
	if data:
		STATE["weather"] = data
		await persist_state()
		return {"ok": True, "weather": STATE["weather"]}
	return JSONResponse(status_code=502, content={"ok": False, "error": "Weather API unavailable"})

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
    # Optionally create drones at base per inventory if none exist
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
        # init queues per station
        STATE["station_queues"] = {
            str(i): {"charging": [], "queue": [], "capacity": int(max(1, cfg.capacity or 2))}
            for i in range(len(stations))
        }
        await persist_state()
        return {"ok": True, "stations": stations}
    except Exception as e:
        return JSONResponse(status_code=400, content={"ok": False, "error": str(e)})

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


async def rebuild_graph_with_zones():
	city = STATE.get("city")
	if not city:
		return
	try:
		city_data = _data_service.get_city_data(city)
		city_data['no_fly_zones'] = list(STATE["no_fly_zones"]) or city_data.get('no_fly_zones', [])
		city_graph = _graph_service.build_city_graph(city_data, city_data.get('drone_type', 'cargo'))
		_routing_service.city_graphs[city] = city_graph
		STATE["city_graph"] = city_graph
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
			# Свободного дрона нужной категории нет — заказ остаётся в очереди
			continue
		# Строим маршрут: от текущей позиции дрона → точка старта заказа → точка доставки (без лишнего заезда на базу)
		drone = STATE["drones"][drone_id]
		_, coords_to_start, _ = plan_route_for(drone["pos"], order["start"], drone["type"], drone["battery"])
		_, coords_start_to_end, length_seg = plan_via_base_if_needed(order["start"], order["end"], drone["type"], drone["battery"])
		if coords_to_start and coords_start_to_end:
			# Склеиваем: дрон → старт заказа → доставка (без дублирования точки старта)
			full_coords = list(coords_to_start) + list(coords_start_to_end)[1:]
			length = (length_seg + _route_length(coords_to_start)) if coords_to_start else length_seg
			apply_midroute_charging(drone, full_coords)
			order["status"] = "assigned"
			order["drone_id"] = drone_id
			order["route_length"] = length
			order["pickup_waypoint_count"] = len(coords_to_start)  # загружен после стольких прибытий
			drone["loaded_after_waypoint_count"] = len(coords_to_start)
			drone["waypoints_completed"] = 0
		elif coords_start_to_end:
			# дрон уже у точки старта или граф не дал путь от дрона — строим только старт→конец
			apply_midroute_charging(drone, coords_start_to_end)
			order["status"] = "assigned"
			order["drone_id"] = drone_id
			order["route_length"] = length_seg
			order["pickup_waypoint_count"] = 0  # уже на старте — сразу загружен
			drone["loaded_after_waypoint_count"] = 0
			drone["waypoints_completed"] = 0

def pick_drone_for_order(order: Dict[str, Any], required_drone_type: str) -> Optional[str]:
	# Выбираем ближайший свободный дрон заданной категории (cargo/operator/cleaner).
	best = None
	best_dist = float('inf')
	for drone_id, d in STATE["drones"].items():
		if d.get("type") != required_drone_type:
			continue
		if d.get("status") not in (None, "idle"):
			continue
		if not d.get("pos"):
			continue
		dist = haversine_m(d["pos"], order["start"])
		if dist < best_dist:
			best_dist = dist
			best = drone_id
	return best


def map_order_to_drone_type(order_type: str) -> str:
	if order_type == "delivery":
		return "cargo"
	if order_type == "shooting":
		return "operator"
	return "cleaner"


def plan_route_for(start: Tuple[float, float], end: Tuple[float, float], drone_type: str, battery_level: float, waypoints: Optional[List[Tuple[float,float]]] = None):
	city = STATE.get("city")
	G = STATE.get("city_graph")
	if not city or G is None:
		return None, None, 0.0
	# ensure routing service has this graph
	_routing_service.city_graphs[city] = G
	max_range = _routing_service._get_drone_params(drone_type).get('battery_range', 20000) * (battery_level/100.0)
	# Use door-to-door planner with temporary nodes
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

def plan_via_base_if_needed(current: Tuple[float,float], end: Tuple[float,float], drone_type: str, battery_level: float):
    base = STATE.get("base")
    if not base:
        return plan_route_for(current, end, drone_type, battery_level)
    _, coords_direct, length_direct = plan_route_for(current, end, drone_type, battery_level)
    if not coords_direct:
        return None, None, 0.0
    per = {
        "cargo": 2000.0,
        "operator": 2500.0,
        "cleaner": 3000.0,
    }.get(drone_type, 2000.0)
    usable_range = per * (battery_level/100.0) * 0.9
    max_range = per * (battery_level/100.0)
    # Не вставляем базу/станцию, если заряда хватает и маршрут не очень длинный — летим сразу в точку доставки
    if length_direct <= usable_range and can_escape_after(end, drone_type):
        return None, coords_direct, length_direct
    if battery_level >= 50.0 and length_direct <= 0.85 * max_range:
        return None, coords_direct, length_direct
    # Кандидаты на дозаряд: только те, что не совпадают с текущей позицией (не отправляем "на базу" если уже на ней)
    candidates: List[Tuple[float,float]] = []
    for cand in [tuple(base)] + [tuple(s) for s in STATE.get("stations", []) if isinstance(s, (list, tuple)) and len(s) == 2]:
        if haversine_m(current, cand) >= 25.0:
            candidates.append(cand)
    best = None
    best_len = float('inf')
    for cand in candidates:
        _, c1, l1 = plan_route_for(current, cand, drone_type, battery_level)
        if not c1:
            continue
        _, c2, l2 = plan_route_for(cand, end, drone_type, 100.0)
        if c2 and (l1 + l2) < best_len:
            best = (c1 + c2, l1 + l2)
            best_len = l1 + l2
    if best:
        return None, best[0], best[1]
    return None, coords_direct, length_direct

def can_escape_after(point: Tuple[float,float], drone_type: str) -> bool:
    # Check there is a reachable base/station within 80% battery from point (assumes charge at end only if station)
    candidates: List[Tuple[float,float]] = []
    if STATE.get("base"):
        candidates.append(tuple(STATE["base"]))
    for s in STATE.get("stations", []):
        try:
            candidates.append(tuple(s))
        except Exception:
            pass
    if not candidates:
        return True
    try:
        per = {
            "cargo": 2000.0,
            "operator": 2500.0,
            "cleaner": 3000.0,
        }.get(drone_type, 2000.0)
        # require possibility to reach some candidate with at most 80% battery
        for cand in candidates:
            _, coords, length = plan_route_for(point, cand, drone_type, 80.0)
            if coords and length <= per * 0.8:
                return True
    except Exception:
        return True
    return False


def simulate_step():
	# move drones along their routes, drain battery, reroute if blocked and avoid collisions
	city = STATE.get("city")
	G = STATE.get("city_graph")
	if not city or G is None:
		return
	for drone_id, drone in STATE["drones"].items():
		if drone.get("status") == "avoidance":
			_avoidance_step(drone_id, drone)
			continue
		if drone.get("status") != "enroute":
			continue
		route = drone.get("route") or []
		idx = drone.get("target_idx", 0)
		if not route or idx >= len(route):
			# Order considered complete if present
			drone["status"] = "idle"
			mark_order_completed_if_any(drone_id)
			# If low battery after completing, head to base
			if drone.get("battery", 100.0) < 30.0:
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
			drone["target_idx"] = idx + 1
			drone["waypoints_completed"] = drone.get("waypoints_completed", 0) + 1
			if drone["target_idx"] >= len(route):
				# If arrived at station/base, join queue or charge
				if is_at_any_station(drone["pos"]) or (STATE.get("base") and haversine_m(drone["pos"], tuple(STATE["base"])) < 20.0):
					assign_to_charger_queue(drone_id)
					drone["status"] = "charging"
				else:
					drone["status"] = "idle"
					# Дрон доставил груз — отмечаем заказ выполненным
					mark_order_completed_if_any(drone_id)
			else:
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
		# low battery behavior: route to nearest charger when under thresholds
		if drone["battery"] <= 5.0:
			drone["status"] = "low_battery"
			maybe_route_to_base_or_station(drone)
		elif drone["battery"] <= 20.0 and drone.get("status") in ("idle", "holding"):
			maybe_route_to_base_or_station(drone)

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
	# Разный расход: пустой дрон экономичнее, загруженный — больше расход
	base_per = {
		"cargo": (2000.0, 1400.0),      # (м на 1% пустой, м на 1% загруженный)
		"operator": (2500.0, 1800.0),
		"cleaner": (3000.0, 2200.0),
	}.get(drone["type"], (2000.0, 1400.0))
	loaded = _is_drone_loaded(drone)
	per = base_per[1] if loaded else base_per[0]
	drain = (distance_m / per) * 100.0
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

def is_at_any_station(point: Tuple[float,float]) -> bool:
    try:
        for s in STATE.get("stations", []):
            if haversine_m(point, tuple(s)) < 20.0:
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
        _, coords, _ = plan_route_for(tuple(drone.get("pos", base)), tuple(base), drone["type"], max(5.0, float(drone.get("battery", 10.0))))
        if coords:
            drone["route"] = coords
            drone["target_idx"] = 0
            if drone["battery"] <= 5.0:
                drone["status"] = "low_battery"
            else:
                drone["status"] = "return_base"
    except Exception:
        pass

def maybe_route_to_base_or_station(drone: Dict[str, Any]):
    # choose nearest charger (station/base) and route there
    chargers: List[Tuple[float,float]] = []
    if STATE.get("base"):
        chargers.append(tuple(STATE["base"]))
    chargers += [tuple(s) for s in STATE.get("stations", []) if isinstance(s, (list, tuple)) and len(s)==2]
    if not chargers:
        return
    pos = tuple(drone.get("pos", chargers[0]))
    # pick nearest by haversine
    best = None
    bestd = float('inf')
    for c in chargers:
        d = haversine_m(pos, c)
        if d < bestd:
            bestd = d
            best = c
    if best:
        _, coords, _ = plan_route_for(pos, best, drone["type"], max(5.0, float(drone.get("battery", 10.0))))
        if coords:
            drone["route"] = coords
            drone["target_idx"] = 0
            drone["status"] = "return_charge"

def assign_to_charger_queue(drone_id: str):
    d = STATE["drones"].get(drone_id)
    if not d:
        return
    pos = tuple(d.get("pos", (0,0)))
    # decide base or station index
    base_close = STATE.get("base") and haversine_m(pos, tuple(STATE["base"])) < 20.0
    if base_close:
        q = STATE.get("base_queue") or {"charging": [], "queue": [], "capacity": 2}
        # occupy slot if available else join queue
        if drone_id not in q["charging"] and drone_id not in q["queue"]:
            if len(q["charging"]) < q.get("capacity", 2):
                q["charging"].append(drone_id)
            else:
                q["queue"].append(drone_id)
        STATE["base_queue"] = q
        return
    # stations
    idx = nearest_station_index(pos)
    if idx is None:
        return
    key = str(idx)
    sq = (STATE.get("station_queues") or {}).get(key)
    if not sq:
        # init default if missing
        sq = {"charging": [], "queue": [], "capacity": 2}
    if drone_id not in sq["charging"] and drone_id not in sq["queue"]:
        if len(sq["charging"]) < sq.get("capacity", 2):
            sq["charging"].append(drone_id)
        else:
            sq["queue"].append(drone_id)
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
            # resume remaining route if exists
            resume = d.get("resume_route") or []
            if resume:
                d["route"] = list(resume)
                d["resume_route"] = []
                d["target_idx"] = 0
                d["status"] = "enroute"
            else:
                d["status"] = "idle"
    for did in done:
        if did in bq["charging"]:
            bq["charging"].remove(did)
    # promote from queue
    while len(bq["charging"]) < bq.get("capacity", 2) and bq["queue"]:
        bq["charging"].append(bq["queue"].pop(0))
    STATE["base_queue"] = bq
    # stations
    sqs = STATE.get("station_queues") or {}
    for key, sq in sqs.items():
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
                else:
                    d["status"] = "idle"
        for did in done:
            if did in sq["charging"]:
                sq["charging"].remove(did)
        while len(sq["charging"]) < sq.get("capacity", 2) and sq["queue"]:
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

def spawn_drone(drone_type: str, pos: Tuple[float, float], battery: float = 100.0) -> str:
    did = f"drone_{len(STATE['drones'])+1}"
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
            at_base = bool(base) and haversine_m(pt, tuple(base)) < 20.0
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
        # rebuild graph if city exists
        if STATE["city"]:
            try:
                await rebuild_graph_with_zones()
            except Exception:
                logger.exception("Failed to rebuild graph on restore")
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
