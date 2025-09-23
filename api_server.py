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

from data_service import DataService
from graph_service import GraphService
from routing_service import RoutingService

logger = logging.getLogger(__name__)

app = FastAPI(title="Drone Planner API", version="0.1.0")
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
}

# Helpers
async def broadcast_state():
	payload = {
		"city": STATE["city"],
		"orders": STATE["orders"],
		"drones": STATE["drones"],
		"no_fly_zones": STATE["no_fly_zones"],
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
		await asyncio.sleep(1.0)

@app.on_event("startup")
async def on_startup():
	asyncio.create_task(broadcaster_loop())
	asyncio.create_task(scheduler_loop())

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
	entry = {
		"id": order_id,
		"type": order_type,
		"start": start,
		"end": end,
		"battery_level": max(1.0, min(100.0, float(order.battery_level))),
		"status": "queued",
		"waypoints": [tuple(wp) for wp in (order.waypoints or []) if isinstance(wp, (list, tuple)) and len(wp)==2],
	}
	STATE["orders"].append(entry)
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
                    d["route"] = coords
                    d["target_idx"] = 0
                    d["status"] = "enroute"
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
async def set_weather(w: Dict[str, float]):
	wind = float(w.get("wind_mps", 3.0))
	STATE["weather"] = {"wind_mps": max(0.0, min(40.0, wind))}
	return {"ok": True, "weather": STATE["weather"]}

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
	except Exception:
		logger.exception("Failed to rebuild graph with zones")

async def assign_orders():
	# Try to assign queued orders to available drones or spawn new
	city = STATE.get("city")
	G = STATE.get("city_graph")
	if not city or G is None:
		return
	queue = [o for o in STATE["orders"] if o.get("status") == "queued"]
	for order in queue:
		# Choose drone: nearest idle or create new
		drone_id = pick_drone_for_order(order)
		if drone_id is None:
			# create drone at start
			drone_id = f"drone_{len(STATE['drones'])+1}"
			STATE["drones"][drone_id] = {
				"pos": order["start"],
				"type": map_order_to_drone_type(order["type"]),
				"battery": order["battery_level"],
				"route": [],
				"target_idx": 0,
				"status": "idle",
			}
		# Plan route
		path, coords, length = plan_route_for(order["start"], order["end"], STATE["drones"][drone_id]["type"], STATE["drones"][drone_id]["battery"], waypoints=order.get("waypoints"))
		if coords:
			STATE["drones"][drone_id]["route"] = coords
			STATE["drones"][drone_id]["target_idx"] = 0
			STATE["drones"][drone_id]["status"] = "enroute"
			order["status"] = "assigned"
			order["drone_id"] = drone_id
			order["route_length"] = length

def pick_drone_for_order(order: Dict[str, Any]) -> Optional[str]:
	# pick nearest idle drone; return None if none
	best = None
	best_dist = float('inf')
	for drone_id, d in STATE["drones"].items():
		if d.get("status") in (None, "idle") and d.get("pos"):
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


def simulate_step():
	# move drones along their routes, drain battery, reroute if blocked and avoid collisions
	city = STATE.get("city")
	G = STATE.get("city_graph")
	if not city or G is None:
		return
	for drone_id, drone in STATE["drones"].items():
		if drone.get("status") != "enroute":
			continue
		route = drone.get("route") or []
		idx = drone.get("target_idx", 0)
		if not route or idx >= len(route):
			drone["status"] = "idle"
			continue
		current = drone["pos"]
		target = route[idx]
		# Check if target lies now inside any zone; if so reroute from current to end
		if is_point_in_any_zone(target):
			# attempt reroute
			end = route[-1]
			_, coords, _ = plan_route_for(current, end, drone["type"], drone["battery"])
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
			if drone["target_idx"] >= len(route):
				drone["status"] = "idle"
			else:
				drone["status"] = "enroute"
			battery_drain(drone, dist)
		else:
			# Move fractionally and apply basic collision avoidance
			next_pos = move_towards(current, target, speed_mps / dist)
			if will_collide(drone_id, next_pos):
				# simple sidestep: pause this tick
				drone["status"] = "avoidance"
			else:
				drone["pos"] = next_pos
			battery_drain(drone, speed_mps)
		# simple low battery behavior
		if drone["battery"] <= 5.0:
			drone["status"] = "low_battery"

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

def will_collide(drone_id: str, next_pos: Tuple[float,float]) -> bool:
	for other_id, other in STATE["drones"].items():
		if other_id == drone_id:
			continue
		if haversine_m(next_pos, other.get("pos", next_pos)) < 8.0:  # 8 meters bubble
			return True
	return False


def battery_drain(drone: Dict[str, Any], distance_m: float):
	# very rough: 1% per 2000 m for cargo, 1% per 2500 m operator, 1% per 3000 m cleaner
	per = {
		"cargo": 2000.0,
		"operator": 2500.0,
		"cleaner": 3000.0,
	}.get(drone["type"], 2000.0)
	drain = (distance_m / per) * 100.0
	drone["battery"] = max(0.0, drone["battery"] - drain)


def is_point_in_any_zone(point: Tuple[float, float]) -> bool:
	lat, lon = point
	for z in STATE["no_fly_zones"]:
		lat_min = min(z["lat_min"], z["lat_max"]) ; lat_max = max(z["lat_min"], z["lat_max"])
		lon_min = min(z["lon_min"], z["lon_max"]) ; lon_max = max(z["lon_min"], z["lon_max"])
		if lat_min <= lat <= lat_max and lon_min <= lon <= lon_max:
			return True
	return False


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
