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
	battery_level: float = 100
	waypoints: Optional[List[Dict[str, Any]]] = None  # [{"coords": [lat, lon], "address": "str", "type": "pickup|delivery|waypoint"}]
	drone_id: Optional[str] = None  # указание конкретного дрона
	priority: Optional[int] = 1  # приоритет заказа (1-10, 10 - высший)

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
	"drones": {},  # drone_id -> {pos, type, battery, route, target_idx, status, health, maintenance_hours, last_maintenance}
	"no_fly_zones": [],
	"clients": set(),  # websockets
	"zone_version": 0,
	"weather": {
		"wind_mps": 3.0, 
		"time_of_day": "day", 
		"visibility": 10.0,
		"temperature": 20.0,
		"humidity": 60.0,
		"pressure": 1013.25,
		"cloud_cover": 30.0,
		"precipitation": 0.0,
		"uv_index": 5.0,
		"last_updated": None
	},
    "base": None,  # base location lat, lon
	"inventory": {},  # type -> count available at base
	"stations": [],  # list of (lat, lon)
    "station_queues": {},  # station_index -> {charging:[], queue:[], capacity:int}
    "base_queue": {"charging": [], "queue": [], "capacity": 2},
    "order_queues": {},  # type -> [orders] - очереди по типам заказов
    "route_optimization": True,  # включена ли оптимизация маршрутов
}

# Helpers
async def broadcast_state():
	payload = {
		"city": STATE["city"],
		"orders": STATE["orders"],
		"drones": STATE["drones"],
		"histories": {k: v.get("history", []) for k,v in STATE["drones"].items()},
		"no_fly_zones": STATE["no_fly_zones"],
		"stations": STATE.get("stations", []),
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

async def weather_update_loop():
	"""Автообновление погоды каждую минуту"""
	while True:
		try:
			city = STATE.get("city")
			if city:
				weather_data = await get_real_weather_data(city)
				STATE["weather"] = weather_data
				await persist_state()
				logger.info(f"Погода обновлена для {city}")
			await asyncio.sleep(60.0)  # обновляем каждую минуту
		except Exception:
			logger.exception("weather_update_loop error")
			await asyncio.sleep(60.0)

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
	asyncio.create_task(weather_update_loop())

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
		"route_optimization": STATE.get("route_optimization", True),
		"order_queues": {
			order_type: len(orders) 
			for order_type, orders in STATE.get("order_queues", {}).items()
		}
	}

@app.get("/api/system_status")
async def get_system_status():
	"""Получение детального статуса системы"""
	try:
		# Статистика дронов
		drone_stats = {}
		for drone_type in ["cargo", "operator", "cleaner"]:
			drones_of_type = [d for d in STATE["drones"].values() if d.get("type") == drone_type]
			drone_stats[drone_type] = {
				"total": len(drones_of_type),
				"idle": len([d for d in drones_of_type if d.get("status") == "idle"]),
				"enroute": len([d for d in drones_of_type if d.get("status") == "enroute"]),
				"charging": len([d for d in drones_of_type if d.get("status") in ("charging", "charging_with_cargo")]),
				"malfunction": len([d for d in drones_of_type if d.get("status") == "malfunction"]),
				"maintenance": len([d for d in drones_of_type if d.get("status") in ("needs_maintenance", "maintenance")])
			}
		
		# Статистика заказов
		order_stats = {
			"total": len(STATE["orders"]),
			"queued": len([o for o in STATE["orders"] if o.get("status") == "queued"]),
			"assigned": len([o for o in STATE["orders"] if o.get("status") == "assigned"]),
			"completed": len([o for o in STATE["orders"] if o.get("status") == "completed"]),
			"cancelled": len([o for o in STATE["orders"] if o.get("status") == "cancelled"])
		}
		
		# Статистика очередей
		queue_stats = {}
		for order_type, orders in STATE.get("order_queues", {}).items():
			queue_stats[order_type] = {
				"count": len(orders),
				"avg_priority": sum(o.get("priority", 1) for o in orders) / len(orders) if orders else 0
			}
		
		return {
			"ok": True,
			"drone_stats": drone_stats,
			"order_stats": order_stats,
			"queue_stats": queue_stats,
			"weather": STATE["weather"],
			"route_optimization": STATE.get("route_optimization", True),
			"timestamp": datetime.utcnow().isoformat()
		}
		
	except Exception as e:
		logger.error(f"Ошибка получения статуса системы: {e}")
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

@app.post("/api/no_fly_zones/clear")
async def clear_all_no_fly_zones():
	"""Очистка всех бесполетных зон"""
	cleared_count = len(STATE["no_fly_zones"])
	STATE["no_fly_zones"] = []
	STATE["zone_version"] += 1
	await rebuild_graph_with_zones()
	await persist_state()
	return {"ok": True, "cleared": cleared_count}

@app.post("/api/stations/clear")
async def clear_all_stations():
	"""Очистка всех станций зарядки"""
	cleared_count = len(STATE["stations"])
	STATE["stations"] = []
	STATE["station_queues"] = {}
	await persist_state()
	return {"ok": True, "cleared": cleared_count}

@app.delete("/api/stations/{station_index}")
async def remove_station(station_index: int):
	"""Удаление конкретной станции зарядки"""
	try:
		station_index = int(station_index)
		if 0 <= station_index < len(STATE["stations"]):
			removed_station = STATE["stations"].pop(station_index)
			# Удаляем соответствующую очередь
			if str(station_index) in STATE["station_queues"]:
				del STATE["station_queues"][str(station_index)]
			# Перенумеровываем оставшиеся очереди
			new_queues = {}
			for i, station in enumerate(STATE["stations"]):
				old_key = str(i + 1) if i >= station_index else str(i)
				if old_key in STATE["station_queues"]:
					new_queues[str(i)] = STATE["station_queues"][old_key]
			STATE["station_queues"] = new_queues
			await persist_state()
			return {"ok": True, "removed": removed_station}
		else:
			return JSONResponse(status_code=404, content={"ok": False, "error": "Station not found"})
	except Exception as e:
		return JSONResponse(status_code=400, content={"ok": False, "error": str(e)})

@app.post("/api/orders")
async def add_order(order: AddOrderRequest):
	# Classify order type
	order_type = classify_order(order)
	start = await resolve_point(order.coords_from, order.address_from)
	end = await resolve_point(order.coords_to, order.address_to)
	if not start or not end:
		return JSONResponse(status_code=400, content={"ok": False, "error": "Invalid start or end"})
	
	# Проверяем, существует ли указанный дрон
	assigned_drone_id = None
	if order.drone_id:
		if order.drone_id in STATE["drones"]:
			assigned_drone_id = order.drone_id
		else:
			return JSONResponse(status_code=400, content={"ok": False, "error": f"Drone {order.drone_id} not found"})
	
	# Обрабатываем промежуточные точки
	processed_waypoints = []
	if order.waypoints:
		for wp in order.waypoints:
			if isinstance(wp, dict):
				coords = wp.get("coords")
				address = wp.get("address")
				wp_type = wp.get("type", "waypoint")
				
				# Разрешаем координаты
				resolved_coords = await resolve_point(coords, address)
				if resolved_coords:
					processed_waypoints.append({
						"coords": resolved_coords,
						"address": address,
						"type": wp_type
					})
			elif isinstance(wp, (list, tuple)) and len(wp) == 2:
				# Старый формат для обратной совместимости
				processed_waypoints.append({
					"coords": tuple(wp),
					"address": None,
					"type": "waypoint"
				})
	
	order_id = f"ord_{len(STATE['orders'])+1}"
	entry = {
		"id": order_id,
		"type": order_type,
		"start": start,
		"end": end,
		"battery_level": max(1.0, min(100.0, float(order.battery_level))),
		"status": "queued",
		"waypoints": processed_waypoints,
		"drone_id": assigned_drone_id,
		"priority": max(1, min(10, order.priority or 1)),
		"created_at": datetime.utcnow().isoformat(),
	}
	
	# Добавляем в общий список заказов
	STATE["orders"].append(entry)
	
	# Добавляем в соответствующую очередь для оптимизации
	add_order_to_queue(entry)
	
	await persist_state()
	return {"ok": True, "order": entry, "queue_size": len(STATE["orders"]) }

@app.get("/api/orders")
async def list_orders():
	return STATE["orders"]

@app.get("/api/order_queues")
async def get_order_queues():
	"""Получение информации об очередях заказов"""
	queues_info = {}
	for order_type, orders in STATE.get("order_queues", {}).items():
		queues_info[order_type] = {
			"count": len(orders),
			"orders": orders[:10],  # Показываем только первые 10 заказов
			"type_display": {
				"delivery": "Доставка",
				"shooting": "Съемка", 
				"work": "Работа"
			}.get(order_type, order_type)
		}
	return queues_info

@app.post("/api/order_queues/{order_type}/optimize")
async def optimize_order_queue_endpoint(order_type: str):
	"""Оптимизация очереди заказов определенного типа"""
	if order_type not in STATE.get("order_queues", {}):
		return JSONResponse(status_code=404, content={"ok": False, "error": "Queue not found"})
	
	optimized_orders = optimize_order_queue(order_type)
	STATE["order_queues"][order_type] = optimized_orders
	await persist_state()
	
	return {"ok": True, "optimized_count": len(optimized_orders)}

@app.post("/api/route_optimization")
async def toggle_route_optimization(enabled: Dict[str, bool]):
	"""Включение/выключение оптимизации маршрутов"""
	STATE["route_optimization"] = enabled.get("enabled", True)
	await persist_state()
	return {"ok": True, "route_optimization": STATE["route_optimization"]}

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
	time_of_day = w.get("time_of_day", "day")
	visibility = float(w.get("visibility", 10.0))
	temperature = float(w.get("temperature", 20.0))
	humidity = float(w.get("humidity", 60.0))
	pressure = float(w.get("pressure", 1013.25))
	cloud_cover = float(w.get("cloud_cover", 30.0))
	precipitation = float(w.get("precipitation", 0.0))
	uv_index = float(w.get("uv_index", 5.0))
	
	STATE["weather"] = {
		"wind_mps": max(0.0, min(40.0, wind)),
		"time_of_day": time_of_day if time_of_day in ["day", "night", "dawn", "dusk"] else "day",
		"visibility": max(0.0, min(10.0, visibility)),
		"temperature": max(-50.0, min(60.0, temperature)),
		"humidity": max(0.0, min(100.0, humidity)),
		"pressure": max(800.0, min(1100.0, pressure)),
		"cloud_cover": max(0.0, min(100.0, cloud_cover)),
		"precipitation": max(0.0, min(100.0, precipitation)),
		"uv_index": max(0.0, min(15.0, uv_index)),
		"last_updated": datetime.utcnow().isoformat()
	}
	await persist_state()
	return {"ok": True, "weather": STATE["weather"]}

@app.get("/api/weather")
async def get_weather():
	"""Получение текущей погоды"""
	return {
		"ok": True,
		"weather": STATE["weather"],
		"weather_impact": calculate_weather_impact()
	}

@app.post("/api/weather/update")
async def update_weather_for_city():
	"""Обновляет погоду для текущего города"""
	city = STATE.get("city")
	if not city:
		return {"ok": False, "error": "Город не загружен"}
	
	try:
		# Получаем реальные данные погоды для города
		weather_data = await get_real_weather_data(city)
		STATE["weather"] = weather_data
		await persist_state()
		return {"ok": True, "weather": STATE["weather"]}
	except Exception as e:
		logger.error(f"Ошибка получения погоды для {city}: {e}")
		return {"ok": False, "error": str(e)}

async def get_real_weather_data(city: str) -> Dict[str, Any]:
	"""Получает реальные данные погоды для города"""
	import random
	
	try:
		# Для демонстрации используем случайные данные, но реалистичные для города
		# В реальном проекте здесь был бы вызов к OpenWeatherMap API
		
		# Получаем координаты города для более точных данных
		coords = _data_service.address_to_coords(city)
		if not coords:
			coords = [48.7080, 44.5133]  # Волгоград по умолчанию
		
		# Генерируем реалистичные данные погоды
		base_temp = 20.0
		if "москва" in city.lower() or "moscow" in city.lower():
			base_temp = 15.0
		elif "волгоград" in city.lower() or "volgograd" in city.lower():
			base_temp = 22.0
		elif "санкт" in city.lower() or "petersburg" in city.lower():
			base_temp = 12.0
		
		# Добавляем случайные вариации
		temperature = base_temp + random.uniform(-5, 10)
		humidity = random.uniform(40, 80)
		wind_speed = random.uniform(1, 8)
		pressure = random.uniform(1000, 1030)
		cloud_cover = random.uniform(10, 90)
		precipitation = random.uniform(0, 20) if random.random() < 0.3 else 0
		visibility = random.uniform(5, 15)
		
		# Определяем время дня
		now = datetime.utcnow()
		hour = now.hour
		if 6 <= hour < 12:
			time_of_day = "day"
		elif 12 <= hour < 18:
			time_of_day = "day"
		elif 18 <= hour < 22:
			time_of_day = "dusk"
		else:
			time_of_day = "night"
		
		return {
			"wind_mps": round(wind_speed, 1),
			"time_of_day": time_of_day,
			"visibility": round(visibility, 1),
			"temperature": round(temperature, 1),
			"humidity": round(humidity, 1),
			"pressure": round(pressure, 1),
			"cloud_cover": round(cloud_cover, 1),
			"precipitation": round(precipitation, 1),
			"uv_index": round(random.uniform(1, 8), 1),
			"last_updated": datetime.utcnow().isoformat(),
			"city": city
		}
	except Exception as e:
		logger.error(f"Ошибка получения погоды: {e}")
		# Возвращаем данные по умолчанию
		return {
			"wind_mps": 3.0,
			"time_of_day": "day",
			"visibility": 10.0,
			"temperature": 20.0,
			"humidity": 60.0,
			"pressure": 1013.25,
			"cloud_cover": 30.0,
			"precipitation": 0.0,
			"uv_index": 5.0,
			"last_updated": datetime.utcnow().isoformat(),
			"city": city
		}

def calculate_weather_impact() -> Dict[str, Any]:
	"""Вычисляет влияние погоды на полеты дронов"""
	try:
		weather = STATE.get("weather", {})
		
		# Оценка безопасности полетов (0-100, где 100 - идеальные условия)
		safety_score = 100.0
		
		# Влияние ветра
		wind = weather.get("wind_mps", 3.0)
		if wind > 25.0:
			safety_score -= 50  # Очень сильный ветер
		elif wind > 15.0:
			safety_score -= 30  # Сильный ветер
		elif wind > 10.0:
			safety_score -= 15  # Умеренный ветер
		
		# Влияние видимости
		visibility = weather.get("visibility", 10.0)
		if visibility < 1.0:
			safety_score -= 40  # Очень плохая видимость
		elif visibility < 3.0:
			safety_score -= 25  # Плохая видимость
		elif visibility < 5.0:
			safety_score -= 10  # Сниженная видимость
		
		# Влияние осадков
		precipitation = weather.get("precipitation", 0.0)
		if precipitation > 50.0:
			safety_score -= 30  # Сильные осадки
		elif precipitation > 20.0:
			safety_score -= 15  # Умеренные осадки
		elif precipitation > 5.0:
			safety_score -= 5   # Легкие осадки
		
		# Влияние времени дня
		time_of_day = weather.get("time_of_day", "day")
		if time_of_day == "night":
			safety_score -= 10  # Ночные полеты сложнее
		elif time_of_day in ["dawn", "dusk"]:
			safety_score -= 5   # Сумерки
		
		# Влияние облачности
		cloud_cover = weather.get("cloud_cover", 30.0)
		if cloud_cover > 90.0:
			safety_score -= 10  # Очень облачно
		elif cloud_cover > 70.0:
			safety_score -= 5   # Облачно
		
		safety_score = max(0.0, min(100.0, safety_score))
		
		# Определяем рекомендации
		recommendations = []
		if wind > 15.0:
			recommendations.append("Ограничить полеты из-за сильного ветра")
		if visibility < 3.0:
			recommendations.append("Отложить полеты из-за плохой видимости")
		if precipitation > 20.0:
			recommendations.append("Избегать полетов во время осадков")
		if time_of_day == "night":
			recommendations.append("Использовать дополнительное освещение")
		
		return {
			"safety_score": round(safety_score, 1),
			"flight_recommended": safety_score >= 70.0,
			"recommendations": recommendations,
			"wind_impact": "high" if wind > 15.0 else "medium" if wind > 8.0 else "low",
			"visibility_impact": "high" if visibility < 3.0 else "medium" if visibility < 5.0 else "low"
		}
		
	except Exception as e:
		logger.error(f"Ошибка вычисления влияния погоды: {e}")
		return {
			"safety_score": 50.0,
			"flight_recommended": False,
			"recommendations": ["Ошибка анализа погодных условий"],
			"wind_impact": "unknown",
			"visibility_impact": "unknown"
		}

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

@app.get("/api/drones")
async def get_drones():
    """Получение списка всех дронов с их состоянием"""
    drones_info = {}
    for drone_id, drone in STATE["drones"].items():
        drone_type = drone.get("type", "cargo")
        drones_info[drone_id] = {
            "pos": drone.get("pos"),
            "type": drone_type,
            "type_display": get_drone_type_display_name(drone_type),
            "shape": get_drone_shape(drone_type),
            "color": get_drone_color(drone_type),
            "battery": drone.get("battery", 0),
            "status": drone.get("status"),
            "health": drone.get("health", 100),
            "status_text": get_drone_status_text(drone),
            "maintenance_hours": drone.get("maintenance_hours", 0),
            "last_maintenance": drone.get("last_maintenance"),
            "temp_c": drone.get("temp_c", 35),
            "link_quality": drone.get("link_quality", 0.8),
            "remaining_m": drone.get("remaining_m", 0),
            "eta_s": drone.get("eta_s", 0),
            "has_cargo": drone.get("has_cargo", False),
            "cargo_type": drone.get("cargo_type"),
            "cargo_weight": drone.get("cargo_weight", 0),
        }
    return drones_info

@app.get("/api/drone_types")
async def get_drone_types():
    """Получение доступных типов дронов"""
    available = get_available_drone_types()
    types_info = {}
    for drone_type, count in available.items():
        types_info[drone_type] = {
            "display_name": get_drone_type_display_name(drone_type),
            "available_count": count,
            "active_count": len([d for d in STATE["drones"].values() if d.get("type") == drone_type]),
            "shape": get_drone_shape(drone_type),
            "color": get_drone_color(drone_type)
        }
    return types_info

def get_drone_shape(drone_type: str) -> str:
    """Получает фигуру для отображения дрона на карте"""
    shapes = {
        "cargo": "circle",
        "operator": "square", 
        "cleaner": "triangle"
    }
    return shapes.get(drone_type, "circle")

def get_drone_color(drone_type: str) -> str:
    """Получает цвет для отображения дрона на карте"""
    colors = {
        "cargo": "#FF6B6B",  # Красный
        "operator": "#4ECDC4",  # Бирюзовый
        "cleaner": "#45B7D1"  # Синий
    }
    return colors.get(drone_type, "#95A5A6")  # Серый по умолчанию

@app.post("/api/drones/{drone_id}/repair")
async def repair_drone_endpoint(drone_id: str):
    """Ремонт дрона"""
    success = repair_drone(drone_id)
    if success:
        return {"ok": True, "message": f"Дрон {drone_id} отремонтирован"}
    else:
        return JSONResponse(status_code=404, content={"ok": False, "error": "Дрон не найден"})

@app.get("/api/drones/{drone_id}/status")
async def get_drone_status(drone_id: str):
    """Получение детального статуса дрона"""
    drone = STATE["drones"].get(drone_id)
    if not drone:
        return JSONResponse(status_code=404, content={"ok": False, "error": "Дрон не найден"})
    
    return {
        "ok": True,
        "drone_id": drone_id,
        "status": drone.get("status"),
        "health": drone.get("health", 100),
        "battery": drone.get("battery", 0),
        "status_text": get_drone_status_text(drone),
        "maintenance_hours": drone.get("maintenance_hours", 0),
        "last_maintenance": drone.get("last_maintenance"),
        "pos": drone.get("pos"),
        "type": drone.get("type"),
        "temp_c": drone.get("temp_c", 35),
        "link_quality": drone.get("link_quality", 0.8),
        "remaining_m": drone.get("remaining_m", 0),
        "eta_s": drone.get("eta_s", 0),
    }

class MultiPointRouteRequest(BaseModel):
    points: List[Dict[str, Any]]  # [{"coords": [lat, lon], "address": "str", "type": "pickup|delivery|waypoint"}]
    drone_id: Optional[str] = None
    drone_type: str = "cargo"
    battery_level: float = 100
    priority: Optional[int] = 1

@app.post("/api/routes/multi_point")
async def create_multi_point_route(route: MultiPointRouteRequest):
    """Создание маршрута с несколькими пунктами"""
    try:
        if len(route.points) < 2:
            return JSONResponse(status_code=400, content={"ok": False, "error": "Необходимо минимум 2 пункта"})
        
        # Разрешаем координаты для всех пунктов
        resolved_points = []
        for i, point in enumerate(route.points):
            coords = point.get("coords")
            address = point.get("address")
            point_type = point.get("type", "waypoint")
            
            resolved_coords = await resolve_point(coords, address)
            if not resolved_coords:
                return JSONResponse(status_code=400, content={"ok": False, "error": f"Не удалось разрешить пункт {i+1}"})
            
            resolved_points.append({
                "coords": resolved_coords,
                "type": point_type,
                "address": address
            })
        
        # Создаем заказы для каждого сегмента маршрута
        created_orders = []
        for i in range(len(resolved_points) - 1):
            start_point = resolved_points[i]
            end_point = resolved_points[i + 1]
            
            order_id = f"ord_{len(STATE['orders'])+1}"
            order_entry = {
                "id": order_id,
                "type": "delivery" if end_point["type"] == "delivery" else "work",
                "start": start_point["coords"],
                "end": end_point["coords"],
                "battery_level": route.battery_level,
                "status": "queued",
                "waypoints": [],
                "drone_id": route.drone_id,
                "priority": route.priority,
                "created_at": datetime.utcnow().isoformat(),
                "route_segment": i + 1,
                "total_segments": len(resolved_points) - 1,
                "multi_route_id": f"multi_{len(STATE['orders'])+1}"
            }
            
            STATE["orders"].append(order_entry)
            created_orders.append(order_entry)
        
        await persist_state()
        return {
            "ok": True, 
            "orders": created_orders, 
            "total_segments": len(resolved_points) - 1,
            "message": f"Создан маршрут с {len(resolved_points)} пунктами"
        }
        
    except Exception as e:
        logger.error(f"Ошибка создания многопунктного маршрута: {e}")
        return JSONResponse(status_code=500, content={"ok": False, "error": str(e)})

@app.get("/api/routes/multi_point/{multi_route_id}")
async def get_multi_point_route_status(multi_route_id: str):
    """Получение статуса многопунктного маршрута"""
    try:
        orders = [o for o in STATE["orders"] if o.get("multi_route_id") == multi_route_id]
        if not orders:
            return JSONResponse(status_code=404, content={"ok": False, "error": "Маршрут не найден"})
        
        # Сортируем по сегментам
        orders.sort(key=lambda x: x.get("route_segment", 0))
        
        return {
            "ok": True,
            "multi_route_id": multi_route_id,
            "orders": orders,
            "total_segments": len(orders),
            "completed_segments": len([o for o in orders if o.get("status") == "completed"]),
            "status": "completed" if all(o.get("status") == "completed" for o in orders) else "in_progress"
        }
        
    except Exception as e:
        logger.error(f"Ошибка получения статуса маршрута: {e}")
        return JSONResponse(status_code=500, content={"ok": False, "error": str(e)})

class WaypointRequest(BaseModel):
    coords: Optional[List[float]] = None
    address: Optional[str] = None
    type: str = "waypoint"  # pickup|delivery|waypoint
    description: Optional[str] = None

@app.post("/api/waypoints/resolve")
async def resolve_waypoints(waypoints: List[WaypointRequest]):
    """Разрешение промежуточных точек в координаты"""
    try:
        resolved_waypoints = []
        
        for i, wp in enumerate(waypoints):
            coords = wp.coords
            address = wp.address
            wp_type = wp.type
            description = wp.description
            
            # Разрешаем координаты
            resolved_coords = await resolve_point(coords, address)
            if not resolved_coords:
                return JSONResponse(
                    status_code=400, 
                    content={"ok": False, "error": f"Не удалось разрешить точку {i+1}"}
                )
            
            resolved_waypoints.append({
                "coords": resolved_coords,
                "address": address,
                "type": wp_type,
                "description": description
            })
        
        return {
            "ok": True,
            "waypoints": resolved_waypoints,
            "count": len(resolved_waypoints)
        }
        
    except Exception as e:
        logger.error(f"Ошибка разрешения промежуточных точек: {e}")
        return JSONResponse(status_code=500, content={"ok": False, "error": str(e)})

@app.get("/api/waypoints/types")
async def get_waypoint_types():
    """Получение доступных типов промежуточных точек"""
    return {
        "ok": True,
        "types": {
            "pickup": {
                "name": "Забор груза",
                "description": "Точка забора груза или пассажира",
                "icon": "📦",
                "color": "#FF6B6B"
            },
            "delivery": {
                "name": "Доставка",
                "description": "Точка доставки груза",
                "icon": "🚚",
                "color": "#4ECDC4"
            },
            "waypoint": {
                "name": "Промежуточная точка",
                "description": "Обычная промежуточная точка маршрута",
                "icon": "📍",
                "color": "#95A5A6"
            },
            "inspection": {
                "name": "Инспекция",
                "description": "Точка для осмотра или проверки",
                "icon": "🔍",
                "color": "#F39C12"
            },
            "charging": {
                "name": "Зарядка",
                "description": "Точка зарядки дрона",
                "icon": "🔋",
                "color": "#9B59B6"
            }
        }
    }

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
	# Try to assign queued orders to available drones or spawn new
	city = STATE.get("city")
	G = STATE.get("city_graph")
	if not city or G is None:
		return
	
	# Обрабатываем заказы по типам
	for order_type in ["delivery", "shooting", "work"]:
		# Получаем следующий заказ из оптимизированной очереди
		order = get_next_order_from_queue(order_type)
		if not order:
			continue
		
		# Проверяем, что заказ еще в статусе "queued"
		if order.get("status") != "queued":
			continue
		# Если указан конкретный дрон, используем его
		if order.get("drone_id"):
			drone_id = order["drone_id"]
			drone = STATE["drones"].get(drone_id)
			if not drone:
				logger.warning(f"Указанный дрон {drone_id} не найден для заказа {order['id']}")
				continue
			
			# Проверяем, что дрон свободен и в хорошем состоянии
			if (drone.get("status") not in ("idle", "charging") or 
			    drone.get("health", 100) < 50 or 
			    drone.get("battery", 0) < 20):
				logger.warning(f"Указанный дрон {drone_id} недоступен для заказа {order['id']}")
				continue
		else:
			# Определяем требуемый тип дрона
			required_type = map_order_to_drone_type(order["type"])
			
			# Ищем доступного дрона нужного типа
			drone_id = pick_drone_for_order_by_type(order, required_type)
			if drone_id is None:
				logger.warning(f"Нет доступных дронов типа {required_type} для заказа {order['id']}. Заказ остается в очереди.")
				continue
		
		# Plan route with energy-aware via base/stations
		waypoints = order.get("waypoints", [])
		path, coords, length = plan_via_base_if_needed(order["start"], order["end"], STATE["drones"][drone_id]["type"], STATE["drones"][drone_id]["battery"])
		if coords:
			# Ensure the drone stops at the first charger on the way and charges before continuing
			apply_midroute_charging(STATE["drones"][drone_id], coords)
			order["status"] = "assigned"
			order["drone_id"] = drone_id
			order["route_length"] = length
			logger.info(f"Заказ {order['id']} назначен дрону {drone_id} типа {STATE['drones'][drone_id]['type']}")

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

def pick_drone_for_order_by_type(order: Dict[str, Any], required_type: str) -> Optional[str]:
	"""Выбирает ближайшего свободного дрона определенного типа"""
	best = None
	best_dist = float('inf')
	
	for drone_id, d in STATE["drones"].items():
		# Проверяем тип дрона
		if d.get("type") != required_type:
			continue
			
		# Проверяем статус и состояние
		if (d.get("status") in (None, "idle", "charging") and 
		    d.get("pos") and 
		    d.get("health", 100) > 50 and 
		    d.get("battery", 0) > 20):
			
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
	
	# Применяем погодные условия к графу
	G = apply_weather_conditions(G, drone_type)
	
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
    # naive: if remaining straight route exceeds 60% of max range, insert base as waypoint
    _, coords_direct, length_direct = plan_route_for(current, end, drone_type, battery_level)
    if not coords_direct:
        return None, None, 0.0
    # energy-aware: account for consumption; require 20% reserve to nearest station/base after delivery
    per = {
        "cargo": 2000.0,
        "operator": 2500.0,
        "cleaner": 3000.0,
    }.get(drone_type, 2000.0)
    usable_range = per * (battery_level/100.0) * 0.9  # 10% enroute headroom
    if length_direct <= usable_range and can_escape_after(end, drone_type):
        return None, coords_direct, length_direct
    # candidates: base + stations
    candidates: List[Tuple[float,float]] = []
    if base:
        candidates.append(tuple(base))
    for s in STATE.get("stations", []):
        try:
            candidates.append(tuple(s))
        except Exception:
            pass
    # pick candidate with feasible legs current->cand and cand->end
    best = None
    best_len = float('inf')
    for cand in candidates:
        _, c1, l1 = plan_route_for(current, cand, drone_type, battery_level)
        if not c1:
            continue
        # Assume charge at station to 100%
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
		# Проверяем здоровье дрона
		if not check_drone_health(drone_id, drone):
			# Если дрон сломался, пытаемся передать заказ другому дрону
			handover_order_if_needed(drone_id)
			continue
		
		# Если дрон требует обслуживания, направляем на базу
		if drone.get("status") == "needs_maintenance":
			maybe_route_to_base_or_station(drone)
			continue
		
		if drone.get("status") != "enroute":
			continue
		route = drone.get("route") or []
		idx = drone.get("target_idx", 0)
		if not route or idx >= len(route):
			# Order considered complete if present
			drone["status"] = "idle"
			mark_order_completed_if_any(drone_id)
			
			# Проверяем, есть ли у дрона груз
			has_cargo = check_drone_has_cargo(drone_id)
			
			# If low battery after completing, head to base or station
			if drone.get("battery", 100.0) < 30.0:
				if has_cargo:
					# Дрон с грузом летит на станцию зарядки
					route_to_charging_station_with_cargo(drone)
				else:
					maybe_route_to_base_or_station(drone)
			else:
				# Если батарея в порядке, строим оптимальный маршрут до базы
				route_to_base_optimal(drone_id)
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
			if drone["target_idx"] >= len(route):
				# If arrived at station/base, join queue or charge
				if is_at_any_station(drone["pos"]) or (STATE.get("base") and haversine_m(drone["pos"], tuple(STATE["base"])) < 20.0):
					assign_to_charger_queue(drone_id)
					drone["status"] = "charging"
				else:
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
		# link quality estimation vs. nearest base or last strong point
		compute_link_quality(drone)
		# low battery behavior: route to nearest charger when under thresholds
		if drone["battery"] <= 5.0:
			# Проверяем, можем ли долететь до ближайшей станции зарядки
			if can_reach_charging_station(drone):
				drone["status"] = "emergency_charging"
				maybe_route_to_base_or_station(drone)
			else:
				# Если не можем долететь, пытаемся передать заказ
				handover_order_if_needed(drone_id)
				drone["status"] = "low_battery"
		elif drone["battery"] <= 20.0 and drone.get("status") in ("idle", "holding"):
			maybe_route_to_base_or_station(drone)
		elif drone["battery"] <= 15.0 and drone.get("status") == "enroute":
			# При очень низком заряде во время полета пытаемся передать заказ
			if not can_reach_charging_station(drone):
				handover_order_if_needed(drone_id)

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
                # Проверяем, есть ли у дрона груз
                if d.get("has_cargo", False):
                    # Дрон с грузом продолжает маршрут
                    resume = d.get("resume_route") or []
                    if resume:
                        d["route"] = list(resume)
                        d["resume_route"] = []
                        d["target_idx"] = 0
                        d["status"] = "enroute"
                    else:
                        d["status"] = "idle"
                else:
                    # Обычный дрон
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

def check_drone_health(drone_id: str, drone: Dict[str, Any]):
    """Проверка состояния дрона и обновление здоровья"""
    try:
        # Увеличиваем часы работы
        drone["maintenance_hours"] += 1.0 / 3600.0  # 1 секунда = 1/3600 часа
        
        # Проверяем вероятность поломки
        import random
        if random.random() < drone["malfunction_probability"] * (1.0 / 3600.0):  # за секунду
            drone["health"] = max(0.0, drone["health"] - random.uniform(5.0, 15.0))
            if drone["health"] <= 0:
                drone["status"] = "malfunction"
                drone["health"] = 0.0
                logger.warning(f"Дрон {drone_id} сломался!")
                return False
        
        # Ухудшение здоровья со временем
        if drone["maintenance_hours"] > 100:  # после 100 часов работы
            health_degradation = (drone["maintenance_hours"] - 100) * 0.1
            drone["health"] = max(0.0, 100.0 - health_degradation)
        
        # Если здоровье критически низкое
        if drone["health"] < 20.0 and drone["status"] not in ["malfunction", "maintenance"]:
            drone["status"] = "needs_maintenance"
            logger.warning(f"Дрон {drone_id} требует обслуживания! Здоровье: {drone['health']:.1f}%")
        
        return drone["health"] > 0
        
    except Exception as e:
        logger.error(f"Ошибка проверки здоровья дрона {drone_id}: {e}")
        return True

def get_drone_status_text(drone: Dict[str, Any]) -> str:
    """Получение текстового описания состояния дрона"""
    if drone["health"] <= 0:
        return "Поломка"
    elif drone["status"] == "malfunction":
        return "Поломка"
    elif drone["status"] == "needs_maintenance":
        return "Требует обслуживания"
    elif drone["status"] == "maintenance":
        return "На обслуживании"
    elif drone["status"] == "emergency_charging":
        return "Экстренная зарядка"
    elif drone["status"] == "charging_with_cargo":
        return "Зарядка с грузом"
    elif drone["status"] == "returning_to_base":
        return "Возвращается на базу"
    elif drone.get("has_cargo", False):
        return "С грузом"
    elif drone["battery"] <= 5:
        return "Критически низкий заряд"
    elif drone["battery"] <= 20:
        return "Низкий заряд"
    elif drone["health"] < 50:
        return "Плохое состояние"
    elif drone["health"] < 80:
        return "Удовлетворительное состояние"
    else:
        return "В норме"

def repair_drone(drone_id: str) -> bool:
    """Ремонт дрона"""
    try:
        drone = STATE["drones"].get(drone_id)
        if not drone:
            return False
        
        drone["health"] = 100.0
        drone["maintenance_hours"] = 0.0
        drone["last_maintenance"] = datetime.utcnow().isoformat()
        drone["status"] = "idle"
        logger.info(f"Дрон {drone_id} отремонтирован")
        return True
    except Exception as e:
        logger.error(f"Ошибка ремонта дрона {drone_id}: {e}")
        return False

def apply_weather_conditions(G, drone_type: str):
    """Применяет погодные условия к графу для планирования маршрута"""
    try:
        import copy
        G = copy.deepcopy(G)  # Создаем копию графа
        
        weather = STATE.get("weather", {})
        wind_mps = weather.get("wind_mps", 3.0)
        time_of_day = weather.get("time_of_day", "day")
        visibility = weather.get("visibility", 10.0)
        
        # Получаем параметры дрона
        drone_params = {
            "cargo": {"max_altitude": 200, "wind_resistance": 0.8},
            "operator": {"max_altitude": 150, "wind_resistance": 0.6},
            "cleaner": {"max_altitude": 100, "wind_resistance": 0.4}
        }.get(drone_type, {"max_altitude": 200, "wind_resistance": 0.8})
        
        # Применяем ветровые условия
        wind_factor = 1.0 + (wind_mps * 0.1 * (1.0 - drone_params["wind_resistance"]))
        
        # Применяем условия видимости
        visibility_factor = 1.0 + (10.0 - visibility) * 0.05
        
        # Применяем время дня
        time_factor = 1.0
        if time_of_day == "night":
            time_factor = 1.2  # Ночью сложнее летать
        elif time_of_day == "dawn" or time_of_day == "dusk":
            time_factor = 1.1  # В сумерках немного сложнее
        
        # Обновляем веса рёбер
        for u, v, data in G.edges(data=True):
            if 'weight' in data:
                # Увеличиваем вес рёбер в зависимости от условий
                data['weight'] *= wind_factor * visibility_factor * time_factor
                
                # Блокируем рёбра при экстремальных условиях
                if wind_mps > 25.0:  # Сильный ветер
                    data['weight'] = float('inf')
                elif visibility < 1.0:  # Очень плохая видимость
                    data['weight'] = float('inf')
        
        # Применяем ограничения по высоте зданий
        G = apply_building_height_restrictions(G, drone_params["max_altitude"])
        
        return G
        
    except Exception as e:
        logger.error(f"Ошибка применения погодных условий: {e}")
        return G

def apply_building_height_restrictions(G, max_altitude: float):
    """Применяет ограничения по высоте зданий"""
    try:
        # Получаем данные о зданиях из графа
        for node, attr in G.nodes(data=True):
            if attr.get('type') == 'cleaner' and 'building_id' in attr:
                # Для дронов-уборщиков проверяем высоту зданий
                building_height = estimate_building_height(attr.get('building_id', 0))
                if building_height > max_altitude:
                    # Блокируем узел если здание слишком высокое
                    attr['weight'] = float('inf')
        
        return G
        
    except Exception as e:
        logger.error(f"Ошибка применения ограничений по высоте: {e}")
        return G

def estimate_building_height(building_id: int) -> float:
    """Оценивает высоту здания (заглушка)"""
    import random
    # В реальной реализации здесь была бы логика получения высоты здания
    # Пока используем случайную высоту от 10 до 200 метров
    return random.uniform(10.0, 200.0)

def check_drone_has_cargo(drone_id: str) -> bool:
    """Проверяет, есть ли у дрона груз"""
    try:
        drone = STATE["drones"].get(drone_id)
        if not drone:
            return False
        
        # Ищем активные заказы доставки для этого дрона
        for order in STATE.get("orders", []):
            if (order.get("drone_id") == drone_id and 
                order.get("status") == "assigned" and 
                order.get("type") == "delivery"):
                return True
        
        # Проверяем, есть ли груз в данных дрона
        return drone.get("has_cargo", False)
        
    except Exception as e:
        logger.error(f"Ошибка проверки груза дрона {drone_id}: {e}")
        return False

def route_to_charging_station_with_cargo(drone: Dict[str, Any]):
    """Направляет дрон с грузом на станцию зарядки"""
    try:
        # Ищем ближайшую станцию зарядки (не базу, так как там может не быть места для груза)
        stations = STATE.get("stations", [])
        if not stations:
            # Если нет станций, летим на базу
            maybe_route_to_base_or_station(drone)
            return
        
        pos = tuple(drone.get("pos", (0, 0)))
        best_station = None
        best_distance = float('inf')
        
        for station in stations:
            distance = haversine_m(pos, tuple(station))
            if distance < best_distance:
                best_distance = distance
                best_station = station
        
        if best_station:
            _, coords, _ = plan_route_for(pos, tuple(best_station), drone["type"], drone["battery"])
            if coords:
                drone["route"] = coords
                drone["target_idx"] = 0
                drone["status"] = "charging_with_cargo"
                logger.info(f"Дрон {drone.get('id', 'unknown')} с грузом направлен на станцию зарядки")
        
    except Exception as e:
        logger.error(f"Ошибка направления дрона с грузом на зарядку: {e}")

def unload_cargo_at_station(drone_id: str):
    """Разгружает груз на станции"""
    try:
        drone = STATE["drones"].get(drone_id)
        if not drone:
            return False
        
        # Помечаем, что груз разгружен
        drone["has_cargo"] = False
        drone["cargo_type"] = None
        drone["cargo_weight"] = 0
        
        logger.info(f"Груз разгружен с дрона {drone_id}")
        return True
        
    except Exception as e:
        logger.error(f"Ошибка разгрузки груза: {e}")
        return False

def load_cargo_for_delivery(drone_id: str, order_id: str):
    """Загружает груз для доставки"""
    try:
        drone = STATE["drones"].get(drone_id)
        order = None
        
        # Находим заказ
        for o in STATE.get("orders", []):
            if o.get("id") == order_id:
                order = o
                break
        
        if not drone or not order:
            return False
        
        # Загружаем груз
        drone["has_cargo"] = True
        drone["cargo_type"] = order.get("type", "delivery")
        drone["cargo_weight"] = 1.0  # кг
        drone["cargo_order_id"] = order_id
        
        logger.info(f"Груз загружен на дрон {drone_id} для заказа {order_id}")
        return True
        
    except Exception as e:
        logger.error(f"Ошибка загрузки груза: {e}")
        return False

def can_reach_charging_station(drone: Dict[str, Any]) -> bool:
    """Проверяет, может ли дрон долететь до ближайшей станции зарядки с текущим зарядом"""
    try:
        pos = tuple(drone.get("pos", (0, 0)))
        battery = drone.get("battery", 0)
        drone_type = drone.get("type", "cargo")
        
        # Получаем параметры дрона
        per = {
            "cargo": 2000.0,
            "operator": 2500.0,
            "cleaner": 3000.0,
        }.get(drone_type, 2000.0)
        
        # Вычисляем максимальную дальность с текущим зарядом
        max_range = per * (battery / 100.0)
        
        # Ищем ближайшую станцию зарядки
        chargers = []
        if STATE.get("base"):
            chargers.append(tuple(STATE["base"]))
        chargers += [tuple(s) for s in STATE.get("stations", []) if isinstance(s, (list, tuple)) and len(s)==2]
        
        if not chargers:
            return False
        
        # Проверяем, можем ли долететь до любой станции
        for charger in chargers:
            distance = haversine_m(pos, charger)
            if distance <= max_range:
                return True
        
        return False
        
    except Exception as e:
        logger.error(f"Ошибка проверки доступности станции зарядки: {e}")
        return False

def handover_order_if_needed(broken_drone_id: str):
    """Передача заказа другому дрону при поломке или низком заряде"""
    try:
        # Находим заказ, назначенный сломанному дрону
        order_to_handover = None
        for order in STATE.get("orders", []):
            if order.get("drone_id") == broken_drone_id and order.get("status") == "assigned":
                order_to_handover = order
                break
        
        if not order_to_handover:
            return False
        
        # Ищем ближайший свободный дрон
        broken_drone = STATE["drones"].get(broken_drone_id)
        if not broken_drone:
            return False
        
        best_drone_id = find_best_replacement_drone(broken_drone, order_to_handover)
        
        if best_drone_id:
            # Передаем заказ новому дрону
            success = transfer_order_to_drone(order_to_handover, best_drone_id)
            if success:
                logger.info(f"Заказ {order_to_handover['id']} передан дрону {best_drone_id}")
                return True
        
        # Если не нашли подходящий дрон, отменяем заказ
        order_to_handover["status"] = "cancelled"
        order_to_handover["drone_id"] = None
        logger.warning(f"Заказ {order_to_handover['id']} отменен - нет доступных дронов")
        return False
        
    except Exception as e:
        logger.error(f"Ошибка передачи заказа: {e}")
        return False

def find_best_replacement_drone(broken_drone: Dict[str, Any], order: Dict[str, Any]) -> Optional[str]:
    """Находит лучшего дрона для замены"""
    try:
        best_drone_id = None
        best_score = float('inf')
        
        for drone_id, drone in STATE["drones"].items():
            if drone_id == order.get("drone_id"):
                continue
            
            # Проверяем базовые требования
            if not is_drone_available_for_order(drone, order):
                continue
            
            # Вычисляем оценку пригодности дрона
            score = calculate_drone_suitability_score(drone, broken_drone, order)
            
            if score < best_score:
                best_score = score
                best_drone_id = drone_id
        
        return best_drone_id
        
    except Exception as e:
        logger.error(f"Ошибка поиска замены дрона: {e}")
        return None

def is_drone_available_for_order(drone: Dict[str, Any], order: Dict[str, Any]) -> bool:
    """Проверяет, доступен ли дрон для заказа"""
    try:
        # Проверяем статус
        if drone.get("status") not in ("idle", "charging"):
            return False
        
        # Проверяем здоровье
        if drone.get("health", 100) < 50:
            return False
        
        # Проверяем заряд
        if drone.get("battery", 0) < 20:
            return False
        
        # Проверяем тип дрона (если заказ требует конкретный тип)
        required_type = map_order_to_drone_type(order.get("type", "work"))
        if drone.get("type") != required_type:
            return False
        
        return True
        
    except Exception as e:
        logger.error(f"Ошибка проверки доступности дрона: {e}")
        return False

def calculate_drone_suitability_score(drone: Dict[str, Any], broken_drone: Dict[str, Any], order: Dict[str, Any]) -> float:
    """Вычисляет оценку пригодности дрона для заказа"""
    try:
        score = 0.0
        
        # Расстояние до точки забора груза
        distance = haversine_m(drone["pos"], order["start"])
        score += distance * 0.1  # 0.1 балла за метр
        
        # Обратный заряд (чем больше, тем лучше)
        battery = drone.get("battery", 0)
        score += (100 - battery) * 0.01  # 0.01 балла за каждый % разряда
        
        # Обратное здоровье (чем больше, тем лучше)
        health = drone.get("health", 100)
        score += (100 - health) * 0.02  # 0.02 балла за каждый % износа
        
        # Штраф за дрона с грузом
        if drone.get("has_cargo", False):
            score += 1000.0
        
        return score
        
    except Exception as e:
        logger.error(f"Ошибка вычисления оценки дрона: {e}")
        return float('inf')

def transfer_order_to_drone(order: Dict[str, Any], new_drone_id: str) -> bool:
    """Передает заказ новому дрону"""
    try:
        new_drone = STATE["drones"].get(new_drone_id)
        if not new_drone:
            return False
        
        # Обновляем заказ
        order["drone_id"] = new_drone_id
        order["status"] = "assigned"
        
        # Планируем маршрут для нового дрона
        start = order["start"]
        end = order["end"]
        waypoints = order.get("waypoints", [])
        
        path, coords, length = plan_route_for(start, end, new_drone["type"], new_drone["battery"], waypoints)
        if coords:
            apply_midroute_charging(new_drone, coords)
            new_drone["status"] = "enroute"
            
            # Если это заказ доставки, загружаем груз
            if order.get("type") == "delivery":
                load_cargo_for_delivery(new_drone_id, order["id"])
            
            return True
        
        return False
        
    except Exception as e:
        logger.error(f"Ошибка передачи заказа дрону: {e}")
        return False

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
    """Создает дронов на базе согласно инвентарю. Удаляет старых дронов и создает новых."""
    base = STATE.get("base")
    inv = STATE.get("inventory") or {}
    if not base or not isinstance(inv, dict):
        return
    
    # Очищаем существующих дронов
    STATE["drones"] = {}
    
    # Создаем дронов согласно инвентарю
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
    
    logger.info(f"Создано {total} дронов на базе согласно инвентарю")

def spawn_drone(drone_type: str, pos: Tuple[float, float], battery: float = 100.0) -> str:
    did = f"drone_{len(STATE['drones'])+1}"
    STATE["drones"][did] = {
        "pos": pos,
        "type": drone_type,
        "battery": float(battery),
        "route": [],
        "target_idx": 0,
        "status": "idle",
        "health": 100.0,  # 0-100, 0 = поломка
        "maintenance_hours": 0.0,  # накопленные часы работы
        "last_maintenance": datetime.utcnow().isoformat(),
        "malfunction_probability": 0.01,  # вероятность поломки за час
        "temp_c": 35.0,
        "link_quality": 0.8,
        "remaining_m": 0.0,
        "eta_s": 0,
        "history": [],
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
            logger.info(f"Создан дрон {did} типа {t} из инвентаря")
            return did
    return None

def get_available_drone_types() -> Dict[str, int]:
    """Получает доступные типы дронов из инвентаря"""
    inv = STATE.get("inventory", {})
    available = {}
    for drone_type in ("cargo", "operator", "cleaner"):
        count = inv.get(drone_type, 0)
        try:
            count = int(count)
        except Exception:
            count = 0
        available[drone_type] = count
    return available

def get_drone_type_display_name(drone_type: str) -> str:
    """Получает отображаемое имя типа дрона"""
    names = {
        "cargo": "Грузовой",
        "operator": "Операторский", 
        "cleaner": "Уборочный"
    }
    return names.get(drone_type, drone_type)

def calculate_route_efficiency(start: Tuple[float, float], end: Tuple[float, float], drone_type: str) -> float:
    """Вычисляет эффективность маршрута (чем меньше, тем лучше)"""
    try:
        # Прямое расстояние
        direct_distance = haversine_m(start, end)
        
        # Планируем маршрут
        path, coords, length = plan_route_for(start, end, drone_type, 100.0)
        if not coords:
            return float('inf')
        
        # Коэффициент эффективности (отношение прямого расстояния к длине маршрута)
        efficiency = direct_distance / length if length > 0 else 1.0
        
        # Учитываем сложность маршрута (количество поворотов)
        complexity_factor = 1.0 + (len(coords) - 2) * 0.1
        
        return efficiency * complexity_factor
        
    except Exception as e:
        logger.error(f"Ошибка вычисления эффективности маршрута: {e}")
        return float('inf')

def optimize_order_queue(order_type: str) -> List[Dict[str, Any]]:
    """Оптимизирует очередь заказов определенного типа"""
    try:
        if not STATE.get("route_optimization", True):
            return STATE.get("order_queues", {}).get(order_type, [])
        
        orders = STATE.get("order_queues", {}).get(order_type, [])
        if len(orders) <= 1:
            return orders
        
        # Сортируем заказы по приоритету и времени создания
        orders.sort(key=lambda x: (x.get("priority", 1), x.get("created_at", "")), reverse=True)
        
        # Группируем заказы по близости
        optimized_orders = []
        used_orders = set()
        
        for i, order in enumerate(orders):
            if order["id"] in used_orders:
                continue
                
            optimized_orders.append(order)
            used_orders.add(order["id"])
            
            # Ищем близкие заказы для группировки
            start_pos = order["start"]
            for j, other_order in enumerate(orders[i+1:], i+1):
                if other_order["id"] in used_orders:
                    continue
                    
                other_start = other_order["start"]
                distance = haversine_m(start_pos, other_start)
                
                # Если заказы близко друг к другу, группируем их
                if distance < 1000:  # 1км
                    optimized_orders.append(other_order)
                    used_orders.add(other_order["id"])
        
        return optimized_orders
        
    except Exception as e:
        logger.error(f"Ошибка оптимизации очереди заказов: {e}")
        return STATE.get("order_queues", {}).get(order_type, [])

def add_order_to_queue(order: Dict[str, Any]):
    """Добавляет заказ в соответствующую очередь"""
    try:
        order_type = order.get("type", "work")
        if order_type not in STATE["order_queues"]:
            STATE["order_queues"][order_type] = []
        
        STATE["order_queues"][order_type].append(order)
        
        # Оптимизируем очередь
        STATE["order_queues"][order_type] = optimize_order_queue(order_type)
        
    except Exception as e:
        logger.error(f"Ошибка добавления заказа в очередь: {e}")

def get_next_order_from_queue(order_type: str) -> Optional[Dict[str, Any]]:
    """Получает следующий заказ из очереди"""
    try:
        queue = STATE.get("order_queues", {}).get(order_type, [])
        if not queue:
            return None
        
        # Берем первый заказ из оптимизированной очереди
        order = queue.pop(0)
        STATE["order_queues"][order_type] = queue
        
        return order
        
    except Exception as e:
        logger.error(f"Ошибка получения заказа из очереди: {e}")
        return None

def route_to_base_optimal(drone_id: str):
    """Строит оптимальный маршрут до базы для дрона"""
    try:
        drone = STATE["drones"].get(drone_id)
        if not drone:
            return
        
        base = STATE.get("base")
        if not base:
            return
        
        # Проверяем, есть ли новые заказы поблизости
        nearby_orders = find_nearby_orders(drone["pos"], drone["type"])
        if nearby_orders:
            # Если есть заказы поблизости, берем ближайший
            nearest_order = nearby_orders[0]
            assign_order_to_drone(nearest_order, drone_id)
            return
        
        # Если нет заказов, строим оптимальный маршрут до базы
        current_pos = drone["pos"]
        drone_type = drone["type"]
        battery = drone["battery"]
        
        # Планируем маршрут до базы
        path, coords, length = plan_route_for(current_pos, tuple(base), drone_type, battery)
        if coords:
            drone["route"] = coords
            drone["target_idx"] = 0
            drone["status"] = "returning_to_base"
            logger.info(f"Дрон {drone_id} возвращается на базу")
        
    except Exception as e:
        logger.error(f"Ошибка построения оптимального маршрута до базы: {e}")

def find_nearby_orders(position: Tuple[float, float], drone_type: str, max_distance: float = 5000.0) -> List[Dict[str, Any]]:
    """Находит заказы поблизости от дрона"""
    try:
        nearby_orders = []
        required_type = drone_type
        
        for order in STATE.get("orders", []):
            if order.get("status") != "queued":
                continue
            
            # Проверяем тип заказа
            order_type = map_order_to_drone_type(order.get("type", "work"))
            if order_type != required_type:
                continue
            
            # Проверяем расстояние
            distance = haversine_m(position, order["start"])
            if distance <= max_distance:
                nearby_orders.append({
                    "order": order,
                    "distance": distance
                })
        
        # Сортируем по расстоянию
        nearby_orders.sort(key=lambda x: x["distance"])
        return [item["order"] for item in nearby_orders]
        
    except Exception as e:
        logger.error(f"Ошибка поиска заказов поблизости: {e}")
        return []

def assign_order_to_drone(order: Dict[str, Any], drone_id: str) -> bool:
    """Назначает заказ дрону"""
    try:
        drone = STATE["drones"].get(drone_id)
        if not drone:
            return False
        
        # Обновляем заказ
        order["drone_id"] = drone_id
        order["status"] = "assigned"
        
        # Планируем маршрут
        start = order["start"]
        end = order["end"]
        waypoints = order.get("waypoints", [])
        
        path, coords, length = plan_route_for(start, end, drone["type"], drone["battery"], waypoints)
        if coords:
            apply_midroute_charging(drone, coords)
            drone["status"] = "enroute"
            
            # Если это заказ доставки, загружаем груз
            if order.get("type") == "delivery":
                load_cargo_for_delivery(drone_id, order["id"])
            
            logger.info(f"Заказ {order['id']} назначен дрону {drone_id}")
            return True
        
        return False
        
    except Exception as e:
        logger.error(f"Ошибка назначения заказа дрону: {e}")
        return False

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
