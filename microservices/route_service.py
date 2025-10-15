"""
Микросервис для планирования маршрутов
"""
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Tuple, Optional, Dict, Any
import asyncio
import logging
from route_cache import RouteCache
from fast_routing_service import FastRoutingService

logger = logging.getLogger(__name__)

app = FastAPI(title="Route Planning Service", version="1.0.0")

# Глобальные сервисы
route_cache = RouteCache(max_size=50000)
routing_service = FastRoutingService()

class RouteRequest(BaseModel):
    start: Tuple[float, float]
    end: Tuple[float, float]
    drone_type: str = "cargo"
    battery_level: float = 100.0
    waypoints: Optional[List[Tuple[float, float]]] = None
    city_name: str
    graph_data: Dict[str, Any]  # Сериализованный граф

class RouteResponse(BaseModel):
    path: List[int]
    coords: List[Tuple[float, float]]
    length: float
    algorithm_used: str
    cache_hit: bool

@app.post("/plan_route", response_model=RouteResponse)
async def plan_route(request: RouteRequest):
    """Планирование маршрута"""
    try:
        # Проверяем кэш
        cached_result = route_cache.get(
            request.start, request.end, 
            request.drone_type, request.battery_level, 
            request.waypoints
        )
        
        if cached_result:
            path, length, coords = cached_result
            return RouteResponse(
                path=path,
                coords=coords,
                length=length,
                algorithm_used="cached",
                cache_hit=True
            )
        
        # Восстанавливаем граф из данных
        import networkx as nx
        G = nx.node_link_graph(request.graph_data)
        
        # Планируем маршрут
        path, coords, length = routing_service.plan_route_fast(
            G, request.start, request.end, 
            max_range=20000 * (request.battery_level / 100.0),
            city_name=request.city_name,
            waypoints=request.waypoints
        )
        
        if path:
            # Сохраняем в кэш
            route_cache.put(
                request.start, request.end,
                request.drone_type, request.battery_level,
                request.waypoints, path, length, coords
            )
            
            return RouteResponse(
                path=path,
                coords=coords,
                length=length,
                algorithm_used="fast_routing",
                cache_hit=False
            )
        else:
            raise HTTPException(status_code=404, detail="Route not found")
            
    except Exception as e:
        logger.error(f"Route planning error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/cache/stats")
async def get_cache_stats():
    """Статистика кэша"""
    return route_cache.get_stats()

@app.post("/cache/clear")
async def clear_cache():
    """Очистка кэша"""
    route_cache.clear()
    return {"message": "Cache cleared"}

@app.get("/health")
async def health_check():
    """Проверка здоровья сервиса"""
    return {"status": "healthy", "service": "route_planning"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)

