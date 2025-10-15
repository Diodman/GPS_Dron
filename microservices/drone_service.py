"""
Микросервис для управления дронами
"""
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Dict, List, Any, Optional
import asyncio
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

app = FastAPI(title="Drone Management Service", version="1.0.0")

# Состояние дронов
drones: Dict[str, Dict[str, Any]] = {}

class DroneUpdate(BaseModel):
    drone_id: str
    position: tuple
    battery: float
    status: str
    health: float
    route: Optional[List[tuple]] = None
    target_idx: int = 0

class DroneStatus(BaseModel):
    drone_id: str
    position: tuple
    battery: float
    status: str
    health: float
    route: Optional[List[tuple]]
    target_idx: int
    last_update: str

@app.post("/drones/{drone_id}/update")
async def update_drone(drone_id: str, update: DroneUpdate):
    """Обновление состояния дрона"""
    try:
        drones[drone_id] = {
            "position": update.position,
            "battery": update.battery,
            "status": update.status,
            "health": update.health,
            "route": update.route,
            "target_idx": update.target_idx,
            "last_update": datetime.utcnow().isoformat()
        }
        return {"ok": True, "drone_id": drone_id}
    except Exception as e:
        logger.error(f"Error updating drone {drone_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/drones/{drone_id}")
async def get_drone(drone_id: str):
    """Получение состояния дрона"""
    if drone_id not in drones:
        raise HTTPException(status_code=404, detail="Drone not found")
    
    drone = drones[drone_id]
    return DroneStatus(
        drone_id=drone_id,
        position=drone["position"],
        battery=drone["battery"],
        status=drone["status"],
        health=drone["health"],
        route=drone["route"],
        target_idx=drone["target_idx"],
        last_update=drone["last_update"]
    )

@app.get("/drones")
async def get_all_drones():
    """Получение всех дронов"""
    return {
        "drones": {
            drone_id: DroneStatus(
                drone_id=drone_id,
                position=drone["position"],
                battery=drone["battery"],
                status=drone["status"],
                health=drone["health"],
                route=drone["route"],
                target_idx=drone["target_idx"],
                last_update=drone["last_update"]
            ) for drone_id, drone in drones.items()
        }
    }

@app.post("/drones/{drone_id}/repair")
async def repair_drone(drone_id: str):
    """Ремонт дрона"""
    if drone_id not in drones:
        raise HTTPException(status_code=404, detail="Drone not found")
    
    drones[drone_id]["health"] = 100.0
    drones[drone_id]["status"] = "idle"
    drones[drone_id]["last_update"] = datetime.utcnow().isoformat()
    
    return {"ok": True, "message": f"Drone {drone_id} repaired"}

@app.get("/health")
async def health_check():
    """Проверка здоровья сервиса"""
    return {"status": "healthy", "service": "drone_management"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8002)

