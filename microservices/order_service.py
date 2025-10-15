"""
Микросервис для управления заказами
"""
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Dict, List, Any, Optional
import asyncio
import logging
from datetime import datetime
from enum import Enum

logger = logging.getLogger(__name__)

app = FastAPI(title="Order Management Service", version="1.0.0")

class OrderStatus(str, Enum):
    QUEUED = "queued"
    ASSIGNED = "assigned"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    CANCELLED = "cancelled"

class OrderType(str, Enum):
    DELIVERY = "delivery"
    SHOOTING = "shooting"
    WORK = "work"
    INSPECTION = "inspection"

class Order(BaseModel):
    id: str
    type: OrderType
    start: tuple
    end: tuple
    status: OrderStatus
    drone_id: Optional[str] = None
    priority: int = 5
    created_at: str
    waypoints: Optional[List[tuple]] = None
    battery_level: float = 100.0

class CreateOrderRequest(BaseModel):
    type: OrderType
    start: tuple
    end: tuple
    priority: int = 5
    waypoints: Optional[List[tuple]] = None
    battery_level: float = 100.0
    drone_id: Optional[str] = None

# Хранилище заказов
orders: Dict[str, Order] = {}
order_queues: Dict[str, List[str]] = {
    "delivery": [],
    "shooting": [],
    "work": [],
    "inspection": []
}

@app.post("/orders", response_model=Order)
async def create_order(request: CreateOrderRequest):
    """Создание нового заказа"""
    try:
        order_id = f"ord_{len(orders) + 1}"
        order = Order(
            id=order_id,
            type=request.type,
            start=request.start,
            end=request.end,
            status=OrderStatus.QUEUED,
            drone_id=request.drone_id,
            priority=request.priority,
            created_at=datetime.utcnow().isoformat(),
            waypoints=request.waypoints,
            battery_level=request.battery_level
        )
        
        orders[order_id] = order
        order_queues[request.type.value].append(order_id)
        
        # Сортируем очередь по приоритету
        order_queues[request.type.value].sort(
            key=lambda oid: orders[oid].priority, 
            reverse=True
        )
        
        logger.info(f"Created order {order_id} of type {request.type}")
        return order
        
    except Exception as e:
        logger.error(f"Error creating order: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/orders/{order_id}")
async def get_order(order_id: str):
    """Получение заказа"""
    if order_id not in orders:
        raise HTTPException(status_code=404, detail="Order not found")
    return orders[order_id]

@app.get("/orders")
async def get_all_orders():
    """Получение всех заказов"""
    return {"orders": list(orders.values())}

@app.get("/orders/queue/{order_type}")
async def get_queue_orders(order_type: str):
    """Получение заказов в очереди по типу"""
    if order_type not in order_queues:
        raise HTTPException(status_code=400, detail="Invalid order type")
    
    queued_orders = [orders[oid] for oid in order_queues[order_type]]
    return {"orders": queued_orders}

@app.post("/orders/{order_id}/assign")
async def assign_order(order_id: str, drone_id: str):
    """Назначение заказа дрону"""
    if order_id not in orders:
        raise HTTPException(status_code=404, detail="Order not found")
    
    order = orders[order_id]
    if order.status != OrderStatus.QUEUED:
        raise HTTPException(status_code=400, detail="Order not in queue")
    
    order.status = OrderStatus.ASSIGNED
    order.drone_id = drone_id
    
    # Удаляем из очереди
    if order_id in order_queues[order.type.value]:
        order_queues[order.type.value].remove(order_id)
    
    logger.info(f"Assigned order {order_id} to drone {drone_id}")
    return {"ok": True, "order": order}

@app.post("/orders/{order_id}/complete")
async def complete_order(order_id: str):
    """Завершение заказа"""
    if order_id not in orders:
        raise HTTPException(status_code=404, detail="Order not found")
    
    order = orders[order_id]
    order.status = OrderStatus.COMPLETED
    order.drone_id = None
    
    logger.info(f"Completed order {order_id}")
    return {"ok": True, "order": order}

@app.post("/orders/{order_id}/cancel")
async def cancel_order(order_id: str):
    """Отмена заказа"""
    if order_id not in orders:
        raise HTTPException(status_code=404, detail="Order not found")
    
    order = orders[order_id]
    if order.status in [OrderStatus.COMPLETED, OrderStatus.CANCELLED]:
        raise HTTPException(status_code=400, detail="Order already finished")
    
    order.status = OrderStatus.CANCELLED
    order.drone_id = None
    
    # Удаляем из очереди если там есть
    if order_id in order_queues[order.type.value]:
        order_queues[order.type.value].remove(order_id)
    
    logger.info(f"Cancelled order {order_id}")
    return {"ok": True, "order": order}

@app.get("/orders/next/{order_type}")
async def get_next_order(order_type: str):
    """Получение следующего заказа из очереди"""
    if order_type not in order_queues:
        raise HTTPException(status_code=400, detail="Invalid order type")
    
    if not order_queues[order_type]:
        return None
    
    order_id = order_queues[order_type].pop(0)
    return orders[order_id]

@app.get("/health")
async def health_check():
    """Проверка здоровья сервиса"""
    return {"status": "healthy", "service": "order_management"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8003)

