# 🚁 Планировщик маршрутов для дронов / Drone Route Planner

[![Python](https://img.shields.io/badge/Python-3.9%2B-blue)](https://python.org)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.104%2B-green)](https://fastapi.tiangolo.com)
[![Docker](https://img.shields.io/badge/Docker-Ready-blue)](https://docker.com)
[![License](https://img.shields.io/badge/License-MIT-green)](LICENSE)

---

## 🇷🇺 Русский

Продвинутая система планирования маршрутов для беспилотных летательных аппаратов с поддержкой различных типов дронов, обходом бесполётных зон и учетом ограничений батареи. Backend на FastAPI, клиент на Leaflet.

### ✨ Особенности

- **🤖 Поддержка 3 типов дронов**: Грузовой, Операторский, Мойщик окон
- **🎯 Умное планирование**: Автоматический расчет оптимальных маршрутов
- **⚠️ Избегание препятствий**: Система обнаружения и обхода зданий и запретных зон
- **🔋 Учет батареи**: Планирование с учетом уровня заряда и дальности полета
- **👥 Мультидроность**: Одновременное планирование для нескольких дронов
- **🗺️ Визуализация**: Интерактивные карты с отображением маршрутов
- **🏢 Геокодирование**: Поддержка адресов и координат
- **📊 Живая симуляция**: Реальное время движения дронов и статусов

### 🚀 Быстрый старт

#### Вариант 1: Docker (Рекомендуется)

```bash
# Сборка и запуск с Docker
docker build -t drone-planner .
docker run --rm -p 8000:8000 drone-planner

# Откройте http://localhost:8000
```

#### Вариант 2: Локальная разработка

```bash
# Настройка окружения
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt

# Запуск сервера
uvicorn api_server:app --reload

# Откройте http://localhost:8000
```

### 🐳 Развертывание с Docker

#### Простое развертывание
```bash
docker build -t drone-planner .
docker run --rm -p 8000:8000 \
  -e REDIS_URL=redis://localhost:6379/0 \
  drone-planner
```

#### С Redis (Продакшен)
```bash
docker compose up --build
```

### 📋 Руководство по использованию

#### 1. Загрузка данных города
- Введите название города (например, "Волгоград, Россия")
- Нажмите "Загрузить" для загрузки данных OSM
- Дождитесь построения графа

#### 2. Настройка базы и станций
- Установите координаты базы (широта, долгота)
- Добавьте станции зарядки Alt+клик на карте
- Настройте инвентарь дронов

#### 3. Создание заказов
- Введите начальную/конечную точки (адрес или координаты)
- Добавьте промежуточные точки при необходимости
- Установите уровень батареи и тип дрона
- Отправьте заказ

#### 4. Мониторинг дронов
- Следите за движением дронов в реальном времени
- Проверяйте уровень батареи и ETA
- Просматривайте статус зарядки на станциях

### 🎮 Типы дронов

| Тип | Дальность | Высота | Назначение |
|-----|-----------|--------|------------|
| **Грузовой** | 20 км | 200м | Доставка грузов |
| **Операторский** | 15 км | 150м | Аэросъемка |
| **Мойщик** | 10 км | 100м | Мойка окон |

### 🔧 Технические детали

#### Основные сервисы
- **`api_server.py`**: FastAPI сервер с поддержкой WebSocket
- **`data_service.py`**: Загрузка данных OSM и геокодинг
- **`graph_service.py`**: Построение графа города с бесполётными зонами
- **`routing_service.py`**: Алгоритмы поиска пути и оптимизация

#### Ключевые возможности
- **Энергоэффективная маршрутизация**: Автоматическая интеграция станций зарядки
- **Симуляция в реальном времени**: Цикл симуляции с интервалом 1 секунда
- **WebSocket обновления**: Прямая трансляция позиций дронов
- **Геокодинг**: Поддержка российских адресов через Nominatim
- **Кэширование**: Сохранение состояния через Redis

### 🌍 Поддерживаемые города

Оптимизировано для российских городов с полной поддержкой геокодинга адресов:
- Волгоград (основной)
- Москва
- Санкт-Петербург
- И многие другие...

### 🛠️ Разработка

#### Требования
- Python 3.9+
- Docker (опционально)
- Интернет-соединение для данных OSM

#### Настройка
```bash
git clone <repository>
cd drone-route-planner
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn api_server:app --reload
```

#### Переменные окружения
- `REDIS_URL`: Строка подключения к Redis (опционально)
- `PORT`: Порт сервера (по умолчанию: 8000)

---

## 🇺🇸 English

A sophisticated drone route planning system with support for multiple drone types, no-fly zone avoidance, battery management, and real-time simulation. Built with FastAPI backend and interactive Leaflet frontend.

### ✨ Features

- **🤖 Multi-Drone Support**: Cargo, Operator, and Window Cleaner drone types
- **🎯 Smart Routing**: Energy-aware path planning with charging station integration
- **⚠️ Obstacle Avoidance**: Automatic no-fly zone detection and route optimization
- **🔋 Battery Management**: Real-time battery simulation with charging stops
- **🗺️ Interactive Maps**: Real-time visualization with Leaflet.js
- **🌍 Geocoding**: Address-to-coordinates conversion with Russian city support
- **📊 Live Simulation**: Real-time drone movement and status updates
- **🔌 WebSocket**: Live updates for drone positions and status

### 🚀 Quick Start

#### Option 1: Docker (Recommended)

```bash
# Build and run with Docker
docker build -t drone-planner .
docker run --rm -p 8000:8000 drone-planner

# Open http://localhost:8000
```

#### Option 2: Local Development

```bash
# Setup environment
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
pip install -r requirements.txt

# Run server
uvicorn api_server:app --reload

# Open http://localhost:8000
```

### 🐳 Docker Deployment

#### Simple Deployment
```bash
docker build -t drone-planner .
docker run --rm -p 8000:8000 \
  -e REDIS_URL=redis://localhost:6379/0 \
  drone-planner
```

#### With Redis (Production)
```bash
docker compose up --build
```

### 📋 Usage Guide

#### 1. Load City Data
- Enter city name (e.g., "Volgograd, Russia")
- Click "Загрузить" to load OSM data
- Wait for graph construction

#### 2. Configure Base & Stations
- Set base coordinates (lat, lon)
- Add charging stations by Alt+clicking on map
- Configure drone inventory

#### 3. Create Orders
- Enter start/end points (address or coordinates)
- Add waypoints if needed
- Set battery level and drone type
- Submit order

#### 4. Monitor Drones
- Watch real-time drone movement
- Check battery levels and ETA
- View charging status at stations

### 🎮 Drone Types

| Type | Range | Altitude | Purpose |
|------|-------|----------|---------|
| **Cargo** | 20 km | 200m | Package delivery |
| **Operator** | 15 km | 150m | Aerial photography |
| **Cleaner** | 10 km | 100m | Window cleaning |

### 🔧 Technical Details

#### Core Services
- **`api_server.py`**: FastAPI server with WebSocket support
- **`data_service.py`**: OSM data loading and geocoding
- **`graph_service.py`**: City graph construction with no-fly zones
- **`routing_service.py`**: Pathfinding algorithms and optimization

#### Key Features
- **Energy-Aware Routing**: Automatic charging station integration
- **Real-time Simulation**: 1-second tick simulation loop
- **WebSocket Updates**: Live drone position broadcasting
- **Geocoding**: Russian address support via Nominatim
- **Caching**: Redis-based state persistence

### 🌍 Supported Cities

Optimized for Russian cities with full address geocoding support:
- Volgograd (primary)
- Moscow
- St. Petersburg
- And many more...

### 🛠️ Development

#### Prerequisites
- Python 3.9+
- Docker (optional)
- Internet connection for OSM data

#### Setup
```bash
git clone <repository>
cd drone-route-planner
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn api_server:app --reload
```

#### Environment Variables
- `REDIS_URL`: Redis connection string (optional)
- `PORT`: Server port (default: 8000)

### 📊 Performance

- **Graph Construction**: ~30s for Volgograd
- **Route Planning**: <1s per route
- **Simulation**: 1Hz tick rate
- **Memory Usage**: ~200MB with city data

### 🔮 Roadmap

- [ ] 3D route visualization
- [ ] Machine learning optimization
- [ ] Mobile app integration
- [ ] Real drone API support
- [ ] Multi-language support
- [ ] Advanced analytics

### 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

### 🌐 Useful links

- [OSMnx](https://osmnx.readthedocs.io/) for OpenStreetMap integration
- [FastAPI](https://fastapi.tiangolo.com/) for the web framework
- [Leaflet](https://leafletjs.com/) for interactive maps
- [OpenStreetMap](https://openstreetmap.org/) for map data
