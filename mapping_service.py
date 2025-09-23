import folium
import webbrowser
import os
import tempfile
import threading
import logging

class MappingService:
    def __init__(self):
        self.temp_dir = tempfile.mkdtemp(prefix="drone_maps_")
        self.logger = logging.getLogger(__name__)
    
    def create_selection_map(self, city_name, center_point=None, on_click_callback=None):
        """Создание карты для выбора точек с поддержкой российских городов"""
        try:
            # Определяем центр карты для российских городов
            if center_point is None:
                center_point = self._get_city_center(city_name)
            
            self.logger.info(f"Создание карты выбора для {city_name}, центр: {center_point}")
            
            # Создаем карту с подходящим масштабом для города
            zoom_level = self._get_optimal_zoom(city_name)
            m = folium.Map(location=center_point, zoom_start=zoom_level, tiles='OpenStreetMap')
            
            # Добавляем возможность клика для получения координат
            m.add_child(folium.LatLngPopup())
            
            # Добавляем инструкции на русском языке
            instructions = f"""
            <div style="position:fixed;top:10px;right:10px;z-index:1000;background:white;padding:15px;border-radius:8px;max-width:300px;box-shadow:0 2px 10px rgba(0,0,0,0.2);">
                <h4 style='color:#2E7D32;margin-top:0;'>🗺️ Выбор точек маршрута</h4>
                <p><strong>Город:</strong> {city_name}</p>
                <p>1. Кликните на карте для выбора точки</p>
                <p>2. Координаты появятся во всплывающем окне</p>
                <p>3. Скопируйте координаты в программу</p>
                <p style="color:#666;font-size:12px;margin-bottom:0;">Формат: широта, долгота</p>
            </div>
            """
            m.get_root().html.add_child(folium.Element(instructions))
            
            # Добавляем маркер центра города
            folium.Marker(
                center_point,
                popup=f"Центр города {city_name}",
                icon=folium.Icon(color='blue', icon='home')
            ).add_to(m)
            
            filename = os.path.join(self.temp_dir, f"selection_map_{self._sanitize_name(city_name)}.html")
            m.save(filename)
            
            self.logger.info(f"Карта выбора сохранена: {filename}")
            return filename
            
        except Exception as e:
            self.logger.error(f"Ошибка создания карты выбора: {e}")
            return None
    
    def create_route_map(self, city_name, routes, drone_types):
        """Создание карты с маршрутами дронов"""
        try:
            if not routes:
                self.logger.warning("Нет маршрутов для отображения")
                return None
            
            # Определяем центр карты
            center = routes[0][2][0] if routes[0][2] else self._get_city_center(city_name)
            
            self.logger.info(f"Создание карты маршрутов для {city_name}, {len(routes)} маршрутов")
            
            # Создаем карту
            zoom_level = self._get_optimal_zoom(city_name)
            m = folium.Map(location=center, zoom_start=zoom_level, tiles='OpenStreetMap')
            
            # Цвета для разных маршрутов
            colors = ['#4285F4', '#EA4335', '#FBBC05', '#34A853', '#9C27B0', '#FF9800', '#795548', '#607D8B']
            
            # Добавляем маршруты
            for i, (path, length, coords) in enumerate(routes):
                try:
                    if not coords or len(coords) < 2:
                        self.logger.warning(f"Пропуск маршрута {i+1}: недостаточно координат")
                        # Если маршрут состоит из одной точки, создаем небольшой маркер
                        if len(coords) == 1:
                            folium.Marker(
                                coords[0], 
                                popup=f"🚁 Дрон {i+1} ({drone_types[i] if i < len(drone_types) else 'cargo'})<br>Точка: {coords[0][0]:.6f}, {coords[0][1]:.6f}",
                                icon=folium.Icon(color='blue', icon='info-sign', prefix='fa')
                            ).add_to(m)
                        continue
                    
                    color = colors[i % len(colors)]
                    drone_type = drone_types[i] if i < len(drone_types) else "cargo"
                    
                    # Создаем линию маршрута
                    folium.PolyLine(
                        coords, 
                        color=color, 
                        weight=6,
                        opacity=0.8,
                        tooltip=f"Дрон {i+1} ({drone_type}): {length:.1f}м"
                    ).add_to(m)
                    
                    # Добавляем маркеры старта и финиша
                    folium.Marker(
                        coords[0], 
                        popup=f"🚁 Старт дрона {i+1}<br>Тип: {drone_type}<br>Координаты: {coords[0][0]:.6f}, {coords[0][1]:.6f}",
                        icon=folium.Icon(color='green', icon='play', prefix='fa')
                    ).add_to(m)
                    
                    folium.Marker(
                        coords[-1], 
                        popup=f"🏁 Финиш дрона {i+1}<br>Длина: {length:.1f}м<br>Координаты: {coords[-1][0]:.6f}, {coords[-1][1]:.6f}",
                        icon=folium.Icon(color='red', icon='stop', prefix='fa')
                    ).add_to(m)
                    
                except Exception as e:
                    self.logger.error(f"Ошибка добавления маршрута {i+1}: {e}")
                    continue
            
            # Добавляем информацию о маршрутах
            route_info = f"""
            <div style="position:fixed;top:10px;left:10px;z-index:1000;background:white;padding:15px;border-radius:8px;max-width:300px;box-shadow:0 2px 10px rgba(0,0,0,0.2);">
                <h4 style='color:#1976D2;margin-top:0;'>🚁 Маршруты дронов</h4>
                <p><strong>Город:</strong> {city_name}</p>
                <p><strong>Количество маршрутов:</strong> {len(routes)}</p>
                <p><strong>Общая длина:</strong> {sum(r[1] for r in routes):.1f}м</p>
                <div style="margin-top:10px;">
                    <p style="margin:5px 0;font-size:12px;"><span style="color:#4285F4;">●</span> Синий - Дрон 1</p>
                    <p style="margin:5px 0;font-size:12px;"><span style="color:#EA4335;">●</span> Красный - Дрон 2</p>
                    <p style="margin:5px 0;font-size:12px;"><span style="color:#FBBC05;">●</span> Жёлтый - Дрон 3</p>
                    <p style="margin:5px 0;font-size:12px;"><span style="color:#34A853;">●</span> Зелёный - Дрон 4</p>
                </div>
            </div>
            """
            m.get_root().html.add_child(folium.Element(route_info))
            
            filename = os.path.join(self.temp_dir, f"route_map_{self._sanitize_name(city_name)}.html")
            m.save(filename)
            
            self.logger.info(f"Карта маршрутов сохранена: {filename}")
            return filename
            
        except Exception as e:
            self.logger.error(f"Ошибка создания карты маршрутов: {e}")
            return None
    
    def open_map(self, filename):
        """Открытие карты в браузере"""
        if not filename or not os.path.exists(filename):
            self.logger.warning(f"Файл карты не найден: {filename}")
            return False
        
        def open_browser():
            try:
                abs_path = os.path.abspath(filename)
                webbrowser.open(f"file://{abs_path}")
                self.logger.info(f"Карта открыта в браузере: {abs_path}")
            except Exception as e:
                self.logger.error(f"Ошибка открытия карты: {e}")
        
        threading.Thread(target=open_browser, daemon=True).start()
        return True
    
    def _get_city_center(self, city_name):
        """Получение центральных координат для российских городов"""
        city_centers = {
            'волгоград': (48.7080, 44.5133),
            'volgograd': (48.7080, 44.5133),
            'москва': (55.7558, 37.6176),
            'moscow': (55.7558, 37.6176),
            'санкт-петербург': (59.9311, 30.3609),
            'st petersburg': (59.9311, 30.3609),
            'st. petersburg': (59.9311, 30.3609),
            'petersburg': (59.9311, 30.3609),
            'екатеринбург': (56.8431, 60.6454),
            'yekaterinburg': (56.8431, 60.6454),
            'новосибирск': (55.0084, 82.9357),
            'novosibirsk': (55.0084, 82.9357),
            'казань': (55.8304, 49.0661),
            'kazan': (55.8304, 49.0661),
            'нижний новгород': (56.3269, 44.0075),
            'nizhny novgorod': (56.3269, 44.0075),
            'челябинск': (55.1644, 61.4368),
            'chelyabinsk': (55.1644, 61.4368),
            'самара': (53.2001, 50.1500),
            'samara': (53.2001, 50.1500),
            'омск': (54.9885, 73.3242),
            'omsk': (54.9885, 73.3242),
            'ростов-на-дону': (47.2357, 39.7015),
            'rostov-on-don': (47.2357, 39.7015),
            'уфа': (54.7388, 55.9721),
            'ufa': (54.7388, 55.9721)
        }
        
        normalized_name = city_name.lower().strip()
        for key, center in city_centers.items():
            if key in normalized_name:
                return center
        
        # Возвращаем центр Волгограда по умолчанию
        return (48.7080, 44.5133)
    
    def _get_optimal_zoom(self, city_name):
        """Определение оптимального уровня масштабирования для города"""
        city_zooms = {
            'волгоград': 11,
            'volgograd': 11,
            'москва': 10,
            'moscow': 10,
            'санкт-петербург': 10,
            'st petersburg': 10,
            'st. petersburg': 10,
            'petersburg': 10,
            'екатеринбург': 11,
            'yekaterinburg': 11,
            'новосибирск': 10,
            'novosibirsk': 10,
            'казань': 11,
            'kazan': 11,
            'нижний новгород': 11,
            'nizhny novgorod': 11,
            'челябинск': 11,
            'chelyabinsk': 11,
            'самара': 11,
            'samara': 11,
            'омск': 11,
            'omsk': 11,
            'ростов-на-дону': 11,
            'rostov-on-don': 11,
            'уфа': 11,
            'ufa': 11
        }
        
        normalized_name = city_name.lower().strip()
        for key, zoom in city_zooms.items():
            if key in normalized_name:
                return zoom
        
        # Возвращаем стандартный масштаб для городов
        return 12
    
    def _sanitize_name(self, name):
        """Очистка имени файла от недопустимых символов"""
        import re
        name = re.sub(r'[^\w\s-]', '', name)
        return re.sub(r'[-\s]+', '_', name).strip('_')[:50]