import folium
import webbrowser
import os
import tempfile
import threading

class MappingService:
    def __init__(self):
        self.temp_dir = tempfile.mkdtemp(prefix="drone_maps_")
        print(f"Карты сохраняются в: {self.temp_dir}")
    
    def create_clickable_map(self, city_name, center_point=None):
        """Создание интерактивной карты для выбора точек"""
        if center_point is None:
            center_point = (52.5200, 13.4050)  # Берлин по умолчанию
        
        try:
            m = folium.Map(location=center_point, zoom_start=13, tiles='OpenStreetMap')
            
            # Добавляем функционал для выбора точек
            folium.LatLngPopup().add_to(m)
            
            # Стилизованные инструкции
            instructions = """
            <div style="
                position: fixed; 
                top: 10px; 
                right: 10px; 
                z-index: 1000; 
                background: rgba(255, 255, 255, 0.95);
                padding: 15px; 
                border-radius: 8px;
                box-shadow: 0 2px 10px rgba(0,0,0,0.3);
                border: 2px solid #4CAF50;
                font-family: Arial, sans-serif;
                max-width: 300px;
            ">
                <h4 style="margin: 0 0 10px 0; color: #2E7D32;">📌 Инструкция:</h4>
                <p style="margin: 5px 0; font-size: 12px;">1. Кликните по карте для выбора точки</p>
                <p style="margin: 5px 0; font-size: 12px;">2. Координаты появятся во всплывающем окне</p>
                <p style="margin: 5px 0; font-size: 12px;">3. Скопируйте координаты в программу</p>
                <p style="margin: 5px 0; font-size: 11px; color: #666;">Формат: широта, долгота (52.123456, 13.123456)</p>
            </div>
            """
            
            m.get_root().html.add_child(folium.Element(instructions))
            
            # Создаем безопасное имя файла
            safe_name = self._sanitize_name(city_name)
            filename = os.path.join(self.temp_dir, f"click_map_{safe_name}.html")
            
            m.save(filename)
            print(f"Карта сохранена: {filename}")
            
            return filename
            
        except Exception as e:
            print(f"Ошибка создания карты: {e}")
            return None
    
    def create_route_map(self, city_name, route_coords):
        """Создание карты с маршрутом"""
        if not route_coords or len(route_coords) < 2:
            print("Недостаточно точек для построения маршрута")
            return None
        
        try:
            center = route_coords[len(route_coords)//2]
            m = folium.Map(location=center, zoom_start=14, tiles='OpenStreetMap')
            
            # Добавляем маршрут
            folium.PolyLine(
                route_coords, 
                color="#4285F4", 
                weight=6, 
                opacity=0.8,
                tooltip="Маршрут дрона"
            ).add_to(m)
            
            # Добавляем точки начала и конца
            folium.Marker(
                route_coords[0], 
                popup="🟢 Старт",
                icon=folium.Icon(color="green", icon="play", prefix='fa')
            ).add_to(m)
            
            folium.Marker(
                route_coords[-1], 
                popup="🔴 Финиш", 
                icon=folium.Icon(color="red", icon="stop", prefix='fa')
            ).add_to(m)
            
            # Информация о маршруте
            info_html = f"""
            <div style="font-family: Arial; padding: 10px;">
                <h4 style="color: #4285F4; margin-bottom: 10px;">Маршрут дрона</h4>
                <p><strong>Город:</strong> {city_name}</p>
                <p><strong>Точек маршрута:</strong> {len(route_coords)}</p>
                <p><strong>Старт:</strong> {route_coords[0][0]:.6f}, {route_coords[0][1]:.6f}</p>
                <p><strong>Финиш:</strong> {route_coords[-1][0]:.6f}, {route_coords[-1][1]:.6f}</p>
            </div>
            """
            
            m.get_root().html.add_child(folium.Element(info_html))
            
            safe_name = self._sanitize_name(city_name)
            filename = os.path.join(self.temp_dir, f"route_map_{safe_name}.html")
            
            m.save(filename)
            print(f"Карта маршрута сохранена: {filename}")
            
            return filename
            
        except Exception as e:
            print(f"Ошибка создания карты маршрута: {e}")
            return None
    
    def open_map(self, filename):
        """Открытие карты в браузере"""
        if not filename or not os.path.exists(filename):
            print(f"Файл не существует: {filename}")
            return False
        
        try:
            # Открываем в отдельном потоке чтобы не блокировать GUI
            def open_browser():
                try:
                    abs_path = os.path.abspath(filename)
                    webbrowser.open(f"file://{abs_path}")
                    print(f"Карта открыта: {abs_path}")
                except Exception as e:
                    print(f"Ошибка открытия карты: {e}")
            
            threading.Thread(target=open_browser, daemon=True).start()
            return True
            
        except Exception as e:
            print(f"Ошибка открытия карты: {e}")
            return False
    
    def _sanitize_name(self, name):
        """Очистка имени города"""
        import re
        name = re.sub(r'[^\w\s-]', '', name)
        name = re.sub(r'[-\s]+', '_', name)
        return name.strip('_')[:50]