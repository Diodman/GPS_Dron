import folium
import webbrowser
import os
import tempfile
import threading

class MappingService:
    def __init__(self):
        self.temp_dir = tempfile.mkdtemp(prefix="drone_maps_")
    
    def create_selection_map(self, city_name, center_point=None, on_click_callback=None):
        if center_point is None:
            center_point = (52.5200, 13.4050)
        
        m = folium.Map(location=center_point, zoom_start=13, tiles='OpenStreetMap')
        
        if on_click_callback:
            m.add_child(folium.LatLngPopup())
        
        instructions = """
        <div style="position:fixed;top:10px;right:10px;z-index:1000;background:white;padding:15px;border-radius:8px;max-width:300px;">
            <h4 style='color:#2E7D32'>🗺️ Выбор точек</h4>
            <p>1. Кликните для выбора точки</p>
            <p>2. Координаты появятся во всплывающем окне</p>
            <p>3. Скопируйте координаты в программу</p>
        </div>
        """
        m.get_root().html.add_child(folium.Element(instructions))
        
        filename = os.path.join(self.temp_dir, f"selection_map.html")
        m.save(filename)
        return filename
    
    def create_route_map(self, city_name, routes, drone_types):
        if not routes:
            return None
        
        center = routes[0][2][0] if routes[0][2] else (52.5200, 13.4050)
        m = folium.Map(location=center, zoom_start=13, tiles='OpenStreetMap')
        
        colors = ['#4285F4', '#EA4335', '#FBBC05', '#34A853']
        
        for i, (path, length, coords) in enumerate(routes):
            color = colors[i % len(colors)]
            drone_type = drone_types[i] if i < len(drone_types) else "cargo"
            
            folium.PolyLine(
                coords, 
                color=color, 
                weight=5,
                opacity=0.7,
                tooltip=f"Дрон {i+1} ({drone_type}): {length:.1f}м"
            ).add_to(m)
            
            folium.Marker(
                coords[0], 
                popup=f"Старт {i+1}",
                icon=folium.Icon(color='green', icon='play')
            ).add_to(m)
            
            folium.Marker(
                coords[-1], 
                popup=f"Финиш {i+1}",
                icon=folium.Icon(color='red', icon='stop')
            ).add_to(m)
        
        filename = os.path.join(self.temp_dir, f"route_map.html")
        m.save(filename)
        return filename
    
    def open_map(self, filename):
        if not filename or not os.path.exists(filename):
            return False
        
        def open_browser():
            try:
                abs_path = os.path.abspath(filename)
                webbrowser.open(f"file://{abs_path}")
            except:
                pass
        
        threading.Thread(target=open_browser, daemon=True).start()
        return True