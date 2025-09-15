import os
import osmnx as ox
import geopandas as gpd
import time
import pickle
import requests
from geopy.geocoders import Nominatim

class DataService:
    def __init__(self, cache_dir="cache_data"):
        self.cache_dir = cache_dir
        os.makedirs(cache_dir, exist_ok=True)
        self.progress_callbacks = []
        self.geolocator = Nominatim(user_agent="drone_route_planner")
    
    def add_progress_callback(self, callback):
        self.progress_callbacks.append(callback)
    
    def _update_progress(self, stage, percentage, message=""):
        for callback in self.progress_callbacks:
            callback(stage, percentage, message)
    
    def get_city_data(self, city_name: str):
        cache_file = os.path.join(self.cache_dir, f"{self._sanitize_name(city_name)}.pkl")
        
        if os.path.exists(cache_file):
            self._update_progress("cache", 100, "Загрузка из кэша")
            return self._load_from_cache(cache_file)
        
        return self._download_city_data(city_name, cache_file)
    
    def _download_city_data(self, city_name, cache_file):
        self._update_progress("download", 0, "Начало загрузки данных")
        
        try:
            self._update_progress("download", 30, "Загрузка дорожной сети")
            road_graph = ox.graph_from_place(city_name, network_type='all', simplify=True)
            
            self._update_progress("download", 60, "Загрузка зданий")
            buildings = ox.features_from_place(city_name, tags={"building": True})
            
            self._update_progress("download", 80, "Загрузка запретных зон")
            no_fly_zones = self._get_no_fly_zones(city_name)
            
            data = {
                'road_graph': road_graph,
                'buildings': buildings,
                'no_fly_zones': no_fly_zones,
                'city_name': city_name,
                'timestamp': time.time()
            }
            
            self._update_progress("download", 90, "Сохранение в кэш")
            with open(cache_file, 'wb') as f:
                pickle.dump(data, f)
            
            self._update_progress("download", 100, "Данные загружены")
            return data
            
        except Exception as e:
            self._update_progress("error", 0, f"Ошибка: {str(e)}")
            raise
    
    def _get_no_fly_zones(self, city_name):
        return []  # Заглушка - можно добавить реальные данные
    
    def address_to_coords(self, address):
        try:
            location = self.geolocator.geocode(address)
            if location:
                return (location.latitude, location.longitude)
        except:
            pass
        return None
    
    def _load_from_cache(self, cache_file):
        try:
            with open(cache_file, 'rb') as f:
                return pickle.load(f)
        except:
            if os.path.exists(cache_file):
                os.remove(cache_file)
            raise
    
    def _sanitize_name(self, name):
        import re
        name = re.sub(r'[^\w\s-]', '', name)
        return re.sub(r'[-\s]+', '_', name).strip('_')[:100]