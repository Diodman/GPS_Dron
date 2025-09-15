import networkx as nx
import geopandas as gpd
import numpy as np
from shapely.geometry import Point, box
import osmnx as ox

class GraphService:
    def __init__(self):
        self.graphs = {}
        self.progress_callbacks = []
    
    def add_progress_callback(self, callback):
        self.progress_callbacks.append(callback)
    
    def _update_progress(self, stage, percentage, message=""):
        for callback in self.progress_callbacks:
            callback(stage, percentage, message)
    
    def build_city_graph(self, city_data, drone_params=None):
        """Построение графа города для дронов"""
        self._update_progress("graph", 0, "Начало построения графа")
        
        if drone_params is None:
            drone_params = {
                'road_weight': 1.0,
                'building_weight': 10.0,  # Больший вес для зданий
                'max_building_ratio': 0.1  # Только 10% через здания
            }
        
        # Получаем граф дорог и здания
        road_graph = city_data['road_graph']
        buildings = city_data['buildings']
        
        # Конвертируем граф OSMnx в NetworkX
        self._update_progress("graph", 20, "Конвертация дорожного графа")
        G = self._convert_ox_graph_to_nx(road_graph)
        
        # Добавляем ограниченное количество узлов над зданиями
        self._update_progress("graph", 50, "Добавление узлов над зданиями")
        G = self._add_limited_building_nodes(G, buildings, drone_params, max_buildings=50)
        
        self._update_progress("graph", 100, "Граф построен")
        return G
    
    def _convert_ox_graph_to_nx(self, ox_graph):
        """Конвертация OSMnx графа в NetworkX граф"""
        G = nx.Graph()
        
        # Добавляем узлы
        for node, data in ox_graph.nodes(data=True):
            G.add_node(node, pos=(data['y'], data['x']), type='road', weight=1.0)
        
        # Добавляем ребра
        for u, v, data in ox_graph.edges(data=True):
            if u in G.nodes and v in G.nodes:
                length = data.get('length', 1.0)
                G.add_edge(u, v, weight=length, type='road')
        
        return G
    
    def _add_limited_building_nodes(self, G, buildings, drone_params, max_buildings=50):
        """Добавление ограниченного количества узлов над зданиями"""
        if len(buildings) == 0:
            return G
        
        # Берем только первые max_buildings зданий для производительности
        limited_buildings = buildings.head(min(max_buildings, len(buildings)))
        
        for i, (idx, building) in enumerate(limited_buildings.iterrows()):
            if building.geometry and not building.geometry.is_empty:
                # Добавляем одну точку в центре здания
                centroid = building.geometry.centroid
                if not centroid.is_empty:
                    node_id = f"building_{idx}"
                    G.add_node(node_id, pos=(centroid.y, centroid.x), 
                              type='building', weight=drone_params['building_weight'])
                    
                    # Соединяем с ближайшей дорогой
                    self._connect_to_nearest_road(G, node_id, (centroid.y, centroid.x))
            
            # Обновляем прогресс
            progress = 50 + (i / len(limited_buildings)) * 40
            self._update_progress("graph", progress, f"Обработка зданий: {i+1}/{len(limited_buildings)}")
        
        return G
    
    def _connect_to_nearest_road(self, G, building_node, building_pos):
        """Соединение узла здания с ближайшей дорогой"""
        min_dist = float('inf')
        nearest_road = None
        
        # Ищем среди дорожных узлов
        road_nodes = [n for n, attr in G.nodes(data=True) if attr.get('type') == 'road']
        
        for road_node in road_nodes[:100]:  # Ограничиваем поиск
            road_pos = G.nodes[road_node]['pos']
            dist = self._calculate_distance(building_pos, road_pos)
            
            if dist < min_dist:
                min_dist = dist
                nearest_road = road_node
        
        if nearest_road and min_dist < 0.01:  # Максимальное расстояние
            G.add_edge(building_node, nearest_road, 
                      weight=min_dist * 1000,  # Увеличиваем вес за соединение
                      type='connection')
    
    def _calculate_distance(self, pos1, pos2):
        """Расчет расстояния между двумя точками (в градусах)"""
        return np.sqrt((pos1[0] - pos2[0])**2 + (pos1[1] - pos2[1])**2)