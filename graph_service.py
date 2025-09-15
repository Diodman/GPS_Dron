import networkx as nx
import numpy as np
from shapely.geometry import Point
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
    
    def build_city_graph(self, city_data, drone_type="cargo"):
        self._update_progress("graph", 0, "Построение графа для " + drone_type)
        
        drone_params = self._get_drone_params(drone_type)
        road_graph = city_data['road_graph']
        buildings = city_data['buildings']
        no_fly_zones = city_data['no_fly_zones']
        
        self._update_progress("graph", 20, "Конвертация дорожного графа")
        G = self._convert_ox_graph_to_nx(road_graph)
        
        self._update_progress("graph", 50, "Добавление узлов для " + drone_type)
        G = self._add_drone_specific_nodes(G, buildings, drone_params, drone_type)
        
        self._update_progress("graph", 80, "Добавление запретных зон")
        G = self._add_no_fly_zones(G, no_fly_zones)
        
        self._update_progress("graph", 100, "Граф построен")
        return G
    
    def _get_drone_params(self, drone_type):
        params = {
            "cargo": {"weight": 2.0, "max_altitude": 200, "battery_range": 20000},
            "operator": {"weight": 1.5, "max_altitude": 150, "battery_range": 15000},
            "cleaner": {"weight": 1.0, "max_altitude": 100, "battery_range": 10000}
        }
        return params.get(drone_type, params["cargo"])
    
    def _convert_ox_graph_to_nx(self, ox_graph):
        G = nx.Graph()
        for node, data in ox_graph.nodes(data=True):
            G.add_node(node, pos=(data['y'], data['x']), type='road', weight=1.0)
        for u, v, data in ox_graph.edges(data=True):
            if u in G.nodes and v in G.nodes:
                length = data.get('length', 1.0)
                G.add_edge(u, v, weight=length, type='road')
        return G
    
    def _add_drone_specific_nodes(self, G, buildings, drone_params, drone_type):
        if drone_type == "cleaner":
            for idx, building in buildings.head(20).iterrows():
                if building.geometry and not building.geometry.is_empty:
                    centroid = building.geometry.centroid
                    if not centroid.is_empty:
                        node_id = f"{drone_type}_{idx}"
                        G.add_node(node_id, pos=(centroid.y, centroid.x), 
                                  type=drone_type, weight=drone_params['weight'])
                        self._connect_to_nearest_road(G, node_id, (centroid.y, centroid.x))
        return G
    
    def _add_no_fly_zones(self, G, no_fly_zones):
        for zone in no_fly_zones:
            for node, attr in list(G.nodes(data=True)):
                if self._point_in_no_fly_zone(attr['pos'], zone):
                    G.nodes[node]['weight'] = float('inf')
        return G
    
    def _point_in_no_fly_zone(self, point, zone):
        return False  # Заглушка для проверки точек в запретных зонах
    
    def _connect_to_nearest_road(self, G, building_node, building_pos):
        min_dist = float('inf')
        nearest_road = None
        road_nodes = [n for n, attr in G.nodes(data=True) if attr.get('type') == 'road']
        for road_node in road_nodes[:100]:
            road_pos = G.nodes[road_node]['pos']
            dist = np.sqrt((building_pos[0]-road_pos[0])**2 + (building_pos[1]-road_pos[1])**2)
            if dist < min_dist:
                min_dist = dist
                nearest_road = road_node
        if nearest_road and min_dist < 0.01:
            G.add_edge(building_node, nearest_road, weight=min_dist*1000, type='connection')