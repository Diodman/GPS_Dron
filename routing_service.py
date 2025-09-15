import networkx as nx
import numpy as np
from collections import deque

class RoutingService:
    def __init__(self, graph_service):
        self.graph_service = graph_service
        self.city_graphs = {}
        self.progress_callbacks = []
        self.active_routes = {}
    
    def add_progress_callback(self, callback):
        self.progress_callbacks.append(callback)
    
    def _update_progress(self, stage, percentage, message=""):
        for callback in self.progress_callbacks:
            callback(stage, percentage, message)
    
    def plan_routes(self, city_name, points, drone_type="cargo", battery_level=100):
        self._update_progress("route", 0, f"Планирование для {len(points)} дронов")
        
        if city_name not in self.city_graphs:
            raise ValueError(f"Graph for {city_name} not loaded")
        
        G = self.city_graphs[city_name]
        drone_params = self._get_drone_params(drone_type)
        max_range = drone_params['battery_range'] * (battery_level / 100)
        
        routes = []
        for i, (start, end) in enumerate(points):
            self._update_progress("route", (i/len(points))*80, f"Маршрут {i+1}/{len(points)}")
            
            start_node = self._find_nearest_node(G, start)
            end_node = self._find_nearest_node(G, end)
            
            if start_node and end_node:
                path = self._find_safe_path(G, start_node, end_node, max_range)
                if path:
                    coords = [G.nodes[node]['pos'] for node in path]
                    length = self._calculate_path_length(G, path)
                    routes.append((path, length, coords))
        
        self._update_progress("route", 100, "Все маршруты построены")
        return routes
    
    def emergency_landing(self, drone_id, current_pos, obstacle_pos):
        emergency_path = self._calculate_emergency_path(current_pos, obstacle_pos)
        return emergency_path
    
    def _find_safe_path(self, G, start, end, max_range):
        try:
            path = nx.astar_path(G, start, end, weight='weight')
            if self._calculate_path_length(G, path) <= max_range:
                return path
        except:
            pass
        return None
    
    def _calculate_emergency_path(self, current_pos, obstacle_pos):
        return [current_pos, (current_pos[0] + 0.001, current_pos[1] + 0.001)]
    
    def _find_nearest_node(self, G, point):
        min_dist = float('inf')
        nearest_node = None
        for node, attr in G.nodes(data=True):
            dist = np.sqrt((point[0]-attr['pos'][0])**2 + (point[1]-attr['pos'][1])**2)
            if dist < min_dist:
                min_dist = dist
                nearest_node = node
        return nearest_node
    
    def _calculate_path_length(self, G, path):
        length = 0
        for i in range(len(path)-1):
            node1, node2 = path[i], path[i+1]
            if G.has_edge(node1, node2):
                length += G[node1][node2].get('weight', 1.0)
            else:
                pos1, pos2 = G.nodes[node1]['pos'], G.nodes[node2]['pos']
                length += np.sqrt((pos1[0]-pos2[0])**2 + (pos1[1]-pos2[1])**2)
        return length
    
    def _get_drone_params(self, drone_type):
        return {
            "cargo": {"battery_range": 20000},
            "operator": {"battery_range": 15000},
            "cleaner": {"battery_range": 10000}
        }.get(drone_type, {"battery_range": 20000})