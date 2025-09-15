import networkx as nx
import numpy as np

class RoutingService:
    def __init__(self, graph_service):
        self.graph_service = graph_service
        self.city_graphs = {}
        self.progress_callbacks = []
    
    def add_progress_callback(self, callback):
        self.progress_callbacks.append(callback)
    
    def _update_progress(self, stage, percentage, message=""):
        for callback in self.progress_callbacks:
            callback(stage, percentage, message)
    
    def plan_route(self, city_name, start, end, max_building_ratio=0.1):
        """Планирование маршрута с ограничением на прохождение через здания"""
        self._update_progress("route", 0, "Начало планирования маршрута")
        
        if city_name not in self.city_graphs:
            raise ValueError(f"Graph for {city_name} not loaded")
        
        G = self.city_graphs[city_name]
        
        # Находим ближайшие узлы к начальной и конечной точкам
        self._update_progress("route", 30, "Поиск ближайших узлов")
        start_node = self._find_nearest_node(G, start)
        end_node = self._find_nearest_node(G, end)
        
        if not start_node or not end_node:
            self._update_progress("route", 0, "Не найдены ближайшие узлы")
            return None, None, None
        
        # Планируем маршрут
        self._update_progress("route", 60, "Поиск оптимального пути")
        path = self._find_path_with_constraints(G, start_node, end_node, max_building_ratio)
        
        if not path:
            self._update_progress("route", 0, "Путь не найден")
            return None, None, None
        
        # Извлекаем координаты пути
        self._update_progress("route", 90, "Формирование координат")
        coords = [G.nodes[node]['pos'] for node in path]
        length = self._calculate_path_length(G, path)
        
        self._update_progress("route", 100, "Маршрут построен")
        return path, length, coords
    
    def _find_nearest_node(self, G, point):
        """Поиск ближайшего узла к точке"""
        min_dist = float('inf')
        nearest_node = None
        
        # Преобразуем point в правильный формат (lat, lon)
        target_point = (point[0], point[1])
        
        # Ищем среди всех узлов
        for node, attr in G.nodes(data=True):
            node_point = attr['pos']
            dist = self._calculate_distance(target_point, node_point)
            if dist < min_dist:
                min_dist = dist
                nearest_node = node
        
        return nearest_node
    
    def _find_path_with_constraints(self, G, start, end, max_building_ratio):
        """Поиск пути с ограничениями на здания"""
        try:
            # Сначала пробуем найти путь только по дорогам
            road_path = self._find_road_path(G, start, end)
            if road_path:
                return road_path
            
            # Если не нашли, пробуем с учетом зданий
            return nx.astar_path(G, start, end, weight='weight')
            
        except nx.NetworkXNoPath:
            return None
    
    def _find_road_path(self, G, start, end):
        """Поиск пути только по дорогам"""
        # Создаем подграф только с дорогами
        road_nodes = [n for n, attr in G.nodes(data=True) if attr.get('type') == 'road']
        road_edges = [(u, v) for u, v, attr in G.edges(data=True) if attr.get('type') == 'road']
        
        road_subgraph = G.subgraph(road_nodes).copy()
        
        try:
            return nx.astar_path(road_subgraph, start, end, weight='weight')
        except (nx.NetworkXNoPath, nx.NodeNotFound):
            return None
    
    def _calculate_path_length(self, G, path):
        """Расчет длины пути"""
        length = 0
        for i in range(len(path) - 1):
            node1, node2 = path[i], path[i+1]
            if G.has_edge(node1, node2):
                length += G[node1][node2].get('weight', 1.0)
            else:
                # Если нет прямого ребра, вычисляем расстояние
                pos1 = G.nodes[node1]['pos']
                pos2 = G.nodes[node2]['pos']
                length += self._calculate_distance(pos1, pos2)
        return length
    
    def _calculate_distance(self, pos1, pos2):
        """Расчет расстояния между двумя точками"""
        return np.sqrt((pos1[0] - pos2[0])**2 + (pos1[1] - pos2[1])**2)