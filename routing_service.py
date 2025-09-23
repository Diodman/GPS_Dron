import networkx as nx
import numpy as np
from collections import deque
import logging

class RoutingService:
    def __init__(self, graph_service):
        self.graph_service = graph_service
        self.city_graphs = {}
        self.progress_callbacks = []
        self.active_routes = {}
        self.logger = logging.getLogger(__name__)
    
    def add_progress_callback(self, callback):
        self.progress_callbacks.append(callback)
    
    def _update_progress(self, stage, percentage, message=""):
        for callback in self.progress_callbacks:
            callback(stage, percentage, message)
    
    def plan_routes(self, city_name, points, drone_type="cargo", battery_level=100):
        self._update_progress("route", 0, f"Планирование для {len(points)} дронов")
        
        try:
            if city_name not in self.city_graphs:
                raise ValueError(f"Граф для города '{city_name}' не загружен")
            
            G = self.city_graphs[city_name]
            if len(G.nodes) == 0:
                raise ValueError(f"Граф города '{city_name}' пуст")
            
            drone_params = self._get_drone_params(drone_type)
            max_range = drone_params['battery_range'] * (battery_level / 100)
            
            self.logger.info(f"Планирование маршрутов для {len(points)} дронов типа {drone_type}, батарея: {battery_level}%, макс. дальность: {max_range:.0f}м")
            
            routes = []
            successful_routes = 0
            
            for i, (start, end) in enumerate(points):
                try:
                    progress = int((i / len(points)) * 80)
                    self._update_progress("route", progress, f"Маршрут {i+1}/{len(points)}")
                    
                    self.logger.info(f"Поиск маршрута {i+1}: {start} → {end}")
                    
                    start_node = self._find_nearest_node(G, start)
                    end_node = self._find_nearest_node(G, end)
                    
                    if not start_node:
                        self.logger.warning(f"Не найден стартовый узел для точки {start}")
                        continue
                    
                    if not end_node:
                        self.logger.warning(f"Не найден конечный узел для точки {end}")
                        continue
                    
                    if start_node == end_node:
                        self.logger.warning(f"Стартовая и конечная точки совпадают: {start_node}")
                        # Попробуем найти ближайший соседний узел для создания реального маршрута
                        neighbors = list(G.neighbors(start_node))
                        if neighbors:
                            # Используем первого соседа как промежуточную точку
                            intermediate_node = neighbors[0]
                            path = [start_node, intermediate_node, start_node]
                            coords = [G.nodes[node]['pos'] for node in path]
                            length = self._calculate_path_length(G, path)
                            routes.append((path, length, coords))
                            successful_routes += 1
                            self.logger.info(f"✓ Создан циклический маршрут: {length:.1f}м")
                        else:
                            # Если нет соседей, создаем минимальный маршрут
                            coords = [G.nodes[start_node]['pos']]
                            routes.append(([start_node], 0.0, coords))
                            successful_routes += 1
                            self.logger.info("✓ Создан минимальный маршрут (точка)")
                        continue
                    
                    path = self._find_safe_path(G, start_node, end_node, max_range)
                    if path and len(path) > 1:
                        coords = [G.nodes[node]['pos'] for node in path]
                        length = self._calculate_path_length(G, path)
                        
                        if length <= max_range:
                            routes.append((path, length, coords))
                            successful_routes += 1
                            self.logger.info(f"✓ Маршрут {i+1} найден: {length:.1f}м, {len(path)} точек")
                        else:
                            self.logger.warning(f"Маршрут {i+1} слишком длинный: {length:.1f}м > {max_range:.1f}м")
                    else:
                        self.logger.warning(f"Не удалось найти маршрут {i+1}")
                        
                except Exception as e:
                    self.logger.error(f"Ошибка планирования маршрута {i+1}: {e}")
                    continue
            
            self._update_progress("route", 100, f"Построено {successful_routes}/{len(points)} маршрутов")
            self.logger.info(f"Планирование завершено: {successful_routes}/{len(points)} успешных маршрутов")
            return routes
            
        except Exception as e:
            self.logger.error(f"Критическая ошибка планирования маршрутов: {e}")
            self._update_progress("error", 0, f"Ошибка планирования: {str(e)}")
            raise

    def plan_direct_path(self, G, start_coords, end_coords, max_range, waypoints=None):
        """Планирование маршрута от двери до двери: добавляем временные узлы у точек."""
        try:
            if G is None or len(G.nodes) == 0:
                return None, None, 0.0

            # Find nearest graph nodes to connect temporary points
            start_nearest = self._find_nearest_node(G, start_coords)
            end_nearest = self._find_nearest_node(G, end_coords)
            if start_nearest is None or end_nearest is None:
                return None, None, 0.0

            # Create temp nodes
            tmp_start = ('tmp_start', id(start_coords))
            tmp_end = ('tmp_end', id(end_coords))

            # Ensure unique keys not colliding with existing nodes
            while tmp_start in G.nodes:
                tmp_start = (tmp_start[0], tmp_start[1] + 1)
            while tmp_end in G.nodes:
                tmp_end = (tmp_end[0], tmp_end[1] + 1)

            # Add temp nodes and connect to nearest nodes with realistic weights
            G.add_node(tmp_start, pos=start_coords, type='temp')
            G.add_node(tmp_end, pos=end_coords, type='temp')

            def _approx_dist(a, b):
                # approximate meters between lat/lon pairs
                lat_diff = (a[0] - b[0]) * 111000.0
                avg_lat = (a[0] + b[0]) / 2.0
                lon_diff = (a[1] - b[1]) * 111000.0 * np.cos(np.radians(avg_lat))
                return float(np.sqrt(lat_diff**2 + lon_diff**2))

            # Connect to several nearest neighbors to avoid being blocked by zones
            candidates_start = []
            candidates_end = []
            for node, attr in G.nodes(data=True):
                pos = attr.get('pos')
                if not pos:
                    continue
                if attr.get('weight', 0) == float('inf'):
                    continue
                candidates_start.append((node, _approx_dist(start_coords, pos)))
                candidates_end.append((node, _approx_dist(end_coords, pos)))

            candidates_start.sort(key=lambda x: x[1])
            candidates_end.sort(key=lambda x: x[1])
            k = 5
            for node, dist in candidates_start[:k]:
                G.add_edge(tmp_start, node, weight=max(1.0, dist), type='temp_connection')
            for node, dist in candidates_end[:k]:
                G.add_edge(tmp_end, node, weight=max(1.0, dist), type='temp_connection')

            # Build waypoint chain
            sequence = [tmp_start]
            waypoints = waypoints or []
            temp_nodes = []
            for wp in waypoints:
                node = ('tmp_wp', id(wp))
                while node in G.nodes:
                    node = (node[0], node[1] + 1)
                G.add_node(node, pos=wp, type='temp')
                for n, dist in sorted(candidates_start, key=lambda x: x[1])[:k]:
                    G.add_edge(node, n, weight=max(1.0, _approx_dist(wp, G.nodes[n]['pos'])), type='temp_connection')
                sequence.append(node)
                temp_nodes.append(node)
            sequence.append(tmp_end)

            # Find path through sequence
            full_path = []
            for i in range(len(sequence)-1):
                seg = self._find_safe_path(G, sequence[i], sequence[i+1], max_range)
                if not seg:
                    full_path = None
                    break
                if i > 0:
                    seg = seg[1:]
                full_path.extend(seg)

            path = full_path

            # Cleanup temp nodes/edges
            try:
                if tmp_start in G:
                    G.remove_node(tmp_start)
                if tmp_end in G:
                    G.remove_node(tmp_end)
                for n in temp_nodes:
                    if n in G:
                        G.remove_node(n)
            except Exception:
                pass

            if not path:
                return None, None, 0.0
            coords = [G.nodes[n]['pos'] if n in G.nodes else (start_coords if n == tmp_start else end_coords) for n in path]
            length = self._calculate_path_length(G, path)
            return path, coords, length
        except Exception as e:
            self.logger.error(f"Ошибка door-to-door планирования: {e}")
            return None, None, 0.0
    
    def emergency_landing(self, drone_id, current_pos, obstacle_pos):
        emergency_path = self._calculate_emergency_path(current_pos, obstacle_pos)
        return emergency_path
    
    def _find_safe_path(self, G, start, end, max_range):
        """Поиск безопасного пути с использованием различных алгоритмов"""
        try:
            # Проверяем существование узлов
            if start not in G.nodes or end not in G.nodes:
                self.logger.warning(f"Узлы {start} или {end} не найдены в графе")
                return None
            
            # Если старт и финиш совпадают
            if start == end:
                self.logger.debug("Старт и финиш совпадают")
                return [start]
            
            # Проверяем связность
            if not nx.has_path(G, start, end):
                self.logger.warning(f"Нет пути между узлами {start} и {end}")
                # Попробуем найти ближайший доступный узел к конечной точке
                alternative_end = self._find_nearest_reachable_node(G, start, end)
                if alternative_end and alternative_end != start:
                    self.logger.info(f"Используем альтернативную конечную точку: {alternative_end}")
                    end = alternative_end
                else:
                    return None
            
            # Пробуем разные алгоритмы поиска пути
            algorithms = [
                ('astar', lambda: nx.astar_path(G, start, end, weight='weight')),
                ('dijkstra', lambda: nx.dijkstra_path(G, start, end, weight='weight')),
                ('shortest', lambda: nx.shortest_path(G, start, end, weight='weight'))
            ]
            
            best_path = None
            best_length = float('inf')
            
            for algo_name, algo_func in algorithms:
                try:
                    path = algo_func()
                    if path and len(path) > 1:
                        path_length = self._calculate_path_length(G, path)
                        self.logger.debug(f"Алгоритм {algo_name}: {len(path)} точек, {path_length:.1f}м")
                        
                        if path_length <= max_range:
                            self.logger.debug(f"Найден подходящий путь алгоритмом {algo_name}")
                            return path
                        elif path_length < best_length:
                            best_path = path
                            best_length = path_length
                            
                except Exception as e:
                    self.logger.debug(f"Алгоритм {algo_name} не сработал: {e}")
                    continue
            
            # Если не нашли подходящий путь, но есть слишком длинный
            if best_path and best_length < max_range * 2:  # Если путь не слишком длинный
                self.logger.warning(f"Найден слишком длинный путь: {best_length:.1f}м > {max_range:.1f}м")
                return best_path
            
            self.logger.warning(f"Не удалось найти подходящий путь между {start} и {end}")
            return None
            
        except Exception as e:
            self.logger.error(f"Ошибка поиска пути: {e}")
            return None
    
    def _find_nearest_reachable_node(self, G, start, target):
        """Поиск ближайшего достижимого узла к целевой точке"""
        try:
            target_pos = G.nodes[target]['pos']
            reachable_nodes = nx.node_connected_component(G, start)
            
            min_distance = float('inf')
            nearest_node = None
            
            for node in reachable_nodes:
                if node != start:
                    node_pos = G.nodes[node]['pos']
                    distance = np.sqrt((target_pos[0] - node_pos[0])**2 + (target_pos[1] - node_pos[1])**2)
                    if distance < min_distance:
                        min_distance = distance
                        nearest_node = node
            
            return nearest_node
            
        except Exception as e:
            self.logger.error(f"Ошибка поиска ближайшего достижимого узла: {e}")
            return None
    
    def _calculate_emergency_path(self, current_pos, obstacle_pos):
        return [current_pos, (current_pos[0] + 0.001, current_pos[1] + 0.001)]
    
    def _find_nearest_node(self, G, point):
        """Поиск ближайшего узла к заданной точке"""
        try:
            if not point or len(point) != 2:
                self.logger.warning(f"Некорректная точка: {point}")
                return None
            
            min_dist = float('inf')
            nearest_node = None
            
            # Используем пространственный индекс для более точного поиска
            # Сначала найдем узлы в радиусе ~1км от точки
            search_radius = 0.01  # примерно 1км в градусах
            
            lat, lon = point
            
            # Создаем список узлов для проверки
            nodes_to_check = []
            for node, attr in G.nodes(data=True):
                try:
                    if 'pos' not in attr:
                        continue
                    
                    node_pos = attr['pos']
                    if len(node_pos) != 2:
                        continue
                    
                    node_lat, node_lon = node_pos
                    
                    # Проверяем, находится ли узел в радиусе поиска
                    if (abs(node_lat - lat) <= search_radius and 
                        abs(node_lon - lon) <= search_radius):
                        nodes_to_check.append((node, node_pos))
                        
                except Exception as e:
                    self.logger.debug(f"Ошибка обработки узла {node}: {e}")
                    continue
            
            # Если в радиусе нет узлов, расширяем поиск
            if not nodes_to_check:
                self.logger.warning(f"Нет узлов в радиусе {search_radius}° от точки {point}, расширяем поиск")
                search_radius = 0.1  # примерно 10км
                
                for node, attr in G.nodes(data=True):
                    try:
                        if 'pos' not in attr:
                            continue
                        
                        node_pos = attr['pos']
                        if len(node_pos) != 2:
                            continue
                        
                        node_lat, node_lon = node_pos
                        
                        if (abs(node_lat - lat) <= search_radius and 
                            abs(node_lon - lon) <= search_radius):
                            nodes_to_check.append((node, node_pos))
                            
                    except Exception as e:
                        continue
            
            # Если все еще нет узлов, ищем среди всех узлов (но ограничиваем количество)
            if not nodes_to_check:
                self.logger.warning(f"Нет узлов в расширенном радиусе, ищем среди всех узлов")
                max_nodes = min(5000, len(G.nodes))
                nodes_checked = 0
                
                for node, attr in G.nodes(data=True):
                    try:
                        if 'pos' not in attr:
                            continue
                        
                        node_pos = attr['pos']
                        if len(node_pos) != 2:
                            continue
                        
                        nodes_to_check.append((node, node_pos))
                        nodes_checked += 1
                        
                        if nodes_checked >= max_nodes:
                            break
                            
                    except Exception as e:
                        continue
            
            # Находим ближайший узел среди отобранных
            for node, node_pos in nodes_to_check:
                try:
                    # Вычисляем расстояние в градусах
                    dist = np.sqrt((lat - node_pos[0])**2 + (lon - node_pos[1])**2)
                    if dist < min_dist:
                        min_dist = dist
                        nearest_node = node
                        
                except Exception as e:
                    self.logger.debug(f"Ошибка вычисления расстояния для узла {node}: {e}")
                    continue
            
            if nearest_node:
                # Преобразуем расстояние в метры для логирования
                distance_meters = min_dist * 111000  # приблизительно
                self.logger.info(f"Найден ближайший узел {nearest_node} на расстоянии {distance_meters:.1f}м от {point}")
            else:
                self.logger.error(f"Не удалось найти ближайший узел для точки {point}")
            
            return nearest_node
            
        except Exception as e:
            self.logger.error(f"Ошибка поиска ближайшего узла: {e}")
            return None
    
    def _calculate_path_length(self, G, path):
        """Вычисление длины пути в метрах"""
        try:
            if not path or len(path) < 2:
                return 0.0
            
            length = 0.0
            for i in range(len(path) - 1):
                node1, node2 = path[i], path[i + 1]
                
                try:
                    if G.has_edge(node1, node2):
                        # Используем вес ребра если он есть
                        edge_weight = G[node1][node2].get('weight', 0)
                        if edge_weight > 0:
                            length += edge_weight
                        else:
                            # Вычисляем расстояние между узлами
                            length += self._calculate_node_distance(G, node1, node2)
                    else:
                        # Вычисляем расстояние между несвязанными узлами
                        length += self._calculate_node_distance(G, node1, node2)
                        
                except Exception as e:
                    self.logger.debug(f"Ошибка вычисления расстояния между {node1} и {node2}: {e}")
                    continue
            
            return length
            
        except Exception as e:
            self.logger.error(f"Ошибка вычисления длины пути: {e}")
            return 0.0
    
    def _calculate_node_distance(self, G, node1, node2):
        """Вычисление расстояния между двумя узлами в метрах"""
        try:
            pos1 = G.nodes[node1]['pos']
            pos2 = G.nodes[node2]['pos']
            
            # Вычисляем расстояние в градусах
            lat_diff = pos1[0] - pos2[0]
            lon_diff = pos1[1] - pos2[1]
            
            # Приблизительное преобразование в метры
            # 1 градус широты ≈ 111 км, 1 градус долготы зависит от широты
            lat_meters = lat_diff * 111000  # широта
            avg_lat = (pos1[0] + pos2[0]) / 2
            lon_meters = lon_diff * 111000 * np.cos(np.radians(avg_lat))  # долгота
            
            distance = np.sqrt(lat_meters**2 + lon_meters**2)
            return distance
            
        except Exception as e:
            self.logger.debug(f"Ошибка вычисления расстояния между узлами: {e}")
            return 1000.0  # Возвращаем большое значение по умолчанию
    
    def _get_drone_params(self, drone_type):
        return {
            "cargo": {"battery_range": 20000},
            "operator": {"battery_range": 15000},
            "cleaner": {"battery_range": 10000}
        }.get(drone_type, {"battery_range": 20000})