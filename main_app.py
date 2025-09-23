import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext
import threading
import logging
from data_service import DataService
from graph_service import GraphService
from routing_service import RoutingService
from mapping_service import MappingService

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('drone_planner.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

class ModernDroneRoutePlanner:
    def __init__(self, root):
        self.root = root
        self.root.title("🚁 Advanced Drone Route Planner")
        self.root.geometry("1400x900")
        self.root.configure(bg='#f5f5f5')
        
        # Настройка логирования
        self.logger = logging.getLogger(__name__)
        self.logger.info("Запуск приложения планирования маршрутов дронов")
        
        try:
            # Инициализация сервисов
            self.data_service = DataService()
            self.graph_service = GraphService()
            self.routing_service = RoutingService(self.graph_service)
            self.mapping_service = MappingService()
            
            # Подписка на события прогресса
            self.data_service.add_progress_callback(self.update_progress)
            self.graph_service.add_progress_callback(self.update_progress)
            self.routing_service.add_progress_callback(self.update_progress)
            
            self.current_city = None
            self.last_routes = []
            self.drone_points = []
            self.drone_types = []
            
            self.setup_ui()
            self.logger.info("Приложение успешно инициализировано")
            
        except Exception as e:
            self.logger.error(f"Ошибка инициализации приложения: {e}")
            messagebox.showerror("Критическая ошибка", f"Не удалось запустить приложение: {e}")
            raise
    
    def setup_ui(self):
        # Configure main grid
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)
        
        # Main notebook
        self.notebook = ttk.Notebook(self.root)
        self.notebook.grid(row=0, column=0, sticky='nsew', padx=10, pady=10)
        
        # Setup tabs
        self.setup_route_tab()
        self.setup_management_tab()
        self.setup_log_tab()
        
        # Status bar
        self.status_var = tk.StringVar(value="Готов к работе")
        status_bar = ttk.Label(self.root, textvariable=self.status_var, relief=tk.SUNKEN, anchor=tk.W)
        status_bar.grid(row=1, column=0, sticky='ew', padx=10, pady=(0, 10))
    
    def setup_route_tab(self):
        # Route planning tab
        route_frame = ttk.Frame(self.notebook, padding=20)
        self.notebook.add(route_frame, text="🗺️ Планирование маршрутов")
        
        # Configure grid
        route_frame.columnconfigure(1, weight=1)
        route_frame.rowconfigure(5, weight=1)
        
        # City selection
        ttk.Label(route_frame, text="🏙️ Город:", font=('Arial', 11, 'bold')).grid(row=0, column=0, sticky='w', pady=5)
        self.city_entry = ttk.Entry(route_frame, width=40, font=('Arial', 10))
        self.city_entry.insert(0, "Volgograd, Russia")
        self.city_entry.grid(row=0, column=1, sticky='ew', pady=5, padx=(10, 0))
        
        ttk.Button(route_frame, text="📥 Загрузить данные", command=self.load_city_data, width=20).grid(row=0, column=2, padx=10)
        
        # Drone configuration
        config_frame = ttk.LabelFrame(route_frame, text="Настройки дронов", padding=10)
        config_frame.grid(row=1, column=0, columnspan=3, sticky='ew', pady=10)
        config_frame.columnconfigure(1, weight=1)
        
        ttk.Label(config_frame, text="Тип дрона:").grid(row=0, column=0, sticky='w', pady=2)
        self.drone_type = tk.StringVar(value="cargo")
        drone_combo = ttk.Combobox(config_frame, textvariable=self.drone_type, width=20, state='readonly')
        drone_combo['values'] = ('cargo', 'operator', 'cleaner')
        drone_combo.grid(row=0, column=1, sticky='w', pady=2, padx=(10, 0))
        
        ttk.Label(config_frame, text="Уровень батареи (%):").grid(row=0, column=2, sticky='w', pady=2, padx=(20, 0))
        self.battery_var = tk.StringVar(value="100")
        ttk.Entry(config_frame, textvariable=self.battery_var, width=10).grid(row=0, column=3, sticky='w', pady=2, padx=(10, 0))
        
        ttk.Label(config_frame, text="Количество дронов:").grid(row=1, column=0, sticky='w', pady=2)
        self.drone_count = tk.StringVar(value="1")
        ttk.Entry(config_frame, textvariable=self.drone_count, width=10).grid(row=1, column=1, sticky='w', pady=2, padx=(10, 0))
        
        # Points management
        points_frame = ttk.LabelFrame(route_frame, text="Точки маршрута", padding=10)
        points_frame.grid(row=2, column=0, columnspan=3, sticky='ew', pady=10)
        points_frame.columnconfigure(1, weight=1)
        
        # Address input
        ttk.Label(points_frame, text="Адрес или координаты:").grid(row=0, column=0, sticky='w', pady=2)
        self.address_entry = ttk.Entry(points_frame, width=30)
        self.address_entry.grid(row=0, column=1, sticky='ew', pady=2, padx=(10, 0))
        self.address_entry.bind('<Return>', lambda e: self.add_point())
        
        ttk.Button(points_frame, text="📍 Добавить точку", command=self.add_point, width=15).grid(row=0, column=2, padx=(10, 0))
        ttk.Button(points_frame, text="🗺️ Выбрать на карте", command=self.open_selection_map, width=15).grid(row=0, column=3, padx=(10, 0))
        
        # Points list
        list_frame = ttk.Frame(points_frame)
        list_frame.grid(row=1, column=0, columnspan=4, sticky='ew', pady=(10, 0))
        list_frame.columnconfigure(0, weight=1)
        
        ttk.Label(list_frame, text="Текущие точки:").grid(row=0, column=0, sticky='w')
        self.points_listbox = tk.Listbox(list_frame, height=6, font=('Arial', 9))
        self.points_listbox.grid(row=1, column=0, columnspan=3, sticky='nsew', pady=5)
        
        # Listbox buttons
        btn_frame = ttk.Frame(list_frame)
        btn_frame.grid(row=1, column=3, sticky='ns', padx=(10, 0))
        
        ttk.Button(btn_frame, text="⬆️", command=self.move_point_up, width=5).pack(pady=2)
        ttk.Button(btn_frame, text="⬇️", command=self.move_point_down, width=5).pack(pady=2)
        ttk.Button(btn_frame, text="✖️", command=self.remove_point, width=5).pack(pady=2)
        ttk.Button(btn_frame, text="🧹", command=self.clear_points, width=5).pack(pady=2)
        
        # Route actions
        action_frame = ttk.Frame(route_frame)
        action_frame.grid(row=3, column=0, columnspan=3, pady=20)
        
        ttk.Button(action_frame, text="🛣️ Построить маршруты", command=self.calculate_routes, 
                  style='Accent.TButton', width=20).pack(side=tk.LEFT, padx=5)
        ttk.Button(action_frame, text="📊 Показать на карте", command=self.show_routes_map, 
                  width=20).pack(side=tk.LEFT, padx=5)
        ttk.Button(action_frame, text="💾 Экспорт маршрутов", command=self.export_routes, 
                  width=20).pack(side=tk.LEFT, padx=5)
        
        # Progress
        ttk.Label(route_frame, text="Прогресс:", font=('Arial', 10)).grid(row=4, column=0, sticky='w', pady=(10, 5))
        self.progress_bar = ttk.Progressbar(route_frame, mode='determinate')
        self.progress_bar.grid(row=4, column=1, columnspan=2, sticky='ew', pady=(10, 5), padx=(10, 0))
        
        self.progress_label = ttk.Label(route_frame, text="Готов к работе", font=('Arial', 9), foreground='#666')
        self.progress_label.grid(row=5, column=0, columnspan=3, sticky='w')
        
        # Results
        results_frame = ttk.LabelFrame(route_frame, text="Результаты планирования", padding=10)
        results_frame.grid(row=6, column=0, columnspan=3, sticky='nsew', pady=10)
        results_frame.columnconfigure(0, weight=1)
        results_frame.rowconfigure(0, weight=1)
        
        self.results_text = scrolledtext.ScrolledText(results_frame, width=60, height=12, font=('Consolas', 9))
        self.results_text.pack(fill=tk.BOTH, expand=True)
    
    def setup_management_tab(self):
        # Drone management tab
        management_frame = ttk.Frame(self.notebook, padding=20)
        self.notebook.add(management_frame, text="🚁 Управление дронами")
        
        ttk.Label(management_frame, text="Функционал управления дронами в разработке", 
                 font=('Arial', 14), foreground='#666').pack(expand=True)
    
    def setup_log_tab(self):
        # Log tab
        log_frame = ttk.Frame(self.notebook, padding=10)
        self.notebook.add(log_frame, text="📋 Лог системы")
        
        self.log_text = scrolledtext.ScrolledText(log_frame, width=80, height=20, font=('Consolas', 9))
        self.log_text.pack(fill=tk.BOTH, expand=True)
        
        # Add initial log message
        self.log("=== Система планирования маршрутов дронов запущена ===")
        self.log("Для начала работы:")
        self.log("1. Введите название города и загрузите данные")
        self.log("2. Выберите тип дрона и настройки")
        self.log("3. Добавьте точки маршрута (адрес или выбор на карте)")
        self.log("4. Постройте маршруты")
    
    def log(self, message):
        """Добавление сообщения в лог"""
        self.log_text.config(state=tk.NORMAL)
        self.log_text.insert(tk.END, f"{message}\n")
        self.log_text.see(tk.END)
        self.log_text.config(state=tk.DISABLED)
    
    def update_progress(self, stage, percentage, message=""):
        """Обновление прогресс-бара"""
        def update():
            self.progress_bar['value'] = percentage
            self.progress_label.config(text=message)
            self.status_var.set(message)
            if message:
                self.log(f"[{stage.upper()}] {message}")
        
        self.root.after(0, update)
    
    def load_city_data(self):
        """Загрузка данных города с улучшенной обработкой ошибок"""
        city_name = self.city_entry.get().strip()
        if not city_name:
            messagebox.showwarning("Ошибка", "Введите название города")
            return
        
        def load_thread():
            try:
                self.logger.info(f"Начало загрузки данных для: {city_name}")
                self.log(f"Загрузка данных для: {city_name}")
                
                # Загружаем данные города
                city_data = self.data_service.get_city_data(city_name)
                if not city_data:
                    raise Exception("Не удалось получить данные города")
                
                # Проверяем наличие дорожной сети
                if 'road_graph' not in city_data or len(city_data['road_graph'].nodes) == 0:
                    raise Exception("Дорожная сеть города пуста или недоступна")
                
                drone_type = self.drone_type.get()
                
                # Строим граф города
                city_graph = self.graph_service.build_city_graph(city_data, drone_type)
                if len(city_graph.nodes) == 0:
                    raise Exception("Не удалось построить граф города")
                
                # Сохраняем граф для маршрутизации
                self.routing_service.city_graphs[city_name] = city_graph
                self.current_city = city_name
                
                # Получаем статистику
                stats = city_data.get('stats', {})
                nodes_count = stats.get('nodes', len(city_graph.nodes))
                edges_count = stats.get('edges', len(city_graph.edges))
                buildings_count = stats.get('buildings', 0)
                
                stats_text = f"Узлов: {nodes_count}, Ребер: {edges_count}"
                if buildings_count > 0:
                    stats_text += f", Зданий: {buildings_count}"
                
                self.log(f"✓ Данные загружены: {stats_text}")
                self.logger.info(f"Данные города '{city_name}' успешно загружены: {stats_text}")
                
                messagebox.showinfo("Успех", f"Данные города '{city_name}' загружены\n{stats_text}")
                
            except Exception as e:
                error_msg = str(e)
                self.log(f"✗ Ошибка загрузки: {error_msg}")
                self.logger.error(f"Ошибка загрузки данных для {city_name}: {error_msg}")
                messagebox.showerror("Ошибка", f"Не удалось загрузить данные:\n{error_msg}\n\nПопробуйте:\n1. Проверить интернет-соединение\n2. Указать полное название города\n3. Добавить 'Russia' к названию города")
        
        threading.Thread(target=load_thread, daemon=True).start()
    
    def add_point(self):
        """Добавление точки маршрута с улучшенной обработкой"""
        address = self.address_entry.get().strip()
        if not address:
            messagebox.showwarning("Ошибка", "Введите адрес или координаты")
            return
        
        # Проверяем ограничение на количество точек
        if len(self.drone_points) >= 10:
            messagebox.showwarning("Ограничение", "Максимальное количество точек: 10")
            return
        
        # Попытка парсинга как координат
        if ',' in address:
            try:
                coords = tuple(map(float, [x.strip() for x in address.split(',')]))
                if len(coords) == 2:
                    # Проверяем валидность координат
                    if -90 <= coords[0] <= 90 and -180 <= coords[1] <= 180:
                        point_text = f"{coords[0]:.6f}, {coords[1]:.6f}"
                        self.drone_points.append(coords)
                        self.points_listbox.insert(tk.END, point_text)
                        self.address_entry.delete(0, tk.END)
                        self.log(f"✓ Добавлена точка: {point_text}")
                        self.logger.info(f"Добавлена точка по координатам: {coords}")
                        return
                    else:
                        messagebox.showwarning("Ошибка", "Некорректные координаты:\nШирота: -90 до 90\nДолгота: -180 до 180")
                        return
            except ValueError:
                pass
        
        # Геокодирование адреса
        def geocode_thread():
            try:
                self.log(f"Поиск координат для: {address}")
                # Передаем название текущего города для ограничения поиска
                coords = self.data_service.address_to_coords(address, self.current_city)
                if coords:
                    point_text = f"{coords[0]:.6f}, {coords[1]:.6f} ({address})"
                    self.drone_points.append(coords)
                    self.points_listbox.insert(tk.END, point_text)
                    self.address_entry.delete(0, tk.END)
                    self.log(f"✓ Добавлена точка: {address} → {coords}")
                    self.logger.info(f"Добавлена точка по адресу '{address}': {coords}")
                else:
                    error_msg = f"Не удалось найти адрес: {address}"
                    self.log(f"✗ {error_msg}")
                    self.logger.warning(error_msg)
                    if self.current_city:
                        messagebox.showerror("Ошибка", f"{error_msg}\n\nПопробуйте:\n1. Указать более полный адрес\n2. Убедиться что адрес находится в {self.current_city}\n3. Ввести координаты в формате: широта, долгота")
                    else:
                        messagebox.showerror("Ошибка", f"{error_msg}\n\nПопробуйте:\n1. Сначала загрузить данные города\n2. Указать более полный адрес\n3. Ввести координаты в формате: широта, долгота")
            except Exception as e:
                error_msg = f"Ошибка геокодирования: {str(e)}"
                self.log(f"✗ {error_msg}")
                self.logger.error(f"Ошибка геокодирования для '{address}': {e}")
                messagebox.showerror("Ошибка", error_msg)
        
        threading.Thread(target=geocode_thread, daemon=True).start()
    
    def remove_point(self):
        """Удаление выбранной точки"""
        selection = self.points_listbox.curselection()
        if selection:
            index = selection[0]
            self.points_listbox.delete(index)
            self.drone_points.pop(index)
            self.log("✓ Точка удалена")
    
    def clear_points(self):
        """Очистка всех точек"""
        self.points_listbox.delete(0, tk.END)
        self.drone_points = []
        self.log("✓ Все точки очищены")
    
    def move_point_up(self):
        """Перемещение точки вверх"""
        selection = self.points_listbox.curselection()
        if selection and selection[0] > 0:
            index = selection[0]
            # Swap in listbox
            text = self.points_listbox.get(index)
            self.points_listbox.delete(index)
            self.points_listbox.insert(index-1, text)
            self.points_listbox.select_set(index-1)
            # Swap in points list
            self.drone_points[index], self.drone_points[index-1] = self.drone_points[index-1], self.drone_points[index]
    
    def move_point_down(self):
        """Перемещение точки вниз"""
        selection = self.points_listbox.curselection()
        if selection and selection[0] < len(self.drone_points) - 1:
            index = selection[0]
            # Swap in listbox
            text = self.points_listbox.get(index)
            self.points_listbox.delete(index)
            self.points_listbox.insert(index+1, text)
            self.points_listbox.select_set(index+1)
            # Swap in points list
            self.drone_points[index], self.drone_points[index+1] = self.drone_points[index+1], self.drone_points[index]
    
    def open_selection_map(self):
        """Открытие карты для выбора точек"""
        if not self.current_city:
            messagebox.showwarning("Ошибка", "Сначала загрузите данные города")
            return
        
        def map_thread():
            try:
                filename = self.mapping_service.create_selection_map(self.current_city)
                if filename:
                    self.mapping_service.open_map(filename)
                    self.log("Карта для выбора точек открыта в браузере")
            except Exception as e:
                self.log(f"✗ Ошибка открытия карты: {str(e)}")
        
        threading.Thread(target=map_thread, daemon=True).start()
    
    def calculate_routes(self):
        """Расчет маршрутов для всех дронов с улучшенной валидацией"""
        if not self.current_city:
            messagebox.showwarning("Ошибка", "Сначала загрузите данные города")
            return
        
        if len(self.drone_points) < 2:
            messagebox.showwarning("Ошибка", "Добавьте хотя бы 2 точки маршрута")
            return
        
        try:
            drone_type = self.drone_type.get()
            battery_level = float(self.battery_var.get())
            
            if battery_level <= 0 or battery_level > 100:
                messagebox.showwarning("Ошибка", "Уровень батареи должен быть от 1 до 100%")
                return
            
            # Проверяем что все точки находятся в разумных пределах города
            if not self._validate_points_in_city():
                messagebox.showwarning("Ошибка", "Некоторые точки находятся слишком далеко от города.\nПроверьте координаты точек.")
                return
            
            # Create point pairs for multiple drones
            num_drones = min(int(self.drone_count.get()), len(self.drone_points) - 1)
            point_pairs = []
            
            # Проверяем расстояния между точками
            for i in range(num_drones):
                start = self.drone_points[i % len(self.drone_points)]
                end = self.drone_points[(i + 1) % len(self.drone_points)]
                
                # Вычисляем расстояние между точками
                distance = self._calculate_distance(start, end)
                
                if distance < 10:  # Менее 10 метров
                    self.log(f"⚠️ Очень близкие точки {i+1}: {distance:.1f} м")
                    if not messagebox.askyesno("Близкие точки", 
                                             f"Точки {i+1} находятся очень близко друг к другу: {distance:.1f} м\n"
                                             f"Маршрут будет очень коротким.\n"
                                             f"Продолжить?"):
                        return
                elif distance > 50000:  # 50 км
                    self.log(f"⚠️ Большое расстояние между точками {i+1}: {distance/1000:.1f} км")
                    if not messagebox.askyesno("Большое расстояние", 
                                             f"Расстояние между точками {i+1}: {distance/1000:.1f} км\n"
                                             f"Это может привести к проблемам с маршрутизацией.\n"
                                             f"Продолжить?"):
                        return
                
                point_pairs.append((start, end))
                self.drone_types.append(drone_type)
            
            self.log(f"Расчет маршрутов для {num_drones} дронов типа '{drone_type}'...")
            
            def route_thread():
                try:
                    routes = self.routing_service.plan_routes(
                        self.current_city, point_pairs, drone_type, battery_level
                    )
                    
                    if not routes:
                        self.log("✗ Маршруты не найдены")
                        messagebox.showwarning("Ошибка", "Не удалось построить маршруты.\nВозможные причины:\n1. Точки находятся слишком далеко друг от друга\n2. Недостаточно данных дорожной сети между точками\n3. Низкий уровень батареи")
                        return
                    
                    self.last_routes = routes
                    
                    # Display results
                    self.results_text.delete(1.0, tk.END)
                    self.results_text.insert(tk.END, "✅ МАРШРУТЫ ПОСТРОЕНЫ\n\n")
                    
                    for i, (path, length, coords) in enumerate(routes):
                        self.results_text.insert(tk.END, f"Дрон {i+1} ({drone_type}):\n")
                        self.results_text.insert(tk.END, f"  Длина: {length:.1f} м\n")
                        self.results_text.insert(tk.END, f"  Точек: {len(coords)}\n")
                        self.results_text.insert(tk.END, f"  Старт: {coords[0][0]:.6f}, {coords[0][1]:.6f}\n")
                        self.results_text.insert(tk.END, f"  Финиш: {coords[-1][0]:.6f}, {coords[-1][1]:.6f}\n\n")
                    
                    self.log(f"✓ Построено {len(routes)} маршрутов")
                    
                except Exception as e:
                    error_msg = str(e)
                    self.log(f"✗ Ошибка расчета: {error_msg}")
                    messagebox.showerror("Ошибка", f"Ошибка расчета маршрутов: {error_msg}")
            
            threading.Thread(target=route_thread, daemon=True).start()
            
        except ValueError:
            self.log("✗ Ошибка в формате данных")
            messagebox.showerror("Ошибка", "Проверьте правильность введенных данных")
        except Exception as e:
            error_msg = str(e)
            self.log(f"✗ Ошибка: {error_msg}")
            messagebox.showerror("Ошибка", f"Ошибка: {error_msg}")
    
    def _validate_points_in_city(self):
        """Проверка что все точки находятся в границах города"""
        if not self.current_city:
            return True
        
        city_bounds = {
            'волгоград': {'lat': (48.5, 49.0), 'lon': (44.0, 45.0)},
            'volgograd': {'lat': (48.5, 49.0), 'lon': (44.0, 45.0)},
        }
        
        normalized_city = self.current_city.lower().strip()
        if ', russia' in normalized_city:
            normalized_city = normalized_city[:-8].strip()
        
        bounds = None
        for city_key, city_bounds_data in city_bounds.items():
            if city_key in normalized_city:
                bounds = city_bounds_data
                break
        
        if not bounds:
            return True  # Если город не найден в списке, считаем точки валидными
        
        for i, point in enumerate(self.drone_points):
            lat, lon = point
            if not (bounds['lat'][0] <= lat <= bounds['lat'][1] and bounds['lon'][0] <= lon <= bounds['lon'][1]):
                self.log(f"✗ Точка {i+1} ({lat:.6f}, {lon:.6f}) не в границах города")
                return False
        
        return True
    
    def _calculate_distance(self, point1, point2):
        """Вычисление расстояния между двумя точками в метрах"""
        import math
        
        lat1, lon1 = point1
        lat2, lon2 = point2
        
        # Формула гаверсинуса для вычисления расстояния на сфере
        R = 6371000  # Радиус Земли в метрах
        
        lat1_rad = math.radians(lat1)
        lat2_rad = math.radians(lat2)
        delta_lat = math.radians(lat2 - lat1)
        delta_lon = math.radians(lon2 - lon1)
        
        a = (math.sin(delta_lat / 2) ** 2 + 
             math.cos(lat1_rad) * math.cos(lat2_rad) * 
             math.sin(delta_lon / 2) ** 2)
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
        
        distance = R * c
        return distance
    
    def show_routes_map(self):
        """Показать маршруты на карте"""
        if not self.last_routes:
            messagebox.showwarning("Ошибка", "Сначала постройте маршруты")
            return
        
        def map_thread():
            try:
                filename = self.mapping_service.create_route_map(
                    self.current_city, self.last_routes, self.drone_types
                )
                if filename:
                    self.mapping_service.open_map(filename)
                    self.log("Карта маршрутов открыта в браузере")
            except Exception as e:
                self.log(f"✗ Ошибка создания карты: {str(e)}")
        
        threading.Thread(target=map_thread, daemon=True).start()
    
    def export_routes(self):
        """Экспорт маршрутов"""
        if not self.last_routes:
            messagebox.showwarning("Ошибка", "Нет маршрутов для экспорта")
            return
        
        # Здесь можно добавить логику экспорта в файл
        self.log("✓ Маршруты подготовлены для экспорта")
        messagebox.showinfo("Экспорт", "Функция экспорта в разработке")

if __name__ == "__main__":
    root = tk.Tk()
    app = ModernDroneRoutePlanner(root)
    root.mainloop()