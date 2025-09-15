import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext
import threading
from data_service import DataService
from graph_service import GraphService
from routing_service import RoutingService
from mapping_service import MappingService

class ModernDroneRoutePlanner:
    def __init__(self, root):
        self.root = root
        self.root.title("🚁 Планировщик маршрутов дронов")
        self.root.geometry("1000x700")
        self.root.configure(bg='#f0f0f0')
        
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
        self.last_route = None
        
        self.setup_ui()
    
    def setup_ui(self):
        """Настройка современного интерфейса"""
        # Main frame
        main_frame = ttk.Frame(self.root, padding="20")
        main_frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        
        # Configure grid weights
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)
        main_frame.columnconfigure(1, weight=1)
        
        # Header
        header = ttk.Label(main_frame, text="🚁 Планировщик маршрутов дронов", 
                          font=('Arial', 16, 'bold'), foreground='#2E7D32')
        header.grid(row=0, column=0, columnspan=2, pady=(0, 20))
        
        # Input fields
        ttk.Label(main_frame, text="🏙️ Город:", font=('Arial', 10)).grid(row=1, column=0, sticky=tk.W, pady=5)
        self.city_entry = ttk.Entry(main_frame, width=40, font=('Arial', 10))
        self.city_entry.insert(0, "Berlin, Germany")
        self.city_entry.grid(row=1, column=1, sticky=(tk.W, tk.E), pady=5, padx=(10, 0))
        
        ttk.Label(main_frame, text="📍 Старт (широта, долгота):", font=('Arial', 10)).grid(row=2, column=0, sticky=tk.W, pady=5)
        self.start_entry = ttk.Entry(main_frame, width=40, font=('Arial', 10))
        self.start_entry.insert(0, "52.521918, 13.413215")
        self.start_entry.grid(row=2, column=1, sticky=(tk.W, tk.E), pady=5, padx=(10, 0))
        
        ttk.Label(main_frame, text="🎯 Финиш (широта, долгота):", font=('Arial', 10)).grid(row=3, column=0, sticky=tk.W, pady=5)
        self.end_entry = ttk.Entry(main_frame, width=40, font=('Arial', 10))
        self.end_entry.insert(0, "52.516275, 13.377704")
        self.end_entry.grid(row=3, column=1, sticky=(tk.W, tk.E), pady=5, padx=(10, 0))
        
        # Buttons frame
        button_frame = ttk.Frame(main_frame)
        button_frame.grid(row=4, column=0, columnspan=2, pady=20)
        
        self.load_btn = ttk.Button(button_frame, text="📥 Загрузить данные города", 
                                  command=self.load_city_data, width=20)
        self.load_btn.pack(side=tk.LEFT, padx=5)
        
        self.route_btn = ttk.Button(button_frame, text="🛣️ Построить маршрут", 
                                   command=self.calculate_route, width=20)
        self.route_btn.pack(side=tk.LEFT, padx=5)
        
        self.map_btn = ttk.Button(button_frame, text="🗺️ Карта для выбора точек", 
                                 command=self.open_selection_map, width=20)
        self.map_btn.pack(side=tk.LEFT, padx=5)
        
        self.show_btn = ttk.Button(button_frame, text="📊 Показать маршрут", 
                                  command=self.show_route_map, width=20)
        self.show_btn.pack(side=tk.LEFT, padx=5)
        
        # Progress bar
        ttk.Label(main_frame, text="Прогресс:", font=('Arial', 10)).grid(row=5, column=0, sticky=tk.W, pady=(20, 5))
        self.progress_bar = ttk.Progressbar(main_frame, mode='determinate', length=400)
        self.progress_bar.grid(row=5, column=1, sticky=(tk.W, tk.E), pady=(20, 5), padx=(10, 0))
        
        self.progress_label = ttk.Label(main_frame, text="Готов к работе", font=('Arial', 9), foreground='#666')
        self.progress_label.grid(row=6, column=1, sticky=tk.W, padx=(10, 0))
        
        # Results notebook
        notebook = ttk.Notebook(main_frame)
        notebook.grid(row=7, column=0, columnspan=2, sticky=(tk.W, tk.E, tk.N, tk.S), pady=20)
        
        # Log tab
        log_frame = ttk.Frame(notebook, padding=10)
        self.log_text = scrolledtext.ScrolledText(log_frame, width=80, height=12, font=('Consolas', 9))
        self.log_text.pack(fill=tk.BOTH, expand=True)
        notebook.add(log_frame, text="📋 Лог")
        
        # Results tab
        result_frame = ttk.Frame(notebook, padding=10)
        self.result_text = scrolledtext.ScrolledText(result_frame, width=80, height=12, font=('Consolas', 9))
        self.result_text.pack(fill=tk.BOTH, expand=True)
        notebook.add(result_frame, text="📊 Результаты")
        
        # Status bar
        self.status_var = tk.StringVar(value="Готов к работе")
        status_bar = ttk.Label(main_frame, textvariable=self.status_var, relief=tk.SUNKEN, anchor=tk.W)
        status_bar.grid(row=8, column=0, columnspan=2, sticky=(tk.W, tk.E), pady=(10, 0))
        
        self.log("=== Планировщик маршрутов дронов запущен ===")
        self.log("1. Введите название города (например: Berlin, Germany)")
        self.log("2. Нажмите 'Загрузить данные города'")
        self.log("3. Укажите координаты старта и финиша")
        self.log("4. Нажмите 'Построить маршрут'")
    
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
                self.log(f"[{stage.upper()}] {message} ({percentage}%)")
        
        self.root.after(0, update)
    
    def load_city_data(self):
        """Загрузка данных города"""
        city_name = self.city_entry.get().strip()
        if not city_name:
            messagebox.showwarning("Ошибка", "Введите название города")
            return
        
        def load_thread():
            try:
                self.load_btn.config(state='disabled')
                self.log(f"Загрузка данных для: {city_name}")
                
                # Загрузка данных
                city_data = self.data_service.get_city_data(city_name)
                
                # Построение графа
                city_graph = self.graph_service.build_city_graph(city_data)
                self.routing_service.city_graphs[city_name] = city_graph
                
                self.current_city = city_name
                stats = f"Узлов: {len(city_graph.nodes)}, Ребер: {len(city_graph.edges)}"
                self.log(f"✓ Данные загружены: {stats}")
                
                messagebox.showinfo("Успех", f"Данные города '{city_name}' загружены\n{stats}")
                
            except Exception as e:
                error_msg = str(e)
                self.log(f"✗ Ошибка загрузки: {error_msg}")
                messagebox.showerror("Ошибка", f"Не удалось загрузить данные: {error_msg}")
            finally:
                self.load_btn.config(state='normal')
        
        threading.Thread(target=load_thread, daemon=True).start()
    
    def calculate_route(self):
        """Расчет маршрута"""
        if not self.current_city:
            messagebox.showwarning("Ошибка", "Сначала загрузите данные города")
            return
        
        try:
            start_text = self.start_entry.get().strip()
            end_text = self.end_entry.get().strip()
            
            # Парсим координаты
            start_lat, start_lon = map(float, [x.strip() for x in start_text.split(',')])
            end_lat, end_lon = map(float, [x.strip() for x in end_text.split(',')])
            
            start = (start_lat, start_lon)
            end = (end_lat, end_lon)
            
            self.log(f"Расчет маршрута от {start} до {end}")
            
            def route_thread():
                try:
                    self.route_btn.config(state='disabled')
                    path, length, coords = self.routing_service.plan_route(
                        self.current_city, start, end
                    )
                    
                    if not path:
                        self.log("✗ Маршрут не найден")
                        messagebox.showwarning("Ошибка", "Не удалось построить маршрут")
                        return
                    
                    self.last_route = coords
                    
                    # Вывод результатов
                    self.result_text.delete(1.0, tk.END)
                    self.result_text.insert(tk.END, f"✅ МАРШРУТ ПОСТРОЕН\n")
                    self.result_text.insert(tk.END, f"Город: {self.current_city}\n")
                    self.result_text.insert(tk.END, f"Примерная длина: {length:.2f} км\n")
                    self.result_text.insert(tk.END, f"Точек маршрута: {len(coords)}\n\n")
                    self.result_text.insert(tk.END, "Координаты маршрута:\n")
                    
                    # Показываем первые и последние точки
                    for i, (lat, lon) in enumerate(coords[:5]):
                        self.result_text.insert(tk.END, f"{i+1:2d}. {lat:.6f}, {lon:.6f}\n")
                    
                    if len(coords) > 5:
                        self.result_text.insert(tk.END, "...\n")
                        for i, (lat, lon) in enumerate(coords[-3:], len(coords)-2):
                            self.result_text.insert(tk.END, f"{i+1:2d}. {lat:.6f}, {lon:.6f}\n")
                    
                    self.log("✓ Маршрут успешно построен!")
                    
                except Exception as e:
                    error_msg = str(e)
                    self.log(f"✗ Ошибка расчета: {error_msg}")
                    messagebox.showerror("Ошибка", f"Ошибка расчета маршрута: {error_msg}")
                finally:
                    self.route_btn.config(state='normal')
            
            threading.Thread(target=route_thread, daemon=True).start()
            
        except ValueError:
            self.log("✗ Ошибка формата координат")
            messagebox.showerror("Ошибка", "Проверьте формат координат. Пример: 52.521918, 13.413215")
        except Exception as e:
            error_msg = str(e)
            self.log(f"✗ Ошибка: {error_msg}")
            messagebox.showerror("Ошибка", f"Ошибка: {error_msg}")
    
    def open_selection_map(self):
        """Открытие карты для выбора точек"""
        city_name = self.city_entry.get().strip()
        if not city_name:
            messagebox.showwarning("Ошибка", "Введите название города")
            return
        
        def map_thread():
            try:
                self.log("Создание карты для выбора точек...")
                filename = self.mapping_service.create_clickable_map(city_name)
                if filename:
                    success = self.mapping_service.open_map(filename)
                    if success:
                        self.log("Карта для выбора точек открыта в браузере")
                    else:
                        self.log("Не удалось открыть карту")
                else:
                    self.log("Ошибка создания карты")
            except Exception as e:
                self.log(f"✗ Ошибка создания карты: {str(e)}")
        
        threading.Thread(target=map_thread, daemon=True).start()
    
    def show_route_map(self):
        """Показать маршрут на карте"""
        if not self.last_route:
            messagebox.showwarning("Ошибка", "Сначала постройте маршрут")
            return
        
        def map_thread():
            try:
                self.log("Создание карты маршрута...")
                filename = self.mapping_service.create_route_map(
                    self.current_city, self.last_route
                )
                if filename:
                    success = self.mapping_service.open_map(filename)
                    if success:
                        self.log("Карта маршрута открыта в браузере")
                    else:
                        self.log("Не удалось открыть карту маршрута")
                else:
                    self.log("Ошибка создания карты маршрута")
            except Exception as e:
                self.log(f"✗ Ошибка создания карты маршрута: {str(e)}")
        
        threading.Thread(target=map_thread, daemon=True).start()

if __name__ == "__main__":
    root = tk.Tk()
    app = ModernDroneRoutePlanner(root)
    root.mainloop()