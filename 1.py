import os
import osmnx as ox
import networkx as nx
import tkinter as tk
from tkinter import messagebox, scrolledtext
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
import folium
import webbrowser
from pyproj import Transformer

# ---------- Кэш ----------
CACHE_DIR = "cache_graphs"
os.makedirs(CACHE_DIR, exist_ok=True)


def normalize_crs_input(crs):
    if crs is None:
        return "EPSG:3857"
    if isinstance(crs, dict):
        if "init" in crs:
            return crs["init"].upper()
        if "epsg" in crs:
            return f"EPSG:{crs['epsg']}"
        return "EPSG:3857"
    return crs


def load_city_graph(city_name: str):
    filename = os.path.join(CACHE_DIR, city_name.replace(", ", "_").replace(" ", "_") + ".graphml")

    if os.path.exists(filename):
        print(f"[cache] Загружаем граф {city_name}")
        G = ox.load_graphml(filename)
    else:
        print(f"[download] Скачиваем граф {city_name}")
        G = ox.graph_from_place(city_name, network_type="drive")
        G = ox.project_graph(G)
        ox.save_graphml(G, filename)
    G = ox.add_edge_speeds(G)
    G = ox.add_edge_travel_times(G)
    return G


def get_transformers_for_graph(G):
    graph_crs = normalize_crs_input(G.graph.get("crs", None))
    t_to_graph = Transformer.from_crs("EPSG:4326", graph_crs, always_xy=True)
    t_to_wgs84 = Transformer.from_crs(graph_crs, "EPSG:4326", always_xy=True)
    return t_to_graph, t_to_wgs84


def latlon_to_graph_xy(transformer_to_graph, lat, lon):
    x, y = transformer_to_graph.transform(lon, lat)
    return x, y


def graph_xy_to_latlon(transformer_to_wgs84, x, y):
    lon, lat = transformer_to_wgs84.transform(x, y)
    return lat, lon


def battery_cost(distance_m: float, altitude_m: float = 20) -> float:
    return distance_m + 0.1 * altitude_m


def plan_route(G, start_point, end_point, battery, stations=[]):
    t_to_graph, t_to_wgs84 = get_transformers_for_graph(G)

    sx, sy = latlon_to_graph_xy(t_to_graph, start_point[0], start_point[1])
    ex, ey = latlon_to_graph_xy(t_to_graph, end_point[0], end_point[1])

    orig = ox.distance.nearest_nodes(G, X=sx, Y=sy)
    dest = ox.distance.nearest_nodes(G, X=ex, Y=ey)

    if orig == dest:
        return [orig], 0.0, [start_point]

    try:
        route = nx.shortest_path(G, orig, dest, weight="length")
        route_length = nx.path_weight(G, route, weight="length")
    except nx.NetworkXNoPath:
        return None, None, None

    if battery_cost(route_length) <= battery:
        coords = []
        for n in route:
            nx_, ny_ = G.nodes[n].get("x"), G.nodes[n].get("y")
            lat, lon = graph_xy_to_latlon(t_to_wgs84, nx_, ny_)
            coords.append((lat, lon))
        return route, route_length, coords

    return None, None, None


# ---------- Folium карта ----------
def save_folium_map(coords, filename="route_map.html"):
    if not coords:
        return None

    m = folium.Map(location=coords[0], zoom_start=14, tiles="CartoDB positron")

    folium.PolyLine(coords, color="red", weight=5, opacity=0.8).add_to(m)
    folium.Marker(coords[0], tooltip="Старт", icon=folium.Icon(color="green")).add_to(m)
    folium.Marker(coords[-1], tooltip="Финиш", icon=folium.Icon(color="red")).add_to(m)

    m.save(filename)
    return filename


# ---------- GUI ----------
class DroneApp:
    def __init__(self, root):
        self.root = root
        self.root.title("Drone Route Planner")
        self.G = None
        self.last_coords = None

        tk.Label(root, text="Город:").grid(row=0, column=0, sticky="w")
        self.city_entry = tk.Entry(root, width=40)
        self.city_entry.insert(0, "Berlin, Germany")
        self.city_entry.grid(row=0, column=1, sticky="w")

        tk.Label(root, text="Старт (lat, lon):").grid(row=1, column=0, sticky="w")
        self.start_entry = tk.Entry(root, width=40)
        self.start_entry.insert(0, "52.521918, 13.413215")
        self.start_entry.grid(row=1, column=1, sticky="w")

        tk.Label(root, text="Финиш (lat, lon):").grid(row=2, column=0, sticky="w")
        self.goal_entry = tk.Entry(root, width=40)
        self.goal_entry.insert(0, "52.516275, 13.377704")
        self.goal_entry.grid(row=2, column=1, sticky="w")

        tk.Label(root, text="Батарея (ед.):").grid(row=3, column=0, sticky="w")
        self.battery_entry = tk.Entry(root, width=40)
        self.battery_entry.insert(0, "5000")
        self.battery_entry.grid(row=3, column=1, sticky="w")

        self.btn = tk.Button(root, text="Загрузить карту", command=self.load_graph)
        self.btn.grid(row=4, column=0, pady=5)

        self.run_btn = tk.Button(root, text="Построить маршрут", command=self.run)
        self.run_btn.grid(row=4, column=1, pady=5)

        self.map_btn = tk.Button(root, text="Открыть интерактивную карту", command=self.open_map)
        self.map_btn.grid(row=5, column=0, columnspan=2, pady=5)

        self.coords_text = scrolledtext.ScrolledText(root, width=60, height=12)
        self.coords_text.grid(row=6, column=0, columnspan=2)

        self.canvas = None

    def load_graph(self):
        city = self.city_entry.get().strip()
        if not city:
            messagebox.showwarning("Ошибка", "Введите название города")
            return
        try:
            self.G = load_city_graph(city)
            messagebox.showinfo("ОК", f"Граф города '{city}' загружен")
        except Exception as e:
            messagebox.showerror("Ошибка загрузки", str(e))

    def run(self):
        if self.G is None:
            try:
                self.G = load_city_graph(self.city_entry.get().strip())
            except Exception as e:
                messagebox.showerror("Ошибка", str(e))
                return

        try:
            start = tuple(map(float, self.start_entry.get().split(",")))
            goal = tuple(map(float, self.goal_entry.get().split(",")))
            battery = float(self.battery_entry.get())
        except Exception as e:
            messagebox.showerror("Ошибка входных данных", str(e))
            return

        route, length, coords = plan_route(self.G, start, goal, battery)
        self.coords_text.delete("1.0", tk.END)

        if route is None:
            messagebox.showwarning("Нет маршрута", "Не удалось построить маршрут.")
            return

        self.last_coords = coords
        self.coords_text.insert(tk.END, f"Длина маршрута: {length:.2f} м\n\n")
        for lat, lon in coords:
            self.coords_text.insert(tk.END, f"{lat:.6f}, {lon:.6f}\n")

        try:
            fig, ax = ox.plot_graph_route(
                self.G, route, route_linewidth=3, node_size=0,
                bgcolor="white", show=False, close=False
            )
            if self.canvas:
                self.canvas.get_tk_widget().destroy()
            self.canvas = FigureCanvasTkAgg(fig, master=self.root)
            self.canvas.get_tk_widget().grid(row=7, column=0, columnspan=2)
            self.canvas.draw()
        except Exception as e:
            self.coords_text.insert(tk.END, f"\nОшибка отрисовки: {e}")

    def open_map(self):
        if not self.last_coords:
            messagebox.showwarning("Нет маршрута", "Сначала построй маршрут.")
            return
        filename = save_folium_map(self.last_coords)
        if filename:
            webbrowser.open(f"file://{os.path.abspath(filename)}")


if __name__ == "__main__":
    root = tk.Tk()
    app = DroneApp(root)
    root.mainloop()
