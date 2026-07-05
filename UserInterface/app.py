from __future__ import annotations

import json
import os
import platform
import queue
import subprocess
import sys
import threading
import tkinter as tk
from tkinter import ttk

from monitoreo.controlador import ControladorMonitoreo
from monitoreo.dominio import ModoEjecucion, SnapshotMonitoreo

INTERVALO_MS = 120
HOSTFILE = "cluster/hosts.txt"
RED_MPI = "192.168.56.0/24"

COLOR_SEVERIDAD = {"Alta": "#c0392b", "Media": "#b9770e", "Baja": "#7f8c8d"}
BG = "#EEF3F7"
PANEL = "#FFFFFF"
TXT = "#243748"
ACENTO = "#1B4F72"
BORDE = "#D5E3EF"


def _snapshot_a_dict(snap: SnapshotMonitoreo) -> dict:
    estaciones = []
    for v in snap.estaciones:
        med = str(v.ultima_medicion) if v.ultima_medicion else "—"
        hora = v.ultima_medicion.instante.strftime("%H:%M:%S") if v.ultima_medicion else "—"
        estaciones.append({
            "id": v.id, "nombre": v.nombre, "zona": v.zona,
            "marca": v.estado.value, "medicion": med, "hora": hora,
        })
    e = snap.estadisticas
    return {
        "modo": snap.modo.value,
        "ciclo": snap.ciclo_actual, "ciclos": snap.ciclos_totales,
        "tiempo": snap.tiempo_ejecucion_s, "en_ejecucion": snap.en_ejecucion,
        "estaciones": estaciones,
        "mediciones": e.mediciones_procesadas,
        "alertas": e.alertas_generadas,
        "zona": e.zona_mayor_riesgo,
        "por_variable": [
            {"variable": var.etiqueta, "prom": ev.promedio, "min": ev.minimo, "max": ev.maximo}
            for var, ev in e.por_variable.items()
        ],
        "alertas_lista": [
            {"texto": f"{a.zona} — {a.mensaje}", "severidad": a.severidad}
            for a in snap.alertas
        ],
    }


class MonitoreoGUI(tk.Tk):
    def __init__(self, estaciones: int, ciclos: int, carga: int, procesos_mpi: int) -> None:
        super().__init__()
        self._carga = carga
        self._cola: "queue.Queue[dict]" = queue.Queue()
        self._hilo: threading.Thread | None = None
        self._controlador: ControladorMonitoreo | None = None
        self._proc: subprocess.Popen | None = None
        self._corriendo = False

        self.title("Sistema de Monitoreo Ambiental Urbano - Cuenca")
        self.geometry("1200x730")
        self.minsize(1040, 660)

        self._estilos()
        self._cabecera(estaciones, ciclos, procesos_mpi)
        self._cuerpo()
        self._pie()
        self._entorno_valores()
        self._roster(estaciones)

        self.protocol("WM_DELETE_WINDOW", self._cerrar)
        self.after(INTERVALO_MS, self._sondear)

    # -- Estilos --------------------------------------------------------
    def _estilos(self) -> None:
        st = ttk.Style(self)
        try:
            st.theme_use("clam")
        except tk.TclError:
            pass
        self.configure(bg=BG)
        st.configure("TFrame", background=BG)
        st.configure("TLabel", background=BG, foreground=TXT)
        st.configure("Titulo.TLabel", font=("TkDefaultFont", 16, "bold"), foreground=ACENTO)
        st.configure("Seccion.TLabelframe", background=PANEL, relief="flat", borderwidth=1, bordercolor=BORDE)
        st.configure("Seccion.TLabelframe.Label", font=("TkDefaultFont", 11, "bold"),
                     background=PANEL, foreground=ACENTO)
        st.configure("Stat.TLabel", font=("TkDefaultFont", 9), background=PANEL, foreground="#7C8A99")
        st.configure("StatVal.TLabel", font=("TkDefaultFont", 17, "bold"), background=PANEL, foreground=TXT)
        st.configure("Treeview", rowheight=27, background=PANEL, fieldbackground=PANEL, borderwidth=0)
        st.configure("Treeview.Heading", font=("TkDefaultFont", 10, "bold"),
                     background="#D5E3EF", foreground=TXT, relief="flat")

    # -- Cabecera -------------------------------------------------------
    def _cabecera(self, estaciones: int, ciclos: int, procesos_mpi: int) -> None:
        barra = ttk.Frame(self, padding=(12, 10))
        barra.pack(fill="x")
        ttk.Label(barra, text="Monitoreo Ambiental - Cuenca", style="Titulo.TLabel").pack(side="left")

        ttk.Label(barra, text="Modo:").pack(side="left", padx=(20, 4))
        self._modo = ttk.Combobox(barra, state="readonly", width=11,
                                  values=["Secuencial", "Hilos", "Procesos", "MPI"])
        self._modo.set("Secuencial")
        self._modo.pack(side="left")
        self._modo.bind("<<ComboboxSelected>>", lambda e: self._toggle_mpi())

        ttk.Label(barra, text="Estaciones:").pack(side="left", padx=(14, 4))
        self._sp_est = ttk.Spinbox(barra, from_=4, to=24, width=4)
        self._sp_est.set(str(estaciones))
        self._sp_est.pack(side="left")
        ttk.Label(barra, text="Ciclos:").pack(side="left", padx=(10, 4))
        self._sp_cic = ttk.Spinbox(barra, from_=10, to=40, width=4)
        self._sp_cic.set(str(ciclos))
        self._sp_cic.pack(side="left")

        self._lbl_np = ttk.Label(barra, text="Procesos MPI:")
        self._sp_np = ttk.Spinbox(barra, from_=1, to=12, width=4)
        self._sp_np.set(str(procesos_mpi))

        self._btn = ttk.Button(barra, text="Iniciar", command=self._iniciar)
        self._btn.pack(side="right")
        self._lbl_motor = ttk.Label(barra, text="Detenido", foreground="#9AA7B4")
        self._lbl_motor.pack(side="right", padx=10)

        ttk.Separator(self, orient="horizontal").pack(fill="x")
        info = ttk.Frame(self, padding=(12, 6))
        info.pack(fill="x")
        self._lbl_modo = ttk.Label(info, text="Modo activo: —")
        self._lbl_modo.pack(side="left")
        self._lbl_ciclo = ttk.Label(info, text="Ciclo: 0 / 0")
        self._lbl_ciclo.pack(side="left", padx=24)
        self._lbl_tiempo = ttk.Label(info, text="Tiempo: 0.000 s")
        self._lbl_tiempo.pack(side="left")
        ttk.Separator(self, orient="horizontal").pack(fill="x")
        self._toggle_mpi()

    def _toggle_mpi(self) -> None:
        if self._modo.get() == "MPI":
            self._lbl_np.pack(side="left", padx=(14, 4))
            self._sp_np.pack(side="left")
        else:
            self._lbl_np.pack_forget()
            self._sp_np.pack_forget()

    # -- Cuerpo ---------------------------------------------------------
    def _cuerpo(self) -> None:
        cuerpo = ttk.Frame(self, padding=12)
        cuerpo.pack(fill="both", expand=True)

        izq = ttk.Labelframe(cuerpo, text="Estaciones ambientales", padding=8, style="Seccion.TLabelframe")
        izq.pack(side="left", fill="both", expand=True)
        cols = ("id", "nombre", "zona", "marca", "medicion", "hora")
        self._tabla = ttk.Treeview(izq, columns=cols, show="headings")
        for c, t, w, a in (("id", "ID", 36, "center"), ("nombre", "Estación", 150, "w"),
                           ("zona", "Zona", 150, "w"), ("marca", "Estado / Proc", 105, "center"),
                           ("medicion", "Última medición", 175, "w"), ("hora", "Hora", 78, "center")):
            self._tabla.heading(c, text=t)
            self._tabla.column(c, width=w, anchor=a)
        sc = ttk.Scrollbar(izq, orient="vertical", command=self._tabla.yview)
        self._tabla.configure(yscrollcommand=sc.set)
        self._tabla.pack(side="left", fill="both", expand=True)
        sc.pack(side="right", fill="y")

        der = ttk.Frame(cuerpo)
        der.pack(side="right", fill="both", padx=(12, 0))
        self._panel_stats(der)
        self._panel_variables(der)
        self._panel_alertas(der)
        self._panel_entorno(der)

    def _panel_stats(self, padre) -> None:
        caja = ttk.Labelframe(padre, text="Estadísticas generales", padding=8, style="Seccion.TLabelframe")
        caja.pack(fill="x")
        self._sv: dict[str, tk.StringVar] = {}
        items = [("Estaciones", "estaciones"), ("Mediciones", "mediciones"),
                 ("Alertas", "alertas"), ("Ciclo", "ciclo"),
                 ("Tiempo (s)", "tiempo"), ("Datos/tarea", "reparto")]
        grid = ttk.Frame(caja)
        grid.pack(fill="x", expand=True, pady=5)
        for c in range(3):
            grid.columnconfigure(c, weight=1)
        for i, (tit, k) in enumerate(items):
            v = tk.StringVar(value="—")
            self._sv[k] = v
            cel = ttk.Frame(grid, padding=5)
            cel.grid(row=i // 3, column=i % 3, sticky="nsew", padx=5, pady=5)
            ttk.Label(cel, textvariable=v, style="StatVal.TLabel", anchor="center").pack(fill="x")
            ttk.Label(cel, text=tit, style="Stat.TLabel", anchor="center").pack(fill="x")
        fila = ttk.Frame(caja)
        fila.pack(fill="x", pady=(8, 0))
        ttk.Label(fila, text="Zona de mayor riesgo:").pack(side="left")
        self._lbl_zona = ttk.Label(fila, text="—", font=("TkDefaultFont", 10, "bold"), foreground="#c0392b")
        self._lbl_zona.pack(side="left", padx=6)

    def _panel_variables(self, padre) -> None:
        caja = ttk.Labelframe(padre, text="Por variable (prom / mín / máx)", padding=8, style="Seccion.TLabelframe")
        caja.pack(fill="x", pady=(10, 0))
        cols = ("v", "p", "mn", "mx")
        self._tv = ttk.Treeview(caja, columns=cols, show="headings", height=6)
        for c, t, w in (("v", "Variable", 110), ("p", "Prom.", 70), ("mn", "Mín.", 60), ("mx", "Máx.", 60)):
            self._tv.heading(c, text=t)
            self._tv.column(c, width=w, anchor="center")
        self._tv.pack(fill="x")

    def _panel_alertas(self, padre) -> None:
        caja = ttk.Labelframe(padre, text="Alertas activas", padding=8, style="Seccion.TLabelframe")
        caja.pack(fill="both", expand=True, pady=(10, 0))
        self._txt = tk.Text(caja, height=6, wrap="word", highlightthickness=0, borderwidth=0,
                            font=("TkDefaultFont", 9), background=PANEL, foreground=TXT, state="disabled")
        sc = ttk.Scrollbar(caja, orient="vertical", command=self._txt.yview)
        self._txt.configure(yscrollcommand=sc.set)
        self._txt.pack(side="left", fill="both", expand=True)
        sc.pack(side="right", fill="y")
        for sev, col in COLOR_SEVERIDAD.items():
            self._txt.tag_config(sev, foreground=col)
        self._txt.tag_config("nada", foreground="#AAB4BD")

    def _panel_entorno(self, padre) -> None:
        self._entorno = ttk.Labelframe(padre, text="Entorno de ejecución", padding=8, style="Seccion.TLabelframe")
        self._entorno.pack(fill="x", pady=(10, 0))

    def _pie(self) -> None:
        ttk.Separator(self, orient="horizontal").pack(fill="x")
        pie = ttk.Frame(self, padding=(12, 6))
        pie.pack(fill="x")
        self._lbl_pie = ttk.Label(pie, text="Listo. Elige un modo y pulsa Iniciar.", foreground="#7C8A99")
        self._lbl_pie.pack(side="left")

    def _gil(self) -> str:
        f = getattr(sys, "_is_gil_enabled", None)
        if callable(f):
            return "Desactivado (free-threaded)" if not f() else "Activo"
        return "Activo"

    def _entorno_valores(self) -> None:
        datos = {
            "Versión de Python": platform.python_version(),
            "Sistema operativo": f"{platform.system()} {platform.release()}",
            "Núcleos de CPU": str(os.cpu_count()),
            "Estado del GIL": self._gil(),
        }
        for k, val in datos.items():
            fila = ttk.Frame(self._entorno)
            fila.pack(fill="x", pady=1)
            ttk.Label(fila, text=f"{k}:").pack(side="left")
            ttk.Label(fila, text=val, font=("TkDefaultFont", 9, "bold")).pack(side="right")

    def _roster(self, n: int) -> None:
        from monitoreo.config import crear_estaciones
        self._tabla.delete(*self._tabla.get_children())
        for e in crear_estaciones(n):
            self._tabla.insert("", "end", iid=e.nombre,
                               values=(e.id, e.nombre, e.zona, "Esperando", "—", "—"))
        self._sv["estaciones"].set(str(n))

    # -- Control --------------------------------------------------------
    def _iniciar(self) -> None:
        if self._corriendo:
            return
        modo = self._modo.get()
        n_est = max(4, int(self._sp_est.get()))
        ciclos = max(10, int(self._sp_cic.get()))
        self._roster(n_est)
        self._corriendo = True
        self._btn.config(state="disabled")
        for w in (self._modo, self._sp_est, self._sp_cic, self._sp_np):
            w.config(state="disabled")
        self._lbl_motor.config(text="En ejecución", foreground="#1b7f3b")
        self._lbl_modo.config(text=f"Modo activo: {modo}")
        self._lbl_pie.config(text=f"Ejecutando en modo {modo}…")

        if modo == "MPI":
            np_mpi = max(1, int(self._sp_np.get()))
            self._hilo = threading.Thread(target=self._trabajo_mpi, args=(n_est, ciclos, np_mpi), daemon=True)
        else:
            self._controlador = ControladorMonitoreo(ciclos=ciclos, carga_cpu=self._carga, n_estaciones=n_est)
            self._hilo = threading.Thread(target=self._trabajo_local, args=(modo,), daemon=True)
        self._hilo.start()

    def _trabajo_local(self, modo: str) -> None:
        self._controlador.ejecutar(ModoEjecucion(modo), publicar=lambda s: self._cola.put(_snapshot_a_dict(s)))

    def _trabajo_mpi(self, n_est: int, ciclos: int, np_mpi: int) -> None:
        cmd = ["mpiexec", "-n", str(np_mpi), "--wdir", os.getcwd()]
        if os.path.exists(HOSTFILE):
            cmd += ["-hostfile", HOSTFILE, "--mca", "btl_tcp_if_include", RED_MPI]
        cmd += [sys.executable, "-m", "nucleo.mpi_runner",
                "--estaciones", str(n_est), "--ciclos", str(ciclos), "--carga", str(self._carga)]
        try:
            self._proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        except Exception as ex:
            self._cola.put({"error": str(ex)})
            return
        for linea in self._proc.stdout:
            linea = linea.strip()
            if linea.startswith("{"):
                try:
                    self._cola.put(json.loads(linea))
                except json.JSONDecodeError:
                    pass
        self._proc.wait()

    def _detener_trabajo(self) -> None:
        if self._controlador is not None:
            self._controlador.detener()
        if self._proc is not None and self._proc.poll() is None:
            self._proc.terminate()

    def _cerrar(self) -> None:
        self._detener_trabajo()
        self.destroy()

    # -- Sondeo (hilo principal) ---------------------------------------
    def _sondear(self) -> None:
        try:
            while True:
                d = self._cola.get_nowait()
                if d.get("error"):
                    self._lbl_pie.config(text=f"Error al lanzar MPI: {d['error']}")
                    continue
                if d.get("fin"):
                    self._sv["reparto"].set(str(d.get("reparto", "—")))
                    self._lbl_pie.config(text=f"MPI completado en {d.get('tiempo', 0):.3f} s · reparto {d.get('reparto')}")
                    continue
                self._aplicar(d)
        except queue.Empty:
            pass

        if self._corriendo and self._hilo is not None and not self._hilo.is_alive():
            self._corriendo = False
            self._lbl_motor.config(text="Detenido", foreground="#9AA7B4")
            self._btn.config(state="normal")
            self._modo.config(state="readonly")
            for w in (self._sp_est, self._sp_cic, self._sp_np):
                w.config(state="normal")
        self.after(INTERVALO_MS, self._sondear)

    def _aplicar(self, d: dict) -> None:
        self._lbl_modo.config(text=f"Modo activo: {d['modo']}")
        self._lbl_ciclo.config(text=f"Ciclo: {d['ciclo']} / {d['ciclos']}")
        self._lbl_tiempo.config(text=f"Tiempo: {d['tiempo']:.3f} s")
        for est in d["estaciones"]:
            nombre = est["nombre"]
            if self._tabla.exists(nombre):
                self._tabla.item(nombre, values=(est["id"], nombre, est["zona"],
                                                  est["marca"], est["medicion"], est["hora"]))
        self._sv["mediciones"].set(str(d["mediciones"]))
        self._sv["alertas"].set(str(d["alertas"]))
        self._sv["ciclo"].set(str(d["ciclo"]))
        self._sv["tiempo"].set(f"{d['tiempo']:.2f}")
        self._lbl_zona.config(text=d["zona"])
        self._tv.delete(*self._tv.get_children())
        for v in d["por_variable"]:
            self._tv.insert("", "end", values=(v["variable"], f"{v['prom']:.1f}", f"{v['min']:.1f}", f"{v['max']:.1f}"))
        self._txt.config(state="normal")
        self._txt.delete(1.0, "end")
        if not d["alertas_lista"]:
            self._txt.insert("end", "Sin alertas activas.\n", "nada")
        else:
            for a in reversed(d["alertas_lista"]):
                self._txt.insert("end", f"{a['texto']} ({a['severidad']})\n", a["severidad"])
        self._txt.config(state="disabled")


def lanzar(estaciones: int = 8, ciclos: int = 20, carga: int = 600, procesos_mpi: int = 4) -> None:
    app = MonitoreoGUI(estaciones, ciclos, carga, procesos_mpi)
    app.mainloop()
