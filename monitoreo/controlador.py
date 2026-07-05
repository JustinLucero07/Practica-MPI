from __future__ import annotations

import multiprocessing as mp
import queue
import threading
from dataclasses import dataclass
from time import perf_counter
from typing import Callable

from monitoreo.analizador import AnalizadorDatos
from monitoreo.config import CICLOS_DEFECTO, crear_estaciones
from monitoreo.dominio import (
    AlertaAmbiental,
    EstadoEstacion,
    Medicion,
    ModoEjecucion,
    SnapshotMonitoreo,
    VistaEstacion,
)
from monitoreo.estacion import EstacionAmbiental

Publicador = Callable[[SnapshotMonitoreo], None]


@dataclass(frozen=True)


class _FinCiclo:
    estacion_id: int
    ciclo: int


@dataclass(frozen=True)


class _FinEstacion:
    estacion_id: int


def _trabajo_estacion(
    estacion: EstacionAmbiental,
    ciclos: int,
    carga_cpu: int,
    cola: "mp.Queue",
    barrera: "mp.Barrier",
    stop: "mp.Event",
    semaforo_analisis: "mp.Semaphore",
) -> None:
    for ciclo in range(ciclos):
        if stop.is_set():
            break
        try:
            barrera.wait()
        except threading.BrokenBarrierError:
            break
        with semaforo_analisis:
            mediciones, _ = estacion.trabajar_ciclo(ciclo, carga_cpu)
        for m in mediciones:
            cola.put(m)
        cola.put(_FinCiclo(estacion.id, ciclo))
    cola.put(_FinEstacion(estacion.id))


class ControladorMonitoreo:

    def __init__(
        self,
        estaciones: list[EstacionAmbiental] | None = None,
        ciclos: int = CICLOS_DEFECTO,
        carga_cpu: int = AnalizadorDatos().carga,
        n_estaciones: int | None = None,
        max_analisis: int | None = None,
    ) -> None:
        if estaciones is not None:
            self.estaciones = estaciones
        elif n_estaciones is not None:
            self.estaciones = crear_estaciones(n_estaciones)
        else:
            self.estaciones = crear_estaciones()
        self.ciclos = ciclos
        self.analizador = AnalizadorDatos(carga_cpu)
        self.max_analisis = max_analisis if max_analisis is not None else mp.cpu_count()
        self._stop = threading.Event()
        self._reset()

    def _reset(self) -> None:
        self._lock = threading.Lock()
        self._buffer: list[Medicion] = []
        self._alertas: list[AlertaAmbiental] = []
        self._ultima: dict[str, Medicion] = {}
        self._estado_map: dict[int, EstadoEstacion] = {
            e.id: EstadoEstacion.ESPERANDO for e in self.estaciones
        }
        self._tiempos_ciclo: list[float] = []
        self._t0 = perf_counter()
        self._stop.clear()
        for e in self.estaciones:
            e.estado = EstadoEstacion.ESPERANDO

    def detener(self) -> None:
        self._stop.set()

    def _registrar(self, mediciones: list[Medicion]) -> None:
        with self._lock:
            self._buffer.extend(mediciones)
            for m in mediciones:
                self._ultima[m.estacion] = m
                if m.en_alerta:
                    self._alertas.append(AlertaAmbiental.desde_medicion(m))

    def _estado_de(self, est: EstacionAmbiental) -> EstadoEstacion:
        return self._estado_map.get(est.id, est.estado)

    def _snapshot(
        self, modo: ModoEjecucion, ciclo: int, en_ejecucion: bool
    ) -> SnapshotMonitoreo:
        with self._lock:
            buffer = list(self._buffer)
            alertas = list(self._alertas)
            ultima = dict(self._ultima)

        tiempo_total = perf_counter() - self._t0
        stats = self.analizador.estadisticas(
            buffer, alertas, self._tiempos_ciclo, tiempo_total
        )

        vistas: list[VistaEstacion] = []
        activas = finalizadas = 0
        for e in self.estaciones:
            estado = self._estado_de(e)
            if estado in (EstadoEstacion.ACTIVA, EstadoEstacion.PROCESANDO):
                activas += 1
            elif estado == EstadoEstacion.FINALIZADA:
                finalizadas += 1
            vistas.append(
                VistaEstacion(e.id, e.nombre, e.zona, estado, ultima.get(e.nombre))
            )

        stats = type(stats)(
            por_variable=stats.por_variable,
            mediciones_procesadas=stats.mediciones_procesadas,
            alertas_generadas=stats.alertas_generadas,
            zona_mayor_riesgo=stats.zona_mayor_riesgo,
            tiempo_total_s=stats.tiempo_total_s,
            tiempo_promedio_ciclo_s=stats.tiempo_promedio_ciclo_s,
            estaciones_totales=len(self.estaciones),
            estaciones_activas=activas,
            estaciones_finalizadas=finalizadas,
        )

        return SnapshotMonitoreo(
            estaciones=vistas,
            alertas=alertas[-15:],
            estadisticas=stats,
            modo=modo,
            ciclo_actual=ciclo,
            ciclos_totales=self.ciclos,
            tiempo_ejecucion_s=tiempo_total,
            en_ejecucion=en_ejecucion,
        )

    def _publicar(
        self,
        publicar: Publicador | None,
        modo: ModoEjecucion,
        ciclo: int,
        en_ejecucion: bool,
    ) -> SnapshotMonitoreo:
        snap = self._snapshot(modo, ciclo, en_ejecucion)
        if publicar is not None:
            publicar(snap)
        return snap

    def _marcar_finalizadas(self) -> None:
        for e in self.estaciones:
            e.estado = EstadoEstacion.FINALIZADA
            self._estado_map[e.id] = EstadoEstacion.FINALIZADA

    def ejecutar_secuencial(self, publicar: Publicador | None = None) -> SnapshotMonitoreo:
        self._reset()
        self._t0 = perf_counter()
        previo = self._t0
        for ciclo in range(self.ciclos):
            if self._stop.is_set():
                break
            for est in self.estaciones:
                self._estado_map[est.id] = EstadoEstacion.PROCESANDO
                mediciones, _ = est.trabajar_ciclo(ciclo, self.analizador.carga)
                self._registrar(mediciones)
                self._estado_map[est.id] = EstadoEstacion.ESPERANDO
            ahora = perf_counter()
            self._tiempos_ciclo.append(ahora - previo)
            previo = ahora
            self._publicar(publicar, ModoEjecucion.SECUENCIAL, ciclo + 1, True)
        self._marcar_finalizadas()
        return self._publicar(
            publicar, ModoEjecucion.SECUENCIAL, self._ciclo_completado(), False
        )

    def ejecutar_hilos(self, publicar: Publicador | None = None) -> SnapshotMonitoreo:
        self._reset()
        n = len(self.estaciones)
        barrera = threading.Barrier(n + 1)
        stop = self._stop

        def correr(est: EstacionAmbiental) -> None:
            for ciclo in range(self.ciclos):
                if stop.is_set():
                    break
                self._estado_map[est.id] = EstadoEstacion.PROCESANDO
                mediciones, _ = est.trabajar_ciclo(ciclo, self.analizador.carga)
                self._registrar(mediciones)
                self._estado_map[est.id] = EstadoEstacion.ESPERANDO
                try:
                    barrera.wait()
                except threading.BrokenBarrierError:
                    break

        hilos = [
            threading.Thread(target=correr, args=(e,), name=e.nombre, daemon=True)
            for e in self.estaciones
        ]
        self._t0 = perf_counter()
        previo = self._t0
        for h in hilos:
            h.start()

        for ciclo in range(self.ciclos):
            if stop.is_set():
                barrera.abort()
                break
            try:
                barrera.wait()
            except threading.BrokenBarrierError:
                break
            ahora = perf_counter()
            self._tiempos_ciclo.append(ahora - previo)
            previo = ahora
            self._publicar(publicar, ModoEjecucion.HILOS, ciclo + 1, True)

        for h in hilos:
            h.join()
        self._marcar_finalizadas()
        return self._publicar(
            publicar, ModoEjecucion.HILOS, self._ciclo_completado(), False
        )

    def ejecutar_procesos(self, publicar: Publicador | None = None) -> SnapshotMonitoreo:
        self._reset()
        n = len(self.estaciones)
        ctx = mp.get_context("fork")
        cola: mp.Queue = ctx.Queue()
        barrera = ctx.Barrier(n)
        stop_mp = ctx.Event()
        semaforo = ctx.Semaphore(max(1, min(self.max_analisis, n)))

        procesos = [
            ctx.Process(
                target=_trabajo_estacion,
                args=(
                    est, self.ciclos, self.analizador.carga,
                    cola, barrera, stop_mp, semaforo,
                ),
                name=est.nombre,
                daemon=True,
            )
            for est in self.estaciones
        ]
        self._t0 = perf_counter()
        previo = self._t0
        for p in procesos:
            p.start()
        for est in self.estaciones:
            self._estado_map[est.id] = EstadoEstacion.PROCESANDO

        finalizadas: set[int] = set()
        fines_por_ciclo: dict[int, int] = {}

        while len(finalizadas) < n:
            if self._stop.is_set():
                stop_mp.set()
            try:
                item = cola.get(timeout=0.1)
            except queue.Empty:
                self._publicar(
                    publicar, ModoEjecucion.PROCESOS, max(fines_por_ciclo, default=0), True
                )
                continue

            if isinstance(item, Medicion):
                self._registrar([item])
            elif isinstance(item, _FinCiclo):
                fines_por_ciclo[item.ciclo] = fines_por_ciclo.get(item.ciclo, 0) + 1
                if fines_por_ciclo[item.ciclo] == n:
                    ahora = perf_counter()
                    self._tiempos_ciclo.append(ahora - previo)
                    previo = ahora
                    self._publicar(
                        publicar, ModoEjecucion.PROCESOS, item.ciclo + 1, True
                    )
            elif isinstance(item, _FinEstacion):
                finalizadas.add(item.estacion_id)
                self._estado_map[item.estacion_id] = EstadoEstacion.FINALIZADA

        for p in procesos:
            p.join()
        self._marcar_finalizadas()
        return self._publicar(
            publicar, ModoEjecucion.PROCESOS, self._ciclo_completado(), False
        )

    def ejecutar(
        self, modo: ModoEjecucion, publicar: Publicador | None = None
    ) -> SnapshotMonitoreo:
        if modo == ModoEjecucion.SECUENCIAL:
            return self.ejecutar_secuencial(publicar)
        if modo == ModoEjecucion.HILOS:
            return self.ejecutar_hilos(publicar)
        if modo == ModoEjecucion.PROCESOS:
            return self.ejecutar_procesos(publicar)
        raise ValueError(f"Modo no soportado: {modo!r}")

    def _ciclo_completado(self) -> int:
        return len(self._tiempos_ciclo)
