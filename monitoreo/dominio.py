from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum


class Variable(Enum):

    TEMPERATURA = ("Temperatura", "°C", 26.0)
    HUMEDAD = ("Humedad", "%", 90.0)
    RUIDO = ("Ruido", "dB", 75.0)
    CO2 = ("CO₂", "ppm", 1000.0)
    PM25 = ("PM2.5", "µg/m³", 50.0)
    PM10 = ("PM10", "µg/m³", 100.0)

    def __init__(self, etiqueta: str, unidad: str, umbral: float) -> None:
        self.etiqueta = etiqueta
        self.unidad = unidad
        self.umbral = umbral

    def __str__(self) -> str:
        return self.etiqueta


class EstadoEstacion(str, Enum):

    ACTIVA = "Activa"
    ESPERANDO = "Esperando"
    PROCESANDO = "Procesando"
    FINALIZADA = "Finalizada"


class ModoEjecucion(str, Enum):

    SECUENCIAL = "Secuencial"
    HILOS = "Hilos"
    PROCESOS = "Procesos"


@dataclass(frozen=True)


class Medicion:

    estacion: str
    zona: str
    variable: Variable
    valor: float
    instante: datetime = field(default_factory=datetime.now)

    @property
    def unidad(self) -> str:
        return self.variable.unidad

    @property
    def en_alerta(self) -> bool:
        return self.valor > self.variable.umbral

    def __str__(self) -> str:
        return f"{self.variable.etiqueta}: {self.valor:.1f} {self.variable.unidad}"


@dataclass(frozen=True)


class AlertaAmbiental:

    estacion: str
    zona: str
    variable: Variable
    valor: float
    umbral: float
    instante: datetime = field(default_factory=datetime.now)

    @property
    def severidad(self) -> str:
        razon = self.valor / self.umbral if self.umbral else 0.0
        if razon >= 1.5:
            return "Alta"
        if razon >= 1.2:
            return "Media"
        return "Baja"

    @property
    def mensaje(self) -> str:
        return (
            f"{self.variable.etiqueta} = {self.valor:.1f} {self.variable.unidad} "
            f"(umbral {self.umbral:.0f})"
        )

    @classmethod
    def desde_medicion(cls, m: Medicion) -> "AlertaAmbiental":
        return cls(
            estacion=m.estacion,
            zona=m.zona,
            variable=m.variable,
            valor=m.valor,
            umbral=m.variable.umbral,
            instante=m.instante,
        )


@dataclass(frozen=True)


class EstadisticaVariable:

    variable: Variable
    n: int
    promedio: float
    minimo: float
    maximo: float


@dataclass(frozen=True)


class Estadisticas:

    por_variable: dict[Variable, EstadisticaVariable] = field(default_factory=dict)
    mediciones_procesadas: int = 0
    alertas_generadas: int = 0
    zona_mayor_riesgo: str = "—"
    tiempo_total_s: float = 0.0
    tiempo_promedio_ciclo_s: float = 0.0
    estaciones_totales: int = 0
    estaciones_activas: int = 0
    estaciones_finalizadas: int = 0


@dataclass(frozen=True)


class VistaEstacion:

    id: int
    nombre: str
    zona: str
    estado: EstadoEstacion
    ultima_medicion: Medicion | None = None


@dataclass(frozen=True)


class SnapshotMonitoreo:

    estaciones: list[VistaEstacion] = field(default_factory=list)
    alertas: list[AlertaAmbiental] = field(default_factory=list)
    estadisticas: Estadisticas = field(default_factory=Estadisticas)
    modo: ModoEjecucion = ModoEjecucion.SECUENCIAL
    ciclo_actual: int = 0
    ciclos_totales: int = 0
    tiempo_ejecucion_s: float = 0.0
    en_ejecucion: bool = False
