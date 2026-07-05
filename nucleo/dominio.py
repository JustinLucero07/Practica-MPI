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


@dataclass(frozen=True)
class Medicion:
    estacion: str
    zona: str
    variable: Variable
    valor: float
    ciclo: int = 0
    proceso: int = 0
    instante: datetime = field(default_factory=datetime.now)

    @property
    def unidad(self) -> str:
        return self.variable.unidad

    @property
    def en_alerta(self) -> bool:
        return self.valor > self.variable.umbral

    def __str__(self) -> str:
        return (
            f"[P{self.proceso} c{self.ciclo}] {self.estacion} ({self.zona}) "
            f"{self.variable.etiqueta}: {self.valor:.1f} {self.variable.unidad}"
        )


@dataclass(frozen=True)
class AlertaAmbiental:
    estacion: str
    zona: str
    variable: Variable
    valor: float
    umbral: float
    ciclo: int
    proceso: int

    @property
    def severidad(self) -> str:
        razon = self.valor / self.umbral if self.umbral else 0.0
        if razon >= 1.5:
            return "Alta"
        if razon >= 1.2:
            return "Media"
        return "Baja"

    @classmethod
    def desde_medicion(cls, m: Medicion) -> "AlertaAmbiental":
        return cls(
            estacion=m.estacion,
            zona=m.zona,
            variable=m.variable,
            valor=m.valor,
            umbral=m.variable.umbral,
            ciclo=m.ciclo,
            proceso=m.proceso,
        )
