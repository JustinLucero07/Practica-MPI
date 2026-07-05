from __future__ import annotations

from nucleo.dominio import Variable
from nucleo.estacion import EstacionAmbiental

_TODAS = [
    Variable.TEMPERATURA, Variable.HUMEDAD, Variable.RUIDO,
    Variable.CO2, Variable.PM25, Variable.PM10,
]

DEFINICION_ESTACIONES: list[tuple[str, str, list[Variable]]] = [
    ("Estacion Centro", "El Centro Historico",
     [Variable.TEMPERATURA, Variable.RUIDO, Variable.CO2, Variable.PM25, Variable.PM10]),
    ("Estacion Totoracocha", "Totoracocha",
     [Variable.TEMPERATURA, Variable.HUMEDAD, Variable.PM25, Variable.PM10]),
    ("Estacion Yanuncay", "Yanuncay",
     [Variable.TEMPERATURA, Variable.HUMEDAD, Variable.RUIDO, Variable.PM25]),
    ("Estacion El Vecino", "El Vecino",
     [Variable.TEMPERATURA, Variable.CO2, Variable.PM25, Variable.PM10]),
    ("Estacion Monay", "Monay",
     [Variable.TEMPERATURA, Variable.HUMEDAD, Variable.RUIDO, Variable.CO2]),
    ("Estacion Machangara", "Machangara (industrial)",
     [Variable.TEMPERATURA, Variable.RUIDO, Variable.CO2, Variable.PM25, Variable.PM10]),
    ("Estacion Banos", "Banos",
     [Variable.TEMPERATURA, Variable.HUMEDAD, Variable.RUIDO, Variable.PM25]),
    ("Estacion Ricaurte", "Ricaurte",
     [Variable.TEMPERATURA, Variable.HUMEDAD, Variable.CO2, Variable.PM10]),
    ("Estacion Sayausi", "Sayausi",
     [Variable.TEMPERATURA, Variable.HUMEDAD, Variable.RUIDO, Variable.PM25]),
    ("Estacion El Batan", "El Batan",
     [Variable.TEMPERATURA, Variable.RUIDO, Variable.CO2, Variable.PM25, Variable.PM10]),
    ("Estacion Tarqui", "Tarqui",
     [Variable.TEMPERATURA, Variable.HUMEDAD, Variable.RUIDO, Variable.CO2]),
    ("Estacion El Valle", "El Valle",
     [Variable.TEMPERATURA, Variable.HUMEDAD, Variable.CO2, Variable.PM25, Variable.PM10]),
]


def _definicion(indice: int) -> tuple[str, str, list[Variable]]:
    if indice < len(DEFINICION_ESTACIONES):
        return DEFINICION_ESTACIONES[indice]
    inicio = indice % len(_TODAS)
    variables = [_TODAS[(inicio + k) % len(_TODAS)] for k in range(4)]
    return (f"Estacion Zona {indice + 1}", f"Zona {indice + 1}", variables)


def crear_estacion(indice: int) -> EstacionAmbiental:
    nombre, zona, variables = _definicion(indice)
    return EstacionAmbiental(
        id=indice + 1, nombre=nombre, zona=zona,
        variables=variables, semilla=100 + indice,
    )
