from __future__ import annotations

from monitoreo.estacion import EstacionAmbiental
from monitoreo.dominio import Variable

CICLOS_DEFECTO = 12

ESTACIONES_DEFECTO = 6

_TODAS_VARIABLES: list[Variable] = [
    Variable.TEMPERATURA,
    Variable.HUMEDAD,
    Variable.RUIDO,
    Variable.CO2,
    Variable.PM25,
    Variable.PM10,
]

DEFINICION_ESTACIONES: list[tuple[str, str, list[Variable]]] = [
    (
        "Estación Centro",
        "El Centro Histórico",
        [Variable.TEMPERATURA, Variable.RUIDO, Variable.CO2, Variable.PM25, Variable.PM10],
    ),
    (
        "Estación Totoracocha",
        "Totoracocha",
        [Variable.TEMPERATURA, Variable.HUMEDAD, Variable.PM25, Variable.PM10],
    ),
    (
        "Estación Yanuncay",
        "Yanuncay",
        [Variable.TEMPERATURA, Variable.HUMEDAD, Variable.RUIDO, Variable.PM25],
    ),
    (
        "Estación El Vecino",
        "El Vecino",
        [Variable.TEMPERATURA, Variable.CO2, Variable.PM25, Variable.PM10],
    ),
    (
        "Estación Monay",
        "Monay",
        [Variable.TEMPERATURA, Variable.HUMEDAD, Variable.RUIDO, Variable.CO2],
    ),
    (
        "Estación Machángara",
        "Machángara (industrial)",
        [Variable.TEMPERATURA, Variable.RUIDO, Variable.CO2, Variable.PM25, Variable.PM10],
    ),
    (
        "Estación Baños",
        "Baños",
        [Variable.TEMPERATURA, Variable.HUMEDAD, Variable.RUIDO, Variable.PM25],
    ),
    (
        "Estación Ricaurte",
        "Ricaurte",
        [Variable.TEMPERATURA, Variable.HUMEDAD, Variable.CO2, Variable.PM10],
    ),
    (
        "Estación Sayausí",
        "Sayausí",
        [Variable.TEMPERATURA, Variable.HUMEDAD, Variable.RUIDO, Variable.PM25],
    ),
    (
        "Estación El Batán",
        "El Batán",
        [Variable.TEMPERATURA, Variable.RUIDO, Variable.CO2, Variable.PM25, Variable.PM10],
    ),
    (
        "Estación Tarqui",
        "Tarqui",
        [Variable.TEMPERATURA, Variable.HUMEDAD, Variable.RUIDO, Variable.CO2],
    ),
    (
        "Estación El Valle",
        "El Valle",
        [Variable.TEMPERATURA, Variable.HUMEDAD, Variable.CO2, Variable.PM25, Variable.PM10],
    ),
]


def _definicion_sintetica(indice: int) -> tuple[str, str, list[Variable]]:
    inicio = indice % len(_TODAS_VARIABLES)
    variables = [_TODAS_VARIABLES[(inicio + k) % len(_TODAS_VARIABLES)] for k in range(4)]
    return (f"Estación Zona {indice + 1}", f"Zona {indice + 1}", variables)


def crear_estaciones(n: int = ESTACIONES_DEFECTO) -> list[EstacionAmbiental]:
    n = max(1, n)
    definiciones: list[tuple[str, str, list[Variable]]] = []
    for i in range(n):
        if i < len(DEFINICION_ESTACIONES):
            definiciones.append(DEFINICION_ESTACIONES[i])
        else:
            definiciones.append(_definicion_sintetica(i))

    return [
        EstacionAmbiental(id=i + 1, nombre=nombre, zona=zona, variables=variables, semilla=100 + i)
        for i, (nombre, zona, variables) in enumerate(definiciones)
    ]
