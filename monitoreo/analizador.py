from __future__ import annotations

import math
from collections import Counter

from monitoreo.dominio import (
    AlertaAmbiental,
    EstadisticaVariable,
    Estadisticas,
    Medicion,
    Variable,
)

CARGA_CPU_DEFECTO = 600


class AnalizadorDatos:

    def __init__(self, carga_cpu: int = CARGA_CPU_DEFECTO) -> None:
        self.carga = carga_cpu

    @staticmethod
    def indice_ambiental(valores: list[float], repeticiones: int) -> float:
        if not valores:
            return 0.0
        n = len(valores)
        acumulado = 0.0
        for _ in range(repeticiones):
            parcial = 0.0
            for i in range(n):
                inicio = i - 5 if i >= 5 else 0
                ventana = valores[inicio : i + 1]
                media_movil = sum(ventana) / len(ventana)
                v = valores[i]
                parcial += math.sqrt(abs(media_movil) + 1.0) * math.sin(v) \
                    + math.log1p(abs(v))
            acumulado += parcial
        return acumulado / repeticiones

    def estadisticas(
        self,
        mediciones: list[Medicion],
        alertas: list[AlertaAmbiental],
        tiempos_ciclo: list[float],
        tiempo_total: float,
    ) -> Estadisticas:
        por_variable: dict[Variable, EstadisticaVariable] = {}
        valores_por_var: dict[Variable, list[float]] = {}
        for m in mediciones:
            valores_por_var.setdefault(m.variable, []).append(m.valor)

        for var, valores in valores_por_var.items():
            por_variable[var] = EstadisticaVariable(
                variable=var,
                n=len(valores),
                promedio=sum(valores) / len(valores),
                minimo=min(valores),
                maximo=max(valores),
            )

        zona_riesgo = "—"
        if alertas:
            conteo = Counter(a.zona for a in alertas)
            zona_riesgo = conteo.most_common(1)[0][0]

        promedio_ciclo = (
            sum(tiempos_ciclo) / len(tiempos_ciclo) if tiempos_ciclo else 0.0
        )

        return Estadisticas(
            por_variable=por_variable,
            mediciones_procesadas=len(mediciones),
            alertas_generadas=len(alertas),
            zona_mayor_riesgo=zona_riesgo,
            tiempo_total_s=tiempo_total,
            tiempo_promedio_ciclo_s=promedio_ciclo,
        )
