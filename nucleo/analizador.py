from __future__ import annotations

import math

from nucleo.dominio import AlertaAmbiental, Medicion

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

    @staticmethod
    def resumen_local(mediciones: list[Medicion], proceso: int) -> dict:
        por_variable: dict[str, dict] = {}
        zonas_alerta: dict[str, int] = {}
        alertas: list[dict] = []
        ultimas: dict[str, dict] = {}

        for m in mediciones:
            ultimas[m.estacion] = {
                "texto": f"{m.variable.etiqueta}: {m.valor:.1f} {m.variable.unidad}",
                "proceso": m.proceso, "ciclo": m.ciclo,
                "hora": m.instante.strftime("%H:%M:%S"),
            }
            clave = m.variable.etiqueta
            d = por_variable.get(clave)
            if d is None:
                por_variable[clave] = {
                    "n": 1, "suma": m.valor, "min": m.valor, "max": m.valor,
                }
            else:
                d["n"] += 1
                d["suma"] += m.valor
                d["min"] = min(d["min"], m.valor)
                d["max"] = max(d["max"], m.valor)

            if m.en_alerta:
                a = AlertaAmbiental.desde_medicion(m)
                zonas_alerta[a.zona] = zonas_alerta.get(a.zona, 0) + 1
                alertas.append({
                    "zona": a.zona, "estacion": a.estacion,
                    "variable": a.variable.etiqueta, "valor": a.valor,
                    "umbral": a.umbral, "severidad": a.severidad,
                    "ciclo": a.ciclo, "proceso": a.proceso,
                })

        return {
            "proceso": proceso,
            "n": len(mediciones),
            "n_alertas": len(alertas),
            "por_variable": por_variable,
            "zonas_alerta": zonas_alerta,
            "alertas": alertas,
            "ultimas": ultimas,
        }

    @staticmethod
    def consolidar(resumenes: list[dict]) -> dict:
        por_variable: dict[str, dict] = {}
        zonas_alerta: dict[str, int] = {}
        ultimas: dict[str, dict] = {}
        alertas: list[dict] = []
        total_n = 0
        total_alertas = 0
        procesos: list[int] = []

        for r in resumenes:
            total_n += r["n"]
            total_alertas += r["n_alertas"]
            procesos.append(r["proceso"])
            ultimas.update(r.get("ultimas", {}))
            alertas.extend(r.get("alertas", []))
            for clave, d in r["por_variable"].items():
                g = por_variable.get(clave)
                if g is None:
                    por_variable[clave] = dict(d)
                else:
                    g["n"] += d["n"]
                    g["suma"] += d["suma"]
                    g["min"] = min(g["min"], d["min"])
                    g["max"] = max(g["max"], d["max"])
            for zona, c in r["zonas_alerta"].items():
                zonas_alerta[zona] = zonas_alerta.get(zona, 0) + c

        for g in por_variable.values():
            g["promedio"] = g["suma"] / g["n"] if g["n"] else 0.0

        zona_mayor_riesgo = "—"
        if zonas_alerta:
            zona_mayor_riesgo = max(zonas_alerta.items(), key=lambda kv: kv[1])[0]

        return {
            "mediciones_procesadas": total_n,
            "alertas_generadas": total_alertas,
            "por_variable": por_variable,
            "zona_mayor_riesgo": zona_mayor_riesgo,
            "procesos": sorted(procesos),
            "ultimas": ultimas,
            "alertas": alertas[-15:],
        }
