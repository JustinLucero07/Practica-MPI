from __future__ import annotations

import argparse
import json

from mpi4py import MPI

from nucleo.config import crear_estacion
from nucleo.coordinador import CoordinadorMPI


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--estaciones", type=int, default=8)
    parser.add_argument("--ciclos", type=int, default=20)
    parser.add_argument("--carga", type=int, default=600)
    args = parser.parse_args()

    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()
    coord = CoordinadorMPI(comm, args.carga)

    if rank != 0:
        coord.bucle_trabajador()
        return

    roster = [(crear_estacion(i), i % size) for i in range(args.estaciones)]

    def on_cycle(ciclo: int, total: int, stats: dict, t: float) -> None:
        ult = stats.get("ultimas", {})
        estaciones = []
        for est, proc in roster:
            u = ult.get(est.nombre, {})
            estaciones.append({
                "id": est.id, "nombre": est.nombre, "zona": est.zona,
                "marca": f"P{u.get('proceso', proc)}",
                "medicion": u.get("texto", "—"), "hora": u.get("hora", "—"),
            })
        d = {
            "modo": f"MPI ({size} procesos)",
            "ciclo": ciclo, "ciclos": total, "tiempo": t, "en_ejecucion": True,
            "estaciones": estaciones,
            "mediciones": stats["mediciones_procesadas"],
            "alertas": stats["alertas_generadas"],
            "zona": stats["zona_mayor_riesgo"],
            "por_variable": [
                {"variable": k, "prom": v["promedio"], "min": v["min"], "max": v["max"]}
                for k, v in stats["por_variable"].items()
            ],
            "alertas_lista": [
                {"texto": f"P{a['proceso']} · {a['zona']} — {a['variable']} {a['valor']:.1f}",
                 "severidad": a["severidad"]}
                for a in stats.get("alertas", [])
            ],
        }
        print(json.dumps(d), flush=True)

    tiempo, reparto = coord.ejecutar_para_gui(args.estaciones, args.ciclos, on_cycle)
    print(json.dumps({"fin": True, "tiempo": tiempo, "reparto": reparto}), flush=True)
    coord.terminar()


if __name__ == "__main__":
    main()
