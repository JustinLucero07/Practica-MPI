from __future__ import annotations

import argparse
import platform

from mpi4py import MPI

from nucleo.coordinador import CoordinadorMPI


def _imprimir_entorno(comm) -> None:
    version = MPI.Get_library_version().replace("\x00", "").splitlines()[0]
    print("=" * 70)
    print("SISTEMA DE MONITOREO AMBIENTAL URBANO — VERSION MPI (CUENCA)")
    print("=" * 70)
    print(f"Python : {platform.python_version()}  ({platform.system()} {platform.release()})")
    print(f"MPI    : {version}")
    print(f"Procesos MPI (size): {comm.Get_size()}")
    print("=" * 70)


def _imprimir_stats(stats: dict) -> None:
    print(f"  Mediciones procesadas : {stats['mediciones_procesadas']}")
    print(f"  Alertas generadas     : {stats['alertas_generadas']}")
    print(f"  Zona de mayor riesgo  : {stats['zona_mayor_riesgo']}")
    print(f"  Procesos participantes: {stats['procesos']}")
    print("  Por variable (prom / min / max):")
    for var, d in stats["por_variable"].items():
        print(f"    - {var:12} {d['promedio']:7.1f} / {d['min']:6.1f} / {d['max']:6.1f}  (n={d['n']})")


def comparar(comm, coord: CoordinadorMPI, n_est: int, ciclos: int, solo_paralelo: bool) -> None:
    ts = None
    stats_seq = None
    if comm.Get_rank() == 0 and not solo_paralelo:
        ts, stats_seq = coord.ejecutar_secuencial(n_est, ciclos)
    comm.Barrier()

    resultado = coord.ejecutar_paralelo(n_est, ciclos)

    if comm.Get_rank() != 0:
        return
    tp, stats_par, total, reparto = resultado
    _imprimir_entorno(comm)
    print(f"Configuracion: {n_est} estaciones x {ciclos} ciclos")
    print(f"Reparto de estaciones por proceso: {reparto}\n")

    if ts is not None:
        print(f">>> SECUENCIAL (1 proceso)  Ts = {ts:.3f} s")
        _imprimir_stats(stats_seq)
        print()

    print(f">>> PARALELO MPI ({comm.Get_size()} procesos)  Tp = {tp:.3f} s")
    _imprimir_stats(stats_par)

    print("\n" + "=" * 70)
    print("RENDIMIENTO")
    print("=" * 70)
    print(f"  Procesos (p)        : {comm.Get_size()}")
    print(f"  Datos procesados    : {total} mediciones")
    if ts is not None and tp:
        speedup = ts / tp
        eficiencia = speedup / comm.Get_size()
        print(f"  Tiempo secuencial Ts: {ts:.3f} s")
        print(f"  Tiempo paralelo   Tp: {tp:.3f} s")
        print(f"  Aceleramiento  S=Ts/Tp : x{speedup:.2f}")
        print(f"  Eficiencia    E=S/p    : {eficiencia:.2%}")
    else:
        print(f"  Tiempo paralelo   Tp: {tp:.3f} s")
    print("=" * 70)


def main() -> None:
    parser = argparse.ArgumentParser(description="Monitoreo ambiental urbano — MPI")
    parser.add_argument("--gui", action="store_true", help="Abre la interfaz grafica (rank 0).")
    parser.add_argument("--estaciones", type=int, default=8, help="Numero de estaciones.")
    parser.add_argument("--ciclos", type=int, default=20, help="Ciclos de simulacion.")
    parser.add_argument("--carga", type=int, default=600, help="Carga de CPU del analisis.")
    parser.add_argument("--solo-paralelo", action="store_true",
                        help="Omite el baseline secuencial (no calcula speedup).")
    args = parser.parse_args()

    comm = MPI.COMM_WORLD
    coord = CoordinadorMPI(comm, args.carga)

    if args.gui:
        if comm.Get_rank() == 0:
            from UserInterface.app import lanzar_gui
            lanzar_gui(coord, args.estaciones, args.ciclos)
        else:
            coord.bucle_trabajador()
        return

    comparar(comm, coord, args.estaciones, args.ciclos, args.solo_paralelo)


if __name__ == "__main__":
    main()
