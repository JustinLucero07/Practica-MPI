from __future__ import annotations

import argparse


def _consola(estaciones: int, ciclos: int, carga: int, solo_paralelo: bool) -> None:
    import platform

    from mpi4py import MPI

    from nucleo.coordinador import CoordinadorMPI

    comm = MPI.COMM_WORLD
    coord = CoordinadorMPI(comm, carga)

    def imprimir_stats(stats: dict) -> None:
        print(f"  Mediciones procesadas : {stats['mediciones_procesadas']}")
        print(f"  Alertas generadas     : {stats['alertas_generadas']}")
        print(f"  Zona de mayor riesgo  : {stats['zona_mayor_riesgo']}")
        print(f"  Procesos participantes: {stats['procesos']}")
        print("  Por variable (prom / min / max):")
        for var, d in stats["por_variable"].items():
            print(f"    - {var:12} {d['promedio']:7.1f} / {d['min']:6.1f} / {d['max']:6.1f}  (n={d['n']})")

    ts = None
    stats_seq = None
    if comm.Get_rank() == 0 and not solo_paralelo:
        ts, stats_seq = coord.ejecutar_secuencial(estaciones, ciclos)
    comm.Barrier()

    resultado = coord.ejecutar_paralelo(estaciones, ciclos)
    if comm.Get_rank() != 0:
        return

    tp, stats_par, total, reparto = resultado
    version = MPI.Get_library_version().replace("\x00", "").splitlines()[0]
    print("=" * 70)
    print("SISTEMA DE MONITOREO AMBIENTAL URBANO — VERSION MPI (CUENCA)")
    print("=" * 70)
    print(f"Python : {platform.python_version()}  ({platform.system()} {platform.release()})")
    print(f"MPI    : {version}")
    print(f"Configuracion: {estaciones} estaciones x {ciclos} ciclos")
    print(f"Reparto de estaciones por proceso: {reparto}\n")

    if ts is not None:
        print(f">>> SECUENCIAL (1 proceso)  Ts = {ts:.3f} s")
        imprimir_stats(stats_seq)
        print()

    print(f">>> PARALELO MPI ({comm.Get_size()} procesos)  Tp = {tp:.3f} s")
    imprimir_stats(stats_par)

    print("\n" + "=" * 70)
    print("RENDIMIENTO")
    print("=" * 70)
    print(f"  Procesos (p)        : {comm.Get_size()}")
    print(f"  Datos procesados    : {total} mediciones")
    if ts is not None and tp:
        s = ts / tp
        print(f"  Tiempo secuencial Ts: {ts:.3f} s")
        print(f"  Tiempo paralelo   Tp: {tp:.3f} s")
        print(f"  Aceleramiento  S=Ts/Tp : x{s:.2f}")
        print(f"  Eficiencia    E=S/p    : {s / comm.Get_size():.2%}")
    else:
        print(f"  Tiempo paralelo   Tp: {tp:.3f} s")
    print("=" * 70)


def main() -> None:
    parser = argparse.ArgumentParser(description="Monitoreo ambiental urbano — MPI")
    parser.add_argument("--gui", action="store_true", help="Abre la interfaz grafica (proceso unico).")
    parser.add_argument("--estaciones", type=int, default=8)
    parser.add_argument("--ciclos", type=int, default=20)
    parser.add_argument("--carga", type=int, default=600)
    parser.add_argument("--procesos-mpi", type=int, default=4, help="Procesos por defecto en modo MPI de la GUI.")
    parser.add_argument("--solo-paralelo", action="store_true")
    args = parser.parse_args()

    if args.gui:
        from UserInterface.app import lanzar
        lanzar(args.estaciones, args.ciclos, args.carga, args.procesos_mpi)
        return

    _consola(args.estaciones, args.ciclos, args.carga, args.solo_paralelo)


if __name__ == "__main__":
    main()
