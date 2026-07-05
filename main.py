from __future__ import annotations

import argparse


def _consola(estaciones: int, ciclos: int, carga: int, solo_paralelo: bool) -> None:
    import platform

    from mpi4py import MPI

    from nucleo.config import crear_estacion
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
    print(f"Configuracion: {estaciones} estaciones x {ciclos} ciclos\n")
    print("Distribución MPI (send punto a punto, rank 0 -> cada trabajador):")
    for r, indices in enumerate(reparto):
        nombres = ", ".join(crear_estacion(i).nombre for i in indices)
        print(f"  Proceso {r}: {len(indices)} estacion(es) -> {nombres or '(ninguna)'}")
    print()

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


def _consola_local(modo: str, estaciones: int, ciclos: int, carga: int) -> None:
    """Corre secuencial/hilos/procesos SIN MPI (un solo `python`, sin mpiexec)."""
    import os
    import platform
    from time import perf_counter

    from monitoreo.controlador import ControladorMonitoreo
    from monitoreo.dominio import ModoEjecucion

    ctrl = ControladorMonitoreo(ciclos=ciclos, carga_cpu=carga, n_estaciones=estaciones)
    t0 = perf_counter()
    snap = ctrl.ejecutar(ModoEjecucion(modo.capitalize()))
    t = perf_counter() - t0
    e = snap.estadisticas

    print("=" * 70)
    print(f"SISTEMA DE MONITOREO AMBIENTAL URBANO — VERSION {modo.upper()} (sin MPI)")
    print("=" * 70)
    print(f"Python : {platform.python_version()}  ({platform.system()} {platform.release()})")
    print(f"Nucleos de CPU  : {os.cpu_count()}")
    print(f"Configuracion   : {estaciones} estaciones x {ciclos} ciclos\n")
    print(f"  Mediciones procesadas : {e.mediciones_procesadas}")
    print(f"  Alertas generadas     : {e.alertas_generadas}")
    print(f"  Zona de mayor riesgo  : {e.zona_mayor_riesgo}")
    print("  Por variable (prom / min / max):")
    for var, d in e.por_variable.items():
        print(f"    - {var.etiqueta:12} {d.promedio:7.1f} / {d.minimo:6.1f} / {d.maximo:6.1f}  (n={d.n})")
    print(f"\n  Tiempo total (T{modo[0]}): {t:.3f} s")
    print("=" * 70)


def main() -> None:
    parser = argparse.ArgumentParser(description="Monitoreo ambiental urbano — MPI")
    parser.add_argument("--gui", action="store_true", help="Abre la interfaz grafica (proceso unico).")
    parser.add_argument(
        "--modo", choices=["mpi", "secuencial", "hilos", "procesos"], default="mpi",
        help="Modo de ejecucion en consola. 'mpi' requiere mpiexec; los demas corren con un solo python.",
    )
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

    if args.modo == "mpi":
        _consola(args.estaciones, args.ciclos, args.carga, args.solo_paralelo)
    else:
        _consola_local(args.modo, args.estaciones, args.ciclos, args.carga)


if __name__ == "__main__":
    main()
