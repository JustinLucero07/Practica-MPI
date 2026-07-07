from __future__ import annotations

from typing import Callable

from mpi4py import MPI

from nucleo.analizador import CARGA_CPU_DEFECTO, AnalizadorDatos
from nucleo.config import crear_estacion

CMD_TERMINAR = "__TERMINAR__"
TAG_ASIGNACION = 11


class CoordinadorMPI:
    """Coordina la simulación distribuida con MPI (modelo SPMD).

    - El rank 0 es el proceso COORDINADOR.
    - Los rank != 0 son procesos TRABAJADORES.
    - Estrategia: las estaciones se reparten entre los procesos (round-robin);
      cada proceso trabaja solo con SUS estaciones (memoria local) y envía su
      resumen al coordinador, que lo consolida.
    """

    def __init__(self, comm, carga: int = CARGA_CPU_DEFECTO) -> None:
        self.comm = comm
        self.rank = comm.Get_rank()
        self.size = comm.Get_size()
        self.carga = carga


    def repartir(self, total: int) -> list[list[int]]:
        asign: list[list[int]] = [[] for _ in range(self.size)]
        for i in range(total):
            asign[i % self.size].append(i)
        return asign

    def _resumen_de(self, indices: list[int], ciclos: int) -> dict:
        estaciones = [crear_estacion(i) for i in indices]
        acc = []
        for ciclo in range(ciclos):
            for est in estaciones:
                acc.extend(est.trabajar_ciclo(ciclo, self.carga, self.rank))
        return AnalizadorDatos.resumen_local(acc, self.rank)

   
    
    def ejecutar_secuencial(self, n_est: int, ciclos: int) -> tuple[float, dict]:
        t0 = MPI.Wtime()
        resumen = self._resumen_de(list(range(n_est)), ciclos)
        return MPI.Wtime() - t0, AnalizadorDatos.consolidar([resumen])

    def ejecutar_paralelo(self, n_est: int, ciclos: int):
        
        if self.rank == 0:
            asign = self.repartir(n_est)
            mis = asign[0]
            for r in range(1, self.size):
                self.comm.send(asign[r], dest=r, tag=TAG_ASIGNACION)
        else:
            mis = self.comm.recv(source=0, tag=TAG_ASIGNACION)

        self.comm.Barrier()
        t0 = MPI.Wtime()
        resumen_local = self._resumen_de(mis, ciclos)

        
        resumenes = self.comm.gather(resumen_local, root=0)
        total = self.comm.reduce(resumen_local["n"], op=MPI.SUM, root=0)
        tp = self.comm.reduce(MPI.Wtime() - t0, op=MPI.MAX, root=0)

        if self.rank == 0:
            stats = AnalizadorDatos.consolidar(resumenes)
            return tp, stats, total, self.repartir(n_est)
        return None

    
    
    def bucle_trabajador(self) -> None:
        """Bucle de los procesos trabajadores en modo GUI."""
        while True:
            cmd = self.comm.bcast(None, root=0)
            if cmd == CMD_TERMINAR:
                return
            n_est, ciclos = cmd
            indices = self.comm.recv(source=0, tag=TAG_ASIGNACION)
            estaciones = [crear_estacion(i) for i in indices]
            acc = []
            for ciclo in range(ciclos):
                for est in estaciones:
                    acc.extend(est.trabajar_ciclo(ciclo, self.carga, self.rank))
                self.comm.gather(AnalizadorDatos.resumen_local(acc, self.rank), root=0)

    def ejecutar_para_gui(
        self, n_est: int, ciclos: int,
        on_cycle: Callable[[int, int, dict, float], None],
    ) -> tuple[float, list[int]]:
        """Corrida conducida por la GUI (solo rank 0). Llama on_cycle por ciclo."""
        self.comm.bcast((n_est, ciclos), root=0)
        asign = self.repartir(n_est)
        for r in range(1, self.size):
            self.comm.send(asign[r], dest=r, tag=TAG_ASIGNACION)
        estaciones = [crear_estacion(i) for i in asign[0]]
        acc = []
        t0 = MPI.Wtime()
        for ciclo in range(ciclos):
            for est in estaciones:
                acc.extend(est.trabajar_ciclo(ciclo, self.carga, 0))
            resumenes = self.comm.gather(AnalizadorDatos.resumen_local(acc, 0), root=0)
            stats = AnalizadorDatos.consolidar(resumenes)
            on_cycle(ciclo + 1, ciclos, stats, MPI.Wtime() - t0)
        return MPI.Wtime() - t0, [len(x) for x in asign]

    def terminar(self) -> None:
        self.comm.bcast(CMD_TERMINAR, root=0)
