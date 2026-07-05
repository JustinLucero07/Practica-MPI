#!/usr/bin/env bash
# Pruebas de rendimiento MPI con 1, 2 y 4 procesos.
# Ejecutar desde la raiz del proyecto (Practica-MPI):
#   bash cluster/benchmark.sh 8 20
# En cluster, agrega:  -hostfile cluster/hosts.txt   a cada mpiexec.

EST=${1:-8}
CIC=${2:-20}
PY=${PY:-python3}

echo "Benchmark MPI — $EST estaciones x $CIC ciclos"
for n in 1 2 4; do
  echo "----- $n proceso(s) -----"
  mpiexec -n "$n" "$PY" main.py --estaciones "$EST" --ciclos "$CIC" \
    | grep -E "Aceleramiento|Tiempo (secuencial|paralelo)|Eficiencia|Datos procesados"
done
