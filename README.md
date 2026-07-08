# Sistema de Monitoreo Ambiental Urbano — Versión MPI (Cuenca)

Versión **paralela con paso de mensajes (MPI / mpi4py)** del sistema de monitoreo
ambiental, pensada para ejecutarse en un **clúster** de computadoras. Distribuye
las estaciones ambientales entre varios procesos MPI, cada uno con su **memoria
local**, que se comunican mediante paso de mensajes para consolidar los
resultados en un proceso coordinador.

Incluye una **versión secuencial de referencia** y una **versión paralela MPI**
bajo el modelo **SPMD** (el mismo programa lo ejecutan todos los procesos).

---

## Diseño de la solución (POO)

El sistema reutiliza las clases de la práctica de hilos/procesos y añade un
coordinador MPI. Clases:

| Clase | Responsabilidad |
|---|---|
| `Variable` | Variable ambiental con su unidad y **umbral** de alerta. |
| `Medicion` | Lectura individual. Incluye estación, zona, variable, valor, **ciclo** y **proceso MPI** que la generó. |
| `EstacionAmbiental` | Genera las mediciones simuladas y ejecuta el cálculo CPU-bound. |
| `AnalizadorDatos` | Calcula el resumen local de un proceso y **consolida** los resúmenes de todos. |
| `AlertaAmbiental` | Alerta cuando una variable supera su umbral. |
| `CoordinadorMPI` | Reparte estaciones, coordina la comunicación MPI y mide el rendimiento. |

### ¿Por qué existen `nucleo/` **y** `monitoreo/`?

El proyecto tiene **dos** paquetes con un dominio muy parecido (`Variable`,
`Medicion`, `EstacionAmbiental`, `AnalizadorDatos`, `AlertaAmbiental`), y es
importante dejar claro que **no es duplicación accidental**:

- **`nucleo/`** es **la solución de esta práctica** (la que se evalúa): la
  versión secuencial de referencia y la versión paralela con **MPI**
  (`CoordinadorMPI`), pensadas para ejecutarse en un clúster. Todo lo pedido
  por la rúbrica (punto a punto, colectivas, distribución de trabajo,
  consolidación, Ts/Tp/S/E) vive aquí.
- **`monitoreo/`** es el código de una **práctica anterior** (hilos y
  procesos con `threading`/`multiprocessing`, memoria **compartida** en una
  sola máquina — sin MPI). Se reutiliza tal cual, sin modificarlo, únicamente
  para poder comparar **4 arquitecturas de paralelismo** (secuencial, hilos,
  procesos, MPI) con el mismo problema — ver la sección
  [Comparación de los 4 modos de paralelismo](#comparación-de-los-4-modos-de-paralelismo).
  Esa comparación **no es un requisito de la rúbrica de MPI**, es un extra
  para enriquecer el análisis de rendimiento del informe.

Por eso hay dos versiones de `EstacionAmbiental`/`AnalizadorDatos`/etc. muy
parecidas: cada una está adaptada a un modelo de concurrencia distinto
(`nucleo/` sabe de `rank`/`proceso` MPI y memoria distribuida; `monitoreo/`
sabe de hilos/procesos y memoria compartida, y además trackea el **estado**
de cada estación — `Esperando`/`Procesando`/`Finalizada` — para poder pintarlo
en vivo en la GUI, algo que `nucleo/` no necesita). Si se quisiera eliminar la
duplicación, habría que fusionar ambos dominios en una sola base compartida y
que cada `Coordinador`/`Controlador` solo aporte su estrategia de
concurrencia — no se hizo para no arriesgar el código ya probado de la
práctica de hilos/procesos.

### Estrategia de paralelización

- **Modelo SPMD:** todos los procesos ejecutan `main.py`. El **rank 0** actúa
  de **coordinador**; los demás son **trabajadores**.
- **División de datos:** las estaciones se reparten entre los procesos con un
  esquema *round-robin* (la estación `i` va al proceso `i % size`). Así cada
  proceso trabaja **solo con sus estaciones** (memoria local), nunca todos hacen
  el mismo trabajo.
- **Consolidación:** cada proceso calcula un **resumen local** (sumas, mín/máx,
  alertas por zona) y el coordinador los une en un **resultado global**.

---

## Comunicación MPI utilizada

La rúbrica exige al menos una comunicación **punto a punto** y una **colectiva**.
Este proyecto usa ambas:

| Tipo | Operación | Dónde y por qué |
|---|---|---|
| **Punto a punto** | `comm.send` / `comm.recv` | El coordinador (rank 0) **asigna a cada trabajador** su lista de estaciones, enviándosela individualmente. Es una comunicación dirigida de un proceso a otro. |
| **Colectiva** | `comm.gather` | Recoge en el rank 0 el **resumen local de todos** los procesos en una sola operación. |
| **Colectiva** | `comm.reduce` (SUM y MAX) | Suma el **total de mediciones** procesadas y toma el **tiempo del proceso más lento** (Tp). |

> **Por qué estas operaciones:** el reparto de trabajo es dirigido (cada
> trabajador recibe algo distinto), por eso encaja `send/recv` punto a punto. La
> consolidación necesita juntar datos de **todos** los procesos a la vez, que es
> exactamente lo que hacen `gather` y `reduce` (colectivas), más eficientes y
> claras que hacerlo con muchos `send/recv`.

---

## Requisitos e instalación

- **Python 3.10+** y una implementación de **MPI** (Open MPI o MPICH).
- **mpi4py**.
- En el clúster real de esta práctica se usó específicamente la build
  free-threaded **`python3.14t`**, instalada en la misma ruta absoluta
  (`/usr/local/bin/python3.14t`) en las 3 máquinas — ver la sección
  [Configuración real del clúster](#configuración-real-del-clúster-host--2-máquinas-virtuales).

```bash
# MPI (Debian/Ubuntu)
sudo apt install openmpi-bin libopenmpi-dev

# mpi4py
pip3 install --user mpi4py
```

---

## Ejecución

> Ejecutar siempre **desde la raíz** `Practica-MPI/` (para que se encuentre el
> paquete `nucleo`). Usa el mismo intérprete que tenga `mpi4py` instalado
> (`python3.14t` en este clúster) en vez de `python3` a secas.

### En una sola computadora (consola)

```bash
# Secuencial + paralelo MPI (calcula el speedup automáticamente)
mpiexec -n 1 python3.14t main.py --estaciones 8 --ciclos 20
mpiexec -n 2 python3.14t main.py --estaciones 8 --ciclos 20
mpiexec -n 4 python3.14t main.py --estaciones 8 --ciclos 20

# Solo la parte paralela (sin baseline secuencial)
mpiexec -n 4 python3.14t main.py --estaciones 8 --ciclos 20 --solo-paralelo
```

### Los 4 modos por separado (`--modo`)

`main.py` soporta `--modo secuencial|hilos|procesos|mpi` para comparar las
4 formas de paralelismo con el mismo dominio (estaciones, mediciones,
alertas). **Solo `mpi` necesita `mpiexec`**; los otros tres corren con un
solo `python3.14t` (usan `threading`/`multiprocessing`, no MPI):

```bash
# Mismo tamaño de problema en los 4 modos, para comparar tiempos:
python3.14t main.py --modo secuencial --estaciones 40 --ciclos 50 --carga 600
python3.14t main.py --modo hilos       --estaciones 40 --ciclos 50 --carga 600
python3.14t main.py --modo procesos    --estaciones 40 --ciclos 50 --carga 600
mpiexec -n 4 python3.14t main.py --estaciones 40 --ciclos 50 --carga 600

# Problema grande, para ver diferencias de tiempo más claras (puede tardar):
python3.14t main.py --modo secuencial --estaciones 200 --ciclos 100 --carga 600
mpiexec -n 6 -hostfile cluster/hosts.txt --mca btl_tcp_if_include 192.168.56.0/24 \
  python3.14t main.py --estaciones 200 --ciclos 100 --carga 600
```

Cada uno imprime tiempo total, mediciones procesadas, alertas y estadísticas
por variable — el modo `mpi` además imprime **Ts, Tp, aceleramiento S y
eficiencia E**, y el detalle de **qué estaciones se le mandaron a cada
proceso** (ver más abajo). Si tarda demasiado, baja `--carga` (controla el
costo de CPU por ciclo, no la cantidad de datos) o `--estaciones`/`--ciclos`.

### Con interfaz gráfica (GUI + MPI)

El proceso 0 abre la ventana Tkinter (la misma interfaz de la práctica de
hilos/procesos) y coordina; los demás procesos son trabajadores que calculan y le
envían resultados por MPI:

```bash
mpiexec -n 4 python3.14t main.py --gui --estaciones 8 --ciclos 20
```

La GUI muestra cada estación con el **proceso MPI** que la atiende, su última
medición, las estadísticas globales, las alertas y el entorno (Python, SO, nº de
procesos MPI, librería MPI). El botón **Iniciar** lanza la simulación distribuida
y la ventana se actualiza ciclo a ciclo.

Cada ejecución de consola en modo `mpi` imprime, en este orden: la
**distribución explícita** de qué estaciones (por nombre) se le mandaron a
cada proceso vía `send` punto a punto, las estadísticas globales (idénticas
sin importar el nº de procesos, lo que valida la consolidación), y el bloque
de rendimiento con **Ts, Tp, aceleramiento S y eficiencia E**. Ejemplo real:

```
Distribución MPI (send punto a punto, rank 0 -> cada trabajador):
  Proceso 0: 2 estacion(es) -> Estacion Centro, Estacion Totoracocha
  Proceso 1: 2 estacion(es) -> Estacion Yanuncay, Estacion El Vecino
  Proceso 2: 1 estacion(es) -> Estacion Monay
  Proceso 3: 1 estacion(es) -> Estacion Machangara
  Proceso 4: 1 estacion(es) -> Estacion Banos
  Proceso 5: 1 estacion(es) -> Estacion Ricaurte
```

### En un clúster (varios nodos)

```bash
mpiexec -n 6 -hostfile cluster/hosts.txt --mca btl_tcp_if_include 192.168.56.0/24 \
  python3.14t main.py --estaciones 8 --ciclos 20
```

Ver [cluster/hosts.txt](cluster/hosts.txt) para el formato del archivo de nodos.
Requisitos del clúster: **SSH sin contraseña** entre nodos, **misma ruta** del
proyecto en todos, y **mpi4py instalado** en cada nodo con el **mismo
intérprete** (`python3.14t`).

---

## Configuración real del clúster (host + 2 máquinas virtuales)

El clúster de esta práctica está formado por **3 nodos** sobre una red
**host-only de VirtualBox** (interfaz `vboxnet0`, `192.168.56.0/24`): el
anfitrión actúa de maestro/coordinador y las 2 VMs son los trabajadores.

| Nodo | Hostname | Usuario | Rol | IP | Slots |
|---|---|---|---|---|---|
| Anfitrión | `justin07` | `justin` | maestro / coordinador (rank 0) | `192.168.56.1` | 2 |
| Máquina virtual 1 | `mpi1` | `mpi` | trabajador | `192.168.56.101` | 2 |
| Máquina virtual 2 | `mpi2` | `mpi` | trabajador | `192.168.56.102` | 2 |

El adaptador host-only le da IP fija a cada VM en `192.168.56.0/24` y las
conecta directo con el anfitrión, sin depender de la red externa (WiFi/hotspot).

### 1. Red y SSH en las VMs

Con el adaptador host-only activo, el anfitrión hace `ping 192.168.56.101` y
`ping 192.168.56.102` sin problema. En **cada VM** se instala y activa el
servidor SSH:

```bash
sudo apt install -y openssh-server
sudo systemctl enable --now ssh
```

### 2. Instalar MPI en las 3 máquinas

```bash
sudo apt install -y openmpi-bin libopenmpi-dev
```

### 3. SSH sin contraseña (usuario `mpi` en ambas VMs)

```bash
# En el MAESTRO (justin07):
ssh-keygen -t rsa -N "" -f ~/.ssh/id_rsa
ssh-copy-id mpi@192.168.56.101
ssh-copy-id mpi@192.168.56.102
```

`~/.ssh/config` en el maestro (para no escribir el usuario cada vez):

```
Host 192.168.56.101
    User mpi
Host 192.168.56.102
    User mpi
```

```bash
ssh 192.168.56.101 hostname     # debe entrar SIN contraseña -> mpi1
ssh 192.168.56.102 hostname     # debe entrar SIN contraseña -> mpi2
```

### 4. Mismo intérprete de Python en los 3 nodos: `python3.14t`

Se usa la build **free-threaded** de Python 3.14 (`python3.14t`) instalada en
la **misma ruta absoluta** en las 3 máquinas: `/usr/local/bin/python3.14t`.
Instalar el mismo binario en la misma ruta evita el problema típico de mezclar
intérpretes distintos (el `python3`/`python3.14` de cada sistema) que hace que
MPI, al lanzar el proceso remoto por SSH, use uno sin `mpi4py` instalado.

```bash
# En las 3 máquinas:
/usr/local/bin/python3.14t -m pip install --upgrade pip mpi4py
```

### 5. Código en la misma ruta en los 3 nodos

```bash
# En AMBAS VMs:
sudo mkdir -p /opt/Practica-MPI && sudo chown -R mpi:mpi /opt/Practica-MPI
# Desde el maestro, copiar el proyecto a cada VM:
scp -r /opt/Practica-MPI mpi@192.168.56.101:/opt/
scp -r /opt/Practica-MPI mpi@192.168.56.102:/opt/
```

### 6. Archivo de hosts

[cluster/hosts.txt](cluster/hosts.txt) con las IP reales y los *slots*:

```
192.168.56.1   slots=2
192.168.56.101 slots=2
192.168.56.102 slots=2
```

### 7. Ejecutar en el clúster (desde el maestro)

```bash
cd /opt/Practica-MPI

# Prueba de que corre en las 3 máquinas (evidencia de clúster):
mpiexec -n 6 -hostfile cluster/hosts.txt --mca btl_tcp_if_include 192.168.56.0/24 \
  python3.14t -c "import socket; print(socket.gethostname())"

# La práctica (comparación secuencial vs paralelo + métricas Ts, Tp, S, E):
mpiexec -n 6 -hostfile cluster/hosts.txt --mca btl_tcp_if_include 192.168.56.0/24 \
  python3.14t main.py --estaciones 8 --ciclos 20
```

**Salida real de esta corrida sobre las 3 máquinas** (`justin07` + `mpi1` +
`mpi2`, 6 procesos):

```
======================================================================
SISTEMA DE MONITOREO AMBIENTAL URBANO — VERSION MPI (CUENCA)
======================================================================
Python : 3.14.4  (Linux 6.17.0-35-generic)
MPI    : Open MPI v4.1.6, package: Debian OpenMPI, ident: 4.1.6, repo rev: v4.1.6, Sep 30, 2023
Configuracion: 8 estaciones x 20 ciclos
Reparto de estaciones por proceso: [2, 2, 1, 1, 1, 1]

>>> SECUENCIAL (1 proceso)  Ts = 1.294 s
  Mediciones procesadas : 680
  Alertas generadas     : 86
  Zona de mayor riesgo  : Totoracocha

>>> PARALELO MPI (6 procesos)  Tp = 0.350 s
  Mediciones procesadas : 680
  Alertas generadas     : 86
  Zona de mayor riesgo  : Totoracocha
  Procesos participantes: [0, 1, 2, 3, 4, 5]

======================================================================
RENDIMIENTO
======================================================================
  Procesos (p)        : 6
  Datos procesados    : 680 mediciones
  Tiempo secuencial Ts: 1.294 s
  Tiempo paralelo   Tp: 0.350 s
  Aceleramiento  S=Ts/Tp : x3.70
  Eficiencia    E=S/p    : 61.66%
======================================================================
```

Las estadísticas por variable son **idénticas** a las de la tabla de 1 sola
máquina (misma semilla, mismos datos), lo que confirma que la consolidación
es correcta corriendo en un clúster real de 3 nodos. La eficiencia baja frente
al caso de 1 nodo (61.66% vs. las de la tabla siguiente) porque ahora **el
`send`/`gather` viaja por red Ethernet virtual** entre 3 máquinas distintas en
vez de memoria compartida — es justo el costo de comunicación que la práctica
pide analizar.

Con la GUI (`--gui`), el subproceso MPI se lanza automáticamente con el mismo
intérprete con el que se abrió la GUI, apuntando a `nucleo.mpi_runner`. Por
ejemplo, una corrida de estrés real ya ejecutada en este clúster (1000
estaciones, 12 ciclos, carga 600) se ve así en `mpiexec`:

```bash
mpiexec -n 6 --wdir /opt/Practica-MPI -hostfile cluster/hosts.txt \
  --mca btl_tcp_if_include 192.168.56.0/24 \
  python3.14t -m nucleo.mpi_runner --estaciones 1000 --ciclos 12 --carga 600
```

y se confirma en cada VM viendo los procesos trabajadores corriendo (2 por
VM, según `slots=2`):

```bash
mpi@mpi1:/opt/Practica-MPI$ pgrep -a python3.14t
1669 /usr/local/bin/python3.14t -m nucleo.mpi_runner --estaciones 1000 --ciclos 12 --carga 600
1670 /usr/local/bin/python3.14t -m nucleo.mpi_runner --estaciones 1000 --ciclos 12 --carga 600

mpi@mpi2:/opt/Practica-MPI$ pgrep -a python3.14t
1795 /usr/local/bin/python3.14t -m nucleo.mpi_runner --estaciones 1000 --ciclos 12 --carga 600
1796 /usr/local/bin/python3.14t -m nucleo.mpi_runner --estaciones 1000 --ciclos 12 --carga 600
```

Si se **cuelga sin imprimir** (el anfitrión tiene varias interfaces: WiFi
`wlo1`, puentes de Docker, `vboxnet0`...), es obligatorio forzar la red del
clúster con `--mca btl_tcp_if_include 192.168.56.0/24`, como en los comandos
de arriba.

### Problemas reales encontrados (para el informe)

- **`ssh: Connection refused`** → las VMs no tenían SSH; instalar/arrancar `openssh-server` (paso 1).
- **`scp: No such file / Permiso denegado`** → `/opt/Practica-MPI` era de root en las VMs; hacer `chown mpi:mpi` (paso 5).
- **`no such identity: ~/.ssh/id_rsa`** → faltaba generar la llave (paso 3).
- **`ModuleNotFoundError: mpi4py` solo en las VMs** → el intérprete que MPI usa por SSH (no interactivo) no era el mismo que tenía mpi4py instalado; se resolvió instalando **`python3.14t` en la misma ruta absoluta** (`/usr/local/bin/python3.14t`) en las 3 máquinas (paso 4), en vez de depender del `python3`/`python3.14` que trae cada sistema.
- **Contraseña de SSH en cada arranque** → faltaba `ssh-copy-id` o el `~/.ssh/config` con el usuario `mpi`.
- **Se cuelga sin salida** → el anfitrión tiene varias interfaces de red (WiFi, puentes de Docker, `vboxnet0`); forzar `--mca btl_tcp_if_include 192.168.56.0/24` para que MPI use solo la red host-only del clúster.

### Script de pruebas

```bash
PY=python3.14t bash cluster/benchmark.sh 8 20      # corre 1, 2 y 4 procesos automáticamente
```

---

## Resultados (1 nodo, 20 núcleos, Open MPI 4.1.6)

Métricas: `S = Ts / Tp` (aceleramiento) y `E = S / p` (eficiencia).

### 8 estaciones × 20 ciclos (680 mediciones)

| Procesos (p) | Tp (paralelo) | S = Ts/Tp | Eficiencia |
|---|---|---|---|
| 1 | 0.969 s | x1.01 | 101 % |
| 2 | 0.490 s | x2.01 | 100 % |
| 4 | 0.275 s | x3.66 | 91 % |

(Ts ≈ 0.98 s)

### 12 estaciones × 30 ciclos (1560 mediciones)

| Procesos (p) | Tp (paralelo) | S = Ts/Tp | Eficiencia |
|---|---|---|---|
| 1 | 2.564 s | x1.01 | 100 % |
| 2 | 1.299 s | x2.00 | 99 % |
| 4 | 0.726 s | x3.67 | 91 % |

(Ts ≈ 2.6 s)

**Análisis:** el aceleramiento es **casi lineal** (x2 con 2 procesos, ~x3.7 con
4). La eficiencia baja un poco con 4 procesos por el coste de comunicación
(reparto + gather) y porque el trabajo total es fijo (más procesos = menos
trabajo por proceso, más peso relativo de la comunicación). Las estadísticas
globales son idénticas en todos los casos, lo que confirma que la **distribución
del trabajo y la consolidación son correctas**.

---

## Comparación de los 4 modos de paralelismo

### Secuencial vs. Hilos vs. Procesos vs. MPI (40 estaciones × 50 ciclos, carga 600, 8200 mediciones — 1 sola máquina `justin07`, 20 núcleos)

| Modo | Trabajadores | Tiempo | Speedup vs. secuencial |
|---|---|---|---|
| Secuencial | 1 | 18.114 s | x1.00 (referencia) |
| Hilos | 20 (auto = núcleos) | 2.582 s | x7.02 |
| Procesos | 20 (auto = núcleos) | 2.925 s | x6.19 |
| MPI | 4 (`-n 4`) | 5.225 s | x3.80 (con su propio Ts = 19.838 s) |

> **Ojo con esta tabla:** Hilos/Procesos no reciben cuántos trabajadores usar
> por parámetro — `ControladorMonitoreo` los calcula solo como
> `min(cpu_count(), n_estaciones)`, que en esta PC de 20 núcleos da **20
> trabajadores**. La fila de MPI, en cambio, se lanzó con `-n 4` explícito.
> Por eso Hilos/Procesos "le ganan" a MPI aquí: **no es una comparación de
> arquitecturas en igualdad de condiciones**, es 20 trabajadores contra 4. Si
> quieres comparar arquitecturas de verdad (no solo validar el speedup de tu
> solución MPI), iguala el número de trabajadores, p. ej. corriendo también
> `mpiexec -n 20 python3.14t main.py --estaciones 40 --ciclos 50`.

### Secuencial vs. Hilos vs. Procesos vs. MPI (1000 estaciones × 20 ciclos, carga 600, 80 080 mediciones — 1 sola máquina `justin07`, 20 núcleos)

Comandos usados (los 4 con el mismo tamaño de problema):

```bash
python3.14t main.py --modo secuencial --estaciones 1000 --ciclos 20 --carga 600
python3.14t main.py --modo hilos       --estaciones 1000 --ciclos 20 --carga 600
python3.14t main.py --modo procesos    --estaciones 1000 --ciclos 20 --carga 600
mpiexec -n 6 python3.14t main.py --estaciones 1000 --ciclos 20 --carga 600
```

| Modo | Trabajadores | Tiempo | Speedup vs. secuencial (130.636 s) |
|---|---|---|---|
| Secuencial | 1 | 130.636 s | x1.00 (referencia) |
| Hilos | 20 (auto = núcleos) | 18.775 s | x6.96 |
| Procesos | 20 (auto = núcleos) | 24.436 s | x5.35 |
| MPI | 6 (`-n 6`) | 29.325 s | x4.46 (con su propio Ts = 153.045 s, S=x5.22, E=86.98 %) |

> **Por qué el MPI de esta fila muestra dos velocidades:** el modo `mpi`
> corre su **propio** baseline secuencial dentro de la misma ejecución
> (Ts = 153.045 s) para calcular `S = Ts/Tp` de forma justa (mismo proceso,
> mismas condiciones del sistema en ese instante). Esa Ts interna difiere de
> la corrida `--modo secuencial` independiente (130.636 s) por variación
> normal del sistema entre ejecuciones (~15 %, uso de CPU de fondo, caché,
> etc.) — **no** es un cambio en el algoritmo. Por eso, para juzgar el
> aceleramiento **oficial** de la solución MPI se usa siempre el Ts medido
> **dentro de la misma corrida** (x5.22, E=86.98 %), y solo se compara contra
> Hilos/Procesos como referencia aproximada de qué tan rápido va cada
> arquitectura frente al mismo trabajo.

### MPI en el clúster real (3 nodos: `justin07` + `mpi1` + `mpi2`, 6 procesos)

| Estaciones × ciclos | Mediciones | Ts | Tp | S = Ts/Tp | E = S/p |
|---|---|---|---|---|---|
| 8 × 20 | 680 | 1.294 s | 0.350 s | x3.70 | 61.66 % |
| 200 × 100 | 80 400 | 226.149 s | 49.102 s | x4.61 | 76.76 % |

**Análisis:** con el problema **grande** (200×100) la eficiencia sube de
61.66 % a 76.76 % frente al problema pequeño (8×20), aunque ambos usan los
mismos 6 procesos en los mismos 3 nodos. El costo de comunicación por red
(`send` del reparto + `gather`/`reduce` finales) es prácticamente **fijo**
por corrida; cuando el problema es grande, ese costo fijo pesa mucho menos
frente al cómputo real de cada proceso, así que la eficiencia se acerca más al
ideal. Con el problema chico, la comunicación por red pesa proporcionalmente
más, y por eso la eficiencia es más baja que la del mismo tamaño (8×20) en un
solo nodo, donde no hay red de por medio (compárese con el 91 % de
la tabla "4 procesos" de la sección de un solo nodo).

### ¿Cómo comparar bien secuencial vs. paralelo (Ts vs. Tp)?

Lo único que debe mantenerse **igual** entre la corrida secuencial y la
paralela es el **tamaño del problema**: `--estaciones`, `--ciclos` y `--carga`.
Eso ya lo garantiza el código: dentro de una misma ejecución de `main.py`
(modo `mpi`), tanto `ejecutar_secuencial(...)` como `ejecutar_paralelo(...)`
reciben los mismos `args.estaciones`/`args.ciclos`/`args.carga` — no hay forma
de que Ts y Tp se calculen con datos distintos. Lo único que cambia, y que es
justamente lo que se mide, es **cuántos procesos (`p`)** se usan para repartir
ese mismo trabajo (`-n 1`, `-n 2`, `-n 4`, `-n 6`...).

Para comparar los **4 modos** entre sí (como en la tabla de arriba), la regla
es la misma: fija `--estaciones`, `--ciclos` y `--carga` idénticos en las 4
corridas (así lo hiciste: 40/50/600 en las cuatro) — lo único que puede variar
"a propósito" es cuántos trabajadores usa cada modo, y por eso hay que dejar
explícito cuántos son en cada caso (ver la nota de la tabla anterior).

### ¿Hasta qué tamaño (estaciones × ciclos) puedo probar?

El código no tiene ningún límite fijo — puedes poner cualquier `--estaciones`
y `--ciclos`. El único límite real es el **tiempo**, porque el costo es
CPU-bound y escala aproximadamente lineal con `estaciones × ciclos × carga`.
Con `--carga 600` (el valor por defecto), el tiempo secuencial medido fue:

| Corrida | estaciones × ciclos | Ts medido | ms por "estación-ciclo" |
|---|---|---|---|
| 1 nodo | 8 × 20 = 160 | 1.294 s | ~8.1 ms |
| 1 nodo | 40 × 50 = 2 000 | 18.114 s | ~9.1 ms |
| Clúster | 200 × 100 = 20 000 | 226.149 s | ~11.3 ms |

Es decir, `Ts (s) ≈ 0.010 × estaciones × ciclos` (con `carga=600`). Con eso
puedes estimar antes de lanzar una corrida grande, por ejemplo:

- Para que el **secuencial** tarde ~1 min: `estaciones × ciclos ≈ 6 000` (p. ej. 100×60 o 200×30).
- Para ~5 min (como el 200×100 que ya corriste, ~3.8 min): puedes llegar hasta ~300×100 o 200×150.
- Más allá de eso (p. ej. 500×200 = 100 000 station-ciclos) el secuencial ya tardaría **~17 min**; solo hazlo si lo vas a dejar corriendo en segundo plano, y baja `--carga` (p. ej. `--carga 200`) si quieres reducir el tiempo sin tocar la cantidad de datos procesados.

No hay límite de memoria relevante a estas escalas (decenas de miles de
mediciones ocupan unos pocos MB). El 200×100 en el clúster ya es un buen
punto "grande" para el informe; con el 8×20 (pequeño) y el 40×50 (mediano)
tienes 3 tamaños distintos, que es lo que pide la guía ("variar el tamaño del
problema").

---

## Métricas de rendimiento (fórmulas)

- **Ts** = tiempo secuencial (1 proceso, todas las estaciones).
- **Tp** = tiempo paralelo (el del proceso más lento, vía `reduce` con MAX).
- **Aceleramiento:** `S = Ts / Tp`.
- **Eficiencia:** `E = S / p`.

---

## Estructura

```
Practica-MPI/
├── main.py                 # Punto de entrada SPMD (consola o --gui)
├── nucleo/                 # Diseño orientado a objetos + lógica MPI
│   ├── dominio.py          # Variable, Medicion (con ciclo y proceso MPI), AlertaAmbiental
│   ├── estacion.py         # EstacionAmbiental (genera mediciones + carga CPU)
│   ├── analizador.py       # AnalizadorDatos: resumen local + consolidación global
│   ├── config.py           # Definición de estaciones de Cuenca
│   └── coordinador.py      # CoordinadorMPI (secuencial, paralelo, punto a punto, colectivas)
├── UserInterface/          # Interfaz gráfica (misma de la práctica de hilos/procesos)
│   └── app.py              # Ventana Tkinter en el rank 0
├── cluster/
│   ├── hosts.txt           # Hostfile del clúster
│   └── benchmark.sh        # Pruebas con 1, 2 y 4 procesos
└── README.md
```

