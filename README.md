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

```bash
# MPI (Debian/Ubuntu)
sudo apt install openmpi-bin libopenmpi-dev

# mpi4py
pip3 install --user mpi4py
```

---

## Ejecución

> Ejecutar siempre **desde la raíz** `Practica-MPI/` (para que se encuentre el
> paquete `nucleo`). Si tu Python con mpi4py se llama `python3.14`, usa ese
> nombre en lugar de `python3`.

### En una sola computadora (consola)

```bash
# Secuencial + paralelo (calcula el speedup automáticamente)
mpiexec -n 1 python3 main.py --estaciones 8 --ciclos 20
mpiexec -n 2 python3 main.py --estaciones 8 --ciclos 20
mpiexec -n 4 python3 main.py --estaciones 8 --ciclos 20

# Solo la parte paralela (sin baseline secuencial)
mpiexec -n 4 python3 main.py --estaciones 8 --ciclos 20 --solo-paralelo
```

### Con interfaz gráfica (GUI + MPI)

El proceso 0 abre la ventana Tkinter (la misma interfaz de la práctica de
hilos/procesos) y coordina; los demás procesos son trabajadores que calculan y le
envían resultados por MPI:

```bash
mpiexec -n 4 python3 main.py --gui --estaciones 8 --ciclos 20
```

La GUI muestra cada estación con el **proceso MPI** que la atiende, su última
medición, las estadísticas globales, las alertas y el entorno (Python, SO, nº de
procesos MPI, librería MPI). El botón **Iniciar** lanza la simulación distribuida
y la ventana se actualiza ciclo a ciclo.

Cada ejecución de consola imprime: el reparto de estaciones por proceso, las estadísticas
globales (idénticas sin importar el nº de procesos, lo que valida la
consolidación), y el bloque de rendimiento con **Ts, Tp, aceleramiento S y
eficiencia E**.

### En un clúster (varios nodos)

```bash
mpiexec -n 4 -hostfile cluster/hosts.txt python3 main.py --estaciones 8 --ciclos 20
```

Ver [cluster/hosts.txt](cluster/hosts.txt) para el formato del archivo de nodos.
Requisitos del clúster: **SSH sin contraseña** entre nodos, **misma ruta** del
proyecto en todos, y **mpi4py instalado** en cada nodo.

---

## Cómo montar el clúster (tu máquina = host + otra máquina)

Configuración usada (dos computadoras en la misma red por hotspot):

| Nodo | Usuario | Rol | IP |
|---|---|---|---|
| justin07 (mi máquina) | `justin` | host / coordinador | 172.20.10.5 |
| wimer-asustufgamingf15 | `wimer` | trabajador | 172.20.10.8 |

### 1. Red y SSH en el nodo trabajador

Ambas se ven en la red (`ping 172.20.10.8`). En la máquina de **wimer** hay que
instalar y encender el servidor SSH (si no, `ssh` da *Connection refused*):

```bash
sudo apt install -y openssh-server
sudo systemctl enable --now ssh
```

### 2. Instalar MPI en las dos máquinas

```bash
sudo apt install -y openmpi-bin libopenmpi-dev
```

### 3. SSH sin contraseña + usuarios distintos

Como los usuarios difieren (`justin` vs `wimer`), en el maestro se genera la llave,
se copia al trabajador y se mapea el usuario en `~/.ssh/config`:

```bash
# En el MAESTRO (justin):
ssh-keygen -t rsa -N "" -f ~/.ssh/id_rsa
ssh-copy-id wimer@172.20.10.8

# ~/.ssh/config  ->  para no tener que escribir el usuario cada vez
#   Host 172.20.10.8
#       User wimer
#       IdentityFile ~/.ssh/id_rsa

ssh 172.20.10.8 hostname      # debe entrar SIN contraseña
```

### 4. Código en la misma ruta absoluta en ambos nodos

Como los usuarios difieren, el home difiere; se usa una ruta común:

```bash
# En AMBAS máquinas:
sudo mkdir -p /opt/cluster && sudo chown -R $USER:$USER /opt/cluster
# Desde el maestro, copiar el proyecto al trabajador:
scp -r /opt/cluster/Practica-MPI 172.20.10.8:/opt/cluster/
```

### 5. Entorno de Python idéntico en ambos (venv)

Para evitar líos de PATH (Python gestionado por uv, dos `python3.14` distintos…),
se crea un entorno virtual en la **misma ruta** en cada máquina:

```bash
# En AMBAS máquinas (el venv se crea por separado en cada una):
python3.14 -m venv /opt/cluster/venv
/opt/cluster/venv/bin/pip install --upgrade pip mpi4py
```

### 6. Archivo de hosts

[cluster/hosts.txt](cluster/hosts.txt) con las IP reales y los *slots*:

```
172.20.10.5 slots=2
172.20.10.8 slots=2
```

### 7. Ejecutar en el clúster (desde el maestro)

```bash
cd /opt/cluster/Practica-MPI

# Prueba de que corre en las DOS máquinas (evidencia de clúster):
mpiexec -n 4 -hostfile cluster/hosts.txt hostname

# La práctica (con el python del venv, misma ruta en ambos):
mpiexec -n 4 -hostfile cluster/hosts.txt /opt/cluster/venv/bin/python main.py --estaciones 8 --ciclos 20
```

Si se **cuelga sin imprimir** (varias interfaces de red), fuerza la del hotspot:
`--mca btl_tcp_if_include 172.20.10.0/24`.

### Problemas reales encontrados (para el informe)

- **`ssh: Connection refused`** → el trabajador no tenía SSH; instalar/arrancar `openssh-server` (paso 1).
- **`scp: No such file / Permiso denegado`** → `/opt/cluster` era de root; hacer `chown` al usuario (paso 4).
- **`no such identity: ~/.ssh/id_rsa`** → faltaba generar la llave (paso 3).
- **`ModuleNotFoundError: mpi4py` solo en el trabajador** → el `python3.14` que MPI usa por SSH (no-interactivo) era distinto al que tenía mpi4py; se resolvió con el **venv en ruta común** (paso 5).
- **Contraseña de SSH en cada arranque** → faltaba `ssh-copy-id` o el `~/.ssh/config` con el usuario.
- **Se cuelga sin salida** → varias redes; usar `--mca btl_tcp_if_include`.

### Script de pruebas

```bash
bash cluster/benchmark.sh 8 20      # corre 1, 2 y 4 procesos automáticamente
# (usa PY=python3.14 bash cluster/benchmark.sh ... si tu python se llama así)
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

---

## Entregables

- [x] Código fuente (esta carpeta, POO organizada en paquetes).
- [x] README con instalación y ejecución.
- [x] Tabla de resultados experimentales (arriba).
- [x] Archivo de configuración de nodos → [cluster/hosts.txt](cluster/hosts.txt).
- [ ] Evidencia de ejecución en el clúster *(capturas de `mpiexec -hostfile`)*.
- [ ] Informe técnico en PDF.
