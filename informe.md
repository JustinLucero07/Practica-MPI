# Informe Técnico — Sistema de Monitoreo Ambiental Urbano con MPI

**Práctica:** Programación paralela con paso de mensajes (MPI)
**Estudiante:** Justin Lucero
**Repositorio:** Practica-MPI

---

## 1. Introducción

Este informe documenta el diseño, la implementación y la evaluación de rendimiento
de una aplicación paralela para un **sistema de monitoreo ambiental urbano**,
desarrollada con el modelo de **paso de mensajes (MPI)** usando `mpi4py`, y
ejecutada sobre un **clúster real de 3 nodos** (una máquina física y dos máquinas
virtuales conectadas en red).

La aplicación simula estaciones ambientales distribuidas en distintas zonas de
la ciudad de Cuenca, cada una generando mediciones de variables como
temperatura, humedad, ruido, CO₂, PM2.5 y PM10. El sistema procesa estas
mediciones, calcula estadísticas y genera alertas cuando los valores superan
umbrales definidos, todo ello distribuyendo el trabajo entre procesos MPI que
pueden ejecutarse en distintas computadoras.

---

## 2. Objetivos

- Aplicar el modelo de programación paralela basado en paso de mensajes.
- Diseñar una solución paralela con MPI sobre un clúster de computadoras.
- Distribuir tareas de procesamiento entre varios nodos/procesos.
- Implementar comunicación entre procesos mediante `mpi4py`.
- Evaluar el rendimiento de la solución secuencial frente a la paralela.
- Analizar el impacto de la distribución del trabajo, la comunicación y la
  sincronización en un entorno de memoria distribuida.

---

## 3. Caso de estudio

El sistema simula **estaciones ambientales** ubicadas en 12 zonas reales de
Cuenca (Centro Histórico, Totoracocha, Yanuncay, El Vecino, Monay, Machángara,
Baños, Ricaurte, Sayausí, El Batán, Tarqui, El Valle), y puede escalar a
cualquier número adicional de estaciones sintéticas ("Zona N") para pruebas de
carga. Cada estación mide un subconjunto de 6 variables ambientales:

| Variable | Unidad | Umbral de alerta |
|---|---|---|
| Temperatura | °C | 26.0 |
| Humedad | % | 90.0 |
| Ruido | dB | 75.0 |
| CO₂ | ppm | 1000.0 |
| PM2.5 | µg/m³ | 50.0 |
| PM10 | µg/m³ | 100.0 |

En cada **ciclo de simulación**, cada estación genera una medición por cada
variable que le corresponde (con una distribución gaussiana más picos
aleatorios que simulan eventos de contaminación/ruido). El sistema procesa
estas mediciones, calcula estadísticas locales y globales (promedio, mínimo,
máximo por variable), detecta alertas cuando el valor supera el umbral de su
variable, y consolida todo en un resultado global.

---

## 4. Diseño de la solución (POO)

### 4.1 Clases y responsabilidades

| Clase | Archivo | Responsabilidad |
|---|---|---|
| `Variable` (Enum) | `nucleo/dominio.py` | Representa una variable ambiental con su unidad y umbral de alerta. |
| `Medicion` | `nucleo/dominio.py` | Lectura individual: estación, zona, variable, valor, **ciclo de simulación** y **proceso MPI** que la generó. |
| `AlertaAmbiental` | `nucleo/dominio.py` | Se construye a partir de una `Medicion` en alerta; calcula su severidad (Baja/Media/Alta) según cuánto supera el umbral. |
| `EstacionAmbiental` | `nucleo/estacion.py` | Genera mediciones simuladas por ciclo y ejecuta el cálculo CPU-bound (`indice_ambiental`) sobre su historial. |
| `AnalizadorDatos` | `nucleo/analizador.py` | Calcula el **resumen local** de un proceso (`resumen_local`) y **consolida** los resúmenes de todos los procesos (`consolidar`). |
| `CoordinadorMPI` | `nucleo/coordinador.py` | Reparte las estaciones entre procesos, coordina toda la comunicación MPI (punto a punto y colectiva) y mide los tiempos de ejecución. |

Esta es una variación directa de las clases sugeridas por la guía
(Estación ambiental, Medición, Controlador/coordinador, Analizador de datos,
Alerta ambiental) — se mantienen los mismos nombres en español salvo
`CoordinadorMPI`, que se nombró así para dejar explícito que es el
coordinador **específico de la variante MPI** (existe también una versión no
distribuida del mismo dominio, en el paquete `monitoreo/`, reutilizada de una
práctica previa de hilos/procesos, para poder comparar las 4 arquitecturas).

### 4.2 Relación entre clases

```
CoordinadorMPI
   ├── usa → EstacionAmbiental   (genera mediciones por ciclo)
   ├── usa → AnalizadorDatos     (resumen_local / consolidar)
   └── coordina la comunicación MPI entre procesos

EstacionAmbiental
   ├── contiene → Variable[]     (qué variables mide esta estación)
   └── genera   → Medicion[]     (una por variable, cada ciclo)

Medicion
   └── puede transformarse en → AlertaAmbiental   (si supera el umbral)

AnalizadorDatos
   ├── resumen_local(mediciones, proceso)  -> dict (por proceso)
   └── consolidar(resumenes[])             -> dict (global, en el coordinador)
```

### 4.3 Justificación del diseño

- **`Medicion` es inmutable** (`@dataclass(frozen=True)`) porque representa un
  hecho ya ocurrido (una lectura de sensor); no debe modificarse después de
  creada, y así puede viajar de forma segura por MPI (se serializa con
  `pickle` internamente por `mpi4py`, y un objeto inmutable evita efectos
  secundarios inesperados al recibirlo en otro proceso).
- **Separar `resumen_local` de `consolidar`** en `AnalizadorDatos` refleja
  directamente el modelo de memoria distribuida: cada proceso solo puede
  calcular estadísticas de *sus propios datos* (memoria local); la
  consolidación global solo puede hacerse en el proceso que recibió todos los
  resúmenes (rank 0), después de la comunicación.
- **`CoordinadorMPI` no conoce el contenido de las mediciones**, solo orquesta
  reparto/comunicación/tiempo — separa la lógica de dominio (qué es una
  estación, qué es una alerta) de la lógica de paralelización (cómo se
  reparte y comunica el trabajo). Esto permite que el mismo dominio (`nucleo/`)
  se pruebe en modo secuencial sin ninguna dependencia de MPI.

### 4.4 ¿Por qué existen `nucleo/` **y** `monitoreo/`?

El repositorio tiene **dos** paquetes con un dominio muy parecido (`Variable`,
`Medicion`, `EstacionAmbiental`, `AnalizadorDatos`, `AlertaAmbiental`). No es
duplicación accidental: cada uno resuelve el problema con un **modelo de
concurrencia distinto**, y ambos se usan en el proyecto final.

| | `nucleo/` | `monitoreo/` |
|---|---|---|
| **Qué es** | La solución de **esta** práctica (la que se evalúa): secuencial de referencia + **MPI** sobre clúster | Código de una **práctica anterior** (hilos y procesos con `threading`/`multiprocessing`), reutilizado sin modificar |
| **Modelo de concurrencia** | Memoria **distribuida** — procesos que pueden estar en computadoras distintas, se comunican por mensajes (`send`/`recv`/`gather`/`reduce`/`bcast`) | Memoria **compartida** — hilos o procesos `fork` en la misma máquina, se comunican con `Queue`/`Barrier`/`Semaphore` de Python |
| **Coordinador** | `CoordinadorMPI` (`nucleo/coordinador.py`) | `ControladorMonitoreo` (`monitoreo/controlador.py`) |
| **`Medicion` incluye** | `ciclo` **y** `proceso` (rank MPI que la generó) — necesario para saber qué nodo distribuido la produjo | Solo la medición — no existe "rank" en hilos/procesos, todos comparten el mismo proceso Python (o procesos `fork` sin necesidad de identificarse) |
| **Clases extra** | Ninguna | `EstadoEstacion`, `ModoEjecucion`, `SnapshotMonitoreo`, `VistaEstacion`, `Estadisticas` — necesarias para reportar el **estado en vivo** de cada estación (Esperando/Procesando/Finalizada) a la GUI, algo que MPI no expone tan fácilmente entre nodos remotos |
| **Para qué se usa en el proyecto** | Modo `mpi` (por defecto) en `main.py` y en la GUI | Modos `secuencial`, `hilos` y `procesos` en `main.py` y en la GUI |

**Motivo de mantener ambos:** la rúbrica de esta práctica solo exige
secuencial + MPI (eso vive enteramente en `nucleo/`). El paquete `monitoreo/`
se conserva **sin tocar** únicamente para poder comparar 4 arquitecturas de
paralelismo (secuencial, hilos, procesos, MPI) con el mismo caso de estudio —
ver la sección 9.3 — sin arriesgar el código ya probado de la práctica
anterior fusionándolo con el nuevo código MPI.

---

## 5. Estrategia de paralelización

### 5.1 Modelo SPMD

Todos los procesos ejecutan el mismo programa (`main.py`). Al arrancar, cada
proceso consulta su `rank`:

- **`rank == 0`** → actúa como **proceso coordinador**: reparte el trabajo,
  agrega resultados, mide tiempos e imprime el reporte final.
- **`rank != 0`** → actúan como **procesos trabajadores**: reciben su lista de
  estaciones asignadas, generan y procesan sus mediciones, y envían su
  resumen local al coordinador.

### 5.2 División de datos

Las **estaciones** son la unidad de distribución de trabajo (no las
mediciones individuales ni los ciclos). Se reparten con un esquema
**round-robin**: la estación `i` se asigna al proceso `i % size`
(`CoordinadorMPI.repartir`, `nucleo/coordinador.py`). Cada proceso:

- Recibe una lista de **índices de estación** distintos a la de los demás.
- Crea **sus propias instancias** de `EstacionAmbiental` (memoria local, no
  compartida).
- Ejecuta **todos los ciclos** para **sus** estaciones, sin comunicarse con
  otros procesos durante el cómputo (aislamiento total durante el cálculo,
  que es lo que permite el paralelismo real).

Esto garantiza que **nunca todos los procesos hacen el mismo trabajo**: con 6
procesos y 1000 estaciones, cada uno procesa ~167 estaciones distintas (ver
la tabla de reparto real en la sección 9).

### 5.3 Comunicación entre procesos

| Momento | Operación | Tipo |
|---|---|---|
| Reparto de estaciones (coordinador → cada trabajador) | `comm.send` / `comm.recv` | **Punto a punto** |
| Sincronización antes de medir tiempo paralelo | `comm.Barrier()` | Colectiva (sincronización) |
| Recolección de resúmenes locales (todos → coordinador) | `comm.gather()` | **Colectiva** |
| Total de mediciones procesadas | `comm.reduce(op=MPI.SUM)` | **Colectiva** |
| Tiempo del proceso más lento (Tp) | `comm.reduce(op=MPI.MAX)` | **Colectiva** |
| Comando de inicio / terminación (modo GUI) | `comm.bcast()` | **Colectiva** |

### 5.4 Consolidación de resultados

Cada proceso calcula su `resumen_local` (sumas, mín/máx por variable, alertas
por zona — ver `AnalizadorDatos.resumen_local`). El coordinador reúne todos
los resúmenes con `gather` y los combina en `AnalizadorDatos.consolidar`,
sumando conteos, tomando mínimos/máximos globales y recalculando promedios.
El resultado es **idéntico sin importar cuántos procesos se usen** — esto se
verificó en todas las corridas (las estadísticas por variable son las mismas
con 1, 2, 4 o 6 procesos), lo que confirma que la distribución y la
consolidación son correctas.

### 5.5 Qué método se ejecuta realmente para cada modo (GUI y consola)

Es importante distinguir esto porque **"Secuencial" no siempre es el mismo
código**: hay dos implementaciones de "secuencial" en el proyecto, una en
cada paquete, y cada punto de entrada (GUI o consola) usa una u otra.

#### Tabla resumen

| Modo elegido | Paquete que se ejecuta | Método exacto | ¿Necesita `mpiexec`? | Concurrencia real |
|---|---|---|---|---|
| Secuencial | `monitoreo/` | `ControladorMonitoreo.ejecutar_secuencial` (`monitoreo/controlador.py:181`) | No | Ninguna — un bucle simple, un solo hilo |
| Hilos | `monitoreo/` | `ControladorMonitoreo.ejecutar_hilos` (`monitoreo/controlador.py:202`) | No | `threading.Thread` (memoria compartida, limitada por el GIL salvo build free-threaded) |
| Procesos | `monitoreo/` | `ControladorMonitoreo.ejecutar_procesos` (`monitoreo/controlador.py:254`) | No | `multiprocessing.Process` (memoria compartida vía `fork`, un proceso por grupo de estaciones) |
| MPI | `nucleo/` | `CoordinadorMPI.ejecutar_secuencial` + `CoordinadorMPI.ejecutar_paralelo` (`nucleo/coordinador.py`) | **Sí**, siempre — incluso para medir Ts | Memoria **distribuida**, procesos MPI reales (pueden estar en otra computadora) |

La fila de MPI necesita `mpiexec` incluso para calcular Ts porque
`ejecutar_secuencial` de `nucleo/coordinador.py` también corre **dentro** del
mismo trabajo MPI (solo lo ejecuta el rank 0, los demás esperan en el
`Barrier`) — es la única forma de garantizar que Ts y Tp se midan en las
mismas condiciones de sistema, en la misma corrida.

#### Flujo en la consola (`main.py`)

```
main.py
 └── main()                                    [main.py:105]
      ├── --modo mpi (por defecto)
      │    └── _consola()                      [main.py:6]
      │         ├── CoordinadorMPI.ejecutar_secuencial()   ← nucleo/
      │         └── CoordinadorMPI.ejecutar_paralelo()     ← nucleo/  (send/recv, gather, reduce)
      │
      └── --modo secuencial | hilos | procesos
           └── _consola_local(modo, ...)        [main.py:74]
                └── ControladorMonitoreo(...).ejecutar(ModoEjecucion(modo))   ← monitoreo/
                     ├── if modo == Secuencial → ejecutar_secuencial()
                     ├── if modo == Hilos      → ejecutar_hilos()
                     └── if modo == Procesos   → ejecutar_procesos()
```

`_consola_local` (`main.py:74-102`) es un único método que sirve para los 3
modos no-MPI: crea un `ControladorMonitoreo` y le delega a
`ControladorMonitoreo.ejecutar(...)`, que internamente decide con un
`if/elif` cuál de los 3 métodos (`ejecutar_secuencial`/`ejecutar_hilos`/
`ejecutar_procesos`) llamar, todos definidos en `monitoreo/controlador.py`
(ver `ejecutar` en la línea 323 de ese archivo).

#### Flujo en la interfaz gráfica (GUI, `UserInterface/app.py`)

Al hacer clic en **"Iniciar"** se ejecuta `MonitoreoGUI._iniciar`
(`app.py:264`), que decide según el combo "Modo":

```
_iniciar()                                      [app.py:264]
 ├── modo == "MPI"
 │    └── Thread → _trabajo_mpi(n_est, ciclos, np_mpi)     [app.py:295]
 │         └── subprocess.Popen(["mpiexec", "-n", np_mpi, ...,
 │                                python3.14t, "-m", "nucleo.mpi_runner", ...])
 │              (proceso HIJO, separado de la GUI, usa nucleo/)
 │
 └── modo in {"Secuencial", "Hilos", "Procesos"}
      ├── self._controlador = ControladorMonitoreo(...)     ← monitoreo/
      └── Thread → _trabajo_local(modo)                     [app.py:292]
           └── self._controlador.ejecutar(ModoEjecucion(modo))
                (dentro del MISMO proceso de la GUI, solo un hilo aparte)
```

**La diferencia clave:** para Secuencial/Hilos/Procesos, la GUI crea el
`ControladorMonitoreo` **en su propio proceso** y solo delega el trabajo a un
hilo (`threading.Thread`) para no congelar la ventana — la concurrencia real
(si la hay) ocurre **dentro** de ese método (`ejecutar_hilos` crea sus
propios hilos, `ejecutar_procesos` sus propios `multiprocessing.Process`).
Para MPI, en cambio, la GUI **no** ejecuta nada de MPI directamente: lanza un
**proceso hijo completamente aparte** (`mpiexec ...`) con `subprocess.Popen`,
y se limita a leer su salida.

Esto explica por qué **solo `_trabajo_mpi` "sabe" de MPI** en toda la GUI: es
el único método que construye una línea de `mpiexec`. El resto de la interfaz
(`_sondear` en `app.py:326`, `_aplicar`) es agnóstico — solo consume
diccionarios (`SnapshotMonitoreo` convertido a `dict`, o JSON parseado de la
salida del subproceso MPI) sin importarle si vinieron de hilos, procesos o
MPI.

#### Cómo se lee el resultado en vivo en cada caso

- **Secuencial/Hilos/Procesos:** `ControladorMonitoreo.ejecutar(...)` recibe
  un callback `publicar` que se llama después de cada ciclo con un
  `SnapshotMonitoreo` (objeto Python, en memoria, sin serializar) — la GUI lo
  convierte a `dict` (`_snapshot_a_dict`) y lo mete en la cola.
- **MPI:** no hay ningún objeto Python compartido entre la GUI y el
  subproceso MPI — la única comunicación es el **texto** que
  `nucleo/mpi_runner.py` imprime por `stdout` en formato JSON
  (`print(json.dumps(d), flush=True)`), una línea por ciclo. La GUI lee esas
  líneas y hace `json.loads(...)`. Es, en la práctica, el mismo patrón de
  "paso de mensajes" que MPI usa entre procesos, aplicado aquí entre el
  proceso de la GUI y el proceso `mpiexec` (aunque esta comunicación
  GUI↔mpiexec es por `stdout`/`pipe`, no por `mpi4py`).

---

## 6. Implementación con MPI

### 6.1 Relación con el *Python Parallel Programming Cookbook* (Capítulo 4)

Como referencia de las operaciones MPI disponibles en `mpi4py`, se usó el
Capítulo 4 del repositorio
[Python-Parallel-Programming-Cookbook-Second-Edition](https://github.com/PacktPublishing/Python-Parallel-Programming-Cookbook-Second-Edition/tree/master/Chapter04),
que contiene ejemplos independientes de cada primitiva de comunicación:
`helloworld_MPI.py`, `pointToPointCommunication.py`, `broadcast.py`,
`scatter.py`, `gather.py`, `alltoall.py`, `reduction.py`,
`deadLockProblems.py` y `virtualTopology.py`.

Este proyecto **no copia esos scripts**, pero sí aplica las mismas primitivas
que enseñan, adaptadas al problema real de monitoreo ambiental. La
correspondencia es:

| Ejemplo del Cookbook | Primitiva | Dónde se usa en este proyecto |
|---|---|---|
| `pointToPointCommunication.py` | `comm.send()` / `comm.recv()` | `CoordinadorMPI.ejecutar_paralelo` — el coordinador (rank 0) le envía a **cada** trabajador su lista propia de estaciones asignadas. |
| `broadcast.py` | `comm.bcast()` | `CoordinadorMPI.ejecutar_para_gui` / `terminar()` — el coordinador transmite a **todos** el comando de inicio (`n_est, ciclos`) o la señal de fin (`CMD_TERMINAR`) en el modo GUI. |
| `gather.py` | `comm.gather()` | Recolecta en rank 0 el resumen local de **todos** los procesos en una sola llamada. |
| `reduction.py` | `comm.reduce()` (SUM, MAX) | Suma el total de mediciones procesadas (SUM) y calcula el tiempo del proceso más lento (MAX) — esta última es clave porque Tp de la práctica **debe** ser el tiempo del proceso que más tardó, no el del coordinador. |
| `helloworld_MPI.py` | `Get_rank()` / `Get_size()` | Base del modelo SPMD: todo el reparto de roles (coordinador/trabajador) depende de leer `rank` y `size` al inicio. |

### 6.2 Por qué no se usan `Scatter`, `AllToAll` ni topologías virtuales

- **`Scatter`/`Scatterv`** reparten un buffer contiguo en trozos — funcionan
  mejor cuando todos los trozos tienen el mismo tamaño o cuando los datos ya
  están en un array plano. Aquí cada proceso recibe una **lista de índices de
  estación de tamaño variable** (con `n_est % size != 0` los últimos procesos
  reciben una estación menos), y además el reparto ocurre una sola vez al
  inicio de la corrida, no en un bucle de datos masivos — un `send`/`recv`
  explícito por trabajador es más simple y igual de eficiente para este caso,
  y dejaba clara la comunicación **dirigida** (un remitente, un destinatario)
  que pide la rúbrica.
- **`Alltoall`** intercambia datos entre **todos los pares** de procesos; este
  problema tiene una topología de **estrella** (coordinador ↔ cada
  trabajador), no hay comunicación *trabajador-a-trabajador*, así que
  `Alltoall` no aplica.
- **Topologías virtuales** (`virtualTopology.py`, cartesianas/de grafo) sirven
  para problemas con relaciones espaciales entre procesos (p. ej. simulación
  de una malla física donde el proceso vecino importa). Aquí la relación es
  simplemente "coordinador reparte, trabajadores calculan, coordinador
  reúne" — no hay vecindad relevante entre estaciones que justifique una
  topología.

### 6.3 Prevención de deadlocks

`deadLockProblems.py` en el Cookbook muestra cómo dos `send`/`recv` bloqueantes
cruzados sin orden pueden colgar el programa. Aquí se evita por diseño:

- El coordinador **siempre envía primero** (`comm.send`) y **luego** hace
  su propio trabajo; los trabajadores **solo esperan** (`comm.recv`) al
  inicio. No hay ciclos de espera mutua.
- Antes de medir el tiempo paralelo se usa `comm.Barrier()`, que sincroniza a
  todos los procesos en un punto conocido, evitando que un proceso empiece a
  medir tiempo mientras otro todavía no recibió su asignación.
- En el modo GUI, el `bcast()` de terminación (`CMD_TERMINAR`) es la única
  forma de que los trabajadores salgan de su bucle infinito
  (`bucle_trabajador`), evitando que queden procesos MPI colgados en segundo
  plano tras cerrar la ventana.

---

## 7. Configuración del clúster

### 7.1 Topología

3 nodos sobre una red **host-only de VirtualBox** (`192.168.56.0/24`,
interfaz `vboxnet0`):

| Nodo | Hostname | Usuario | Rol | IP | Slots |
|---|---|---|---|---|---|
| Anfitrión | `justin07` | `justin` | maestro / coordinador (rank 0) | `192.168.56.1` | 2 |
| Máquina virtual 1 | `mpi1` | `mpi` | trabajador | `192.168.56.101` | 2 |
| Máquina virtual 2 | `mpi2` | `mpi` | trabajador | `192.168.56.102` | 2 |

Intérprete usado en las 3 máquinas: `python3.14t` (build free-threaded de
Python 3.14), instalado en la **misma ruta absoluta**
(`/usr/local/bin/python3.14t`) en los 3 nodos, con `mpi4py` instalado para
ese mismo intérprete. El proyecto vive en `/opt/Practica-MPI` en las 3
máquinas.

### 7.2 Pasos de configuración (resumen)

1. Adaptador host-only en las 2 VMs + servidor SSH activo (`openssh-server`).
2. Open MPI (`openmpi-bin`, `libopenmpi-dev`) instalado en las 3 máquinas.
3. SSH sin contraseña desde el maestro hacia `mpi@192.168.56.101` y
   `mpi@192.168.56.102` (`ssh-keygen` + `ssh-copy-id` + `~/.ssh/config`).
4. Mismo intérprete (`python3.14t`) en la misma ruta en las 3 máquinas, con
   `mpi4py` instalado en cada una.
5. Mismo código fuente en la misma ruta absoluta (`/opt/Practica-MPI`) en las
   3 máquinas (`scp -r`).
6. `cluster/hosts.txt` con las 3 IPs y `slots=2` cada una (6 procesos en
   total disponibles).
7. Ejecución desde el maestro con `mpiexec -hostfile cluster/hosts.txt --mca
   btl_tcp_if_include 192.168.56.0/24 ...` (ver detalle completo con todos
   los comandos en [README.md](README.md)).

### 7.3 Problemas reales encontrados

| Problema | Causa | Solución |
|---|---|---|
| `ssh: Connection refused` | Las VMs no tenían servidor SSH | Instalar y arrancar `openssh-server` en cada VM |
| `scp: Permiso denegado` | `/opt/Practica-MPI` era propiedad de `root` en las VMs | `chown mpi:mpi /opt/Practica-MPI` antes de copiar el proyecto |
| `no such identity: ~/.ssh/id_rsa` | No se había generado la llave SSH en el maestro | `ssh-keygen -t rsa -N "" -f ~/.ssh/id_rsa` |
| `ModuleNotFoundError: mpi4py` solo en las VMs | El intérprete que MPI invoca por SSH (no interactivo) no era el mismo que tenía `mpi4py` instalado (mezcla de `python3`/`python3.14` de distintos orígenes) | Instalar `python3.14t` en la **misma ruta absoluta** en las 3 máquinas e instalar `mpi4py` específicamente con ese binario |
| Pedía contraseña de SSH en cada ejecución | Faltaba `ssh-copy-id` o el `~/.ssh/config` con el usuario correcto | Configurar clave pública + `~/.ssh/config` con `User mpi` para cada IP |
| El programa se colgaba sin imprimir nada | El anfitrión tiene varias interfaces de red (WiFi, puentes de Docker, `vboxnet0`) y Open MPI intentaba usar la incorrecta | Forzar la red del clúster con `--mca btl_tcp_if_include 192.168.56.0/24` |

---

## 8. Pruebas realizadas

### 8.1 Variación del número de procesos MPI

Se probó con **1, 2, 4 y 6** procesos (los 3 mínimos que pide la guía, más 6
como máximo disponible en el clúster real con `slots=2` en 3 nodos):

- 1 nodo (`justin07`, 20 núcleos): p = 1, 2, 4.
- Clúster real (3 nodos): p = 6 (2 por nodo, usando los 3 nodos a la vez).

### 8.2 Variación del tamaño del problema

Se justifica probar con tamaños chico, mediano y grande para observar cómo
cambia la eficiencia según cuánto trabajo real hay por proceso frente al
costo fijo de comunicación:

| Tamaño | Estaciones × Ciclos | Mediciones | Dónde se corrió |
|---|---|---|---|
| Chico | 8 × 20 | 680 | 1 nodo y clúster (3 nodos) |
| Chico-mediano | 12 × 30 | 1 560 | 1 nodo |
| Mediano | 40 × 50 | 8 200 | 1 nodo (comparación de 4 modos) |
| Grande | 1000 × 20 | 80 080 | 1 nodo (comparación de 4 modos) |
| Grande | 200 × 100 | 80 400 | Clúster (3 nodos) |

El parámetro `--carga` (repeticiones del cálculo `indice_ambiental` por
estación-ciclo) se mantuvo fijo en 600 en todas las corridas para que la
comparación de tiempos sea válida (representa el costo de CPU del análisis,
constante en todo el estudio).

---

## 9. Resultados experimentales

### 9.1 MPI en 1 sola máquina (`justin07`, 20 núcleos, Open MPI 4.1.6)

**8 estaciones × 20 ciclos (680 mediciones):**

| Procesos (p) | Tp | S = Ts/Tp | Eficiencia E = S/p |
|---|---|---|---|
| 1 | 0.969 s | x1.01 | 101 % |
| 2 | 0.490 s | x2.01 | 100 % |
| 4 | 0.275 s | x3.66 | 91 % |

(Ts ≈ 0.98 s)

**12 estaciones × 30 ciclos (1 560 mediciones):**

| Procesos (p) | Tp | S = Ts/Tp | Eficiencia E = S/p |
|---|---|---|---|
| 1 | 2.564 s | x1.01 | 100 % |
| 2 | 1.299 s | x2.00 | 99 % |
| 4 | 0.726 s | x3.67 | 91 % |

(Ts ≈ 2.6 s)

### 9.2 MPI en el clúster real (3 nodos, 6 procesos)

| Estaciones × ciclos | Mediciones | Ts | Tp | S = Ts/Tp | E = S/p |
|---|---|---|---|---|---|
| 8 × 20 | 680 | 1.294 s | 0.350 s | x3.70 | 61.66 % |
| 200 × 100 | 80 400 | 226.149 s | 49.102 s | x4.61 | 76.76 % |

Distribución real del reparto para 8×20 con 6 procesos:
`[2, 2, 1, 1, 1, 1]` estaciones por proceso (round-robin sobre 8 estaciones y
6 procesos).

### 9.3 Comparación de las 4 arquitecturas de paralelismo (secuencial, hilos, procesos, MPI)

**40 estaciones × 50 ciclos, carga 600 (8 200 mediciones) — 1 nodo:**

| Modo | Trabajadores | Tiempo | Speedup vs. secuencial |
|---|---|---|---|
| Secuencial | 1 | 18.114 s | x1.00 |
| Hilos | 20 (auto) | 2.582 s | x7.02 |
| Procesos | 20 (auto) | 2.925 s | x6.19 |
| MPI | 4 (`-n 4`) | 5.225 s | x3.80 (Ts propio = 19.838 s, E = 94.91 %) |

**1000 estaciones × 20 ciclos, carga 600 (80 080 mediciones) — 1 nodo:**

| Modo | Trabajadores | Tiempo | Speedup vs. secuencial |
|---|---|---|---|
| Secuencial | 1 | 130.636 s | x1.00 |
| Hilos | 20 (auto) | 18.775 s | x6.96 |
| Procesos | 20 (auto) | 24.436 s | x5.35 |
| MPI | 6 (`-n 6`) | 29.325 s | x4.46 (Ts propio = 153.045 s, S = x5.22, E = 86.98 %) |

En ambos casos las mediciones procesadas, alertas generadas y estadísticas
por variable son **idénticas** entre los 4 modos — confirma que los 4
resuelven el mismo problema correctamente, solo cambia cómo distribuyen el
cómputo.

---

## 10. Análisis de rendimiento

### 10.1 Fórmulas utilizadas

- **Aceleramiento:** `S = Ts / Tp`
- **Eficiencia:** `E = S / p`

Donde `Ts` es el tiempo secuencial (1 proceso, todas las estaciones) y `Tp`
es el tiempo paralelo — específicamente, el del **proceso más lento**,
obtenido con `comm.reduce(..., op=MPI.MAX)`, que es la medida correcta de
cuánto tardó realmente la corrida paralela completa (no el promedio ni el
tiempo del coordinador).

### 10.2 Aceleramiento casi lineal con pocos procesos

En 1 nodo, con problemas chicos (8×20, 12×30), el aceleramiento es casi
perfecto hasta 2 procesos (x2.00–x2.01, eficiencia ~100 %) y se mantiene alto
con 4 (x3.66–x3.67, eficiencia ~91 %). La pequeña caída de eficiencia con más
procesos se debe al **costo fijo de comunicación** (reparto + `gather`): con
más procesos, cada uno hace menos trabajo, así que el costo fijo pesa
relativamente más.

### 10.3 Comunicación por red vs. memoria compartida

Comparando el mismo tamaño de problema (8×20) en 1 nodo (4 procesos,
E=91 %) contra el clúster real (6 procesos en 3 máquinas, E=61.66 %), la
eficiencia cae notablemente al pasar a red física. Esto es esperado: en 1
nodo, `send`/`gather` viajan por memoria compartida (extremadamente rápido);
en el clúster, viajan por Ethernet virtual entre 3 sistemas operativos
distintos — el costo de comunicación es mucho mayor.

Sin embargo, al **aumentar el tamaño del problema en el mismo clúster**
(200×100 en vez de 8×20), la eficiencia **sube** de 61.66 % a 76.76 %. El
costo de comunicación por corrida es aproximadamente fijo (una asignación +
un `gather` final), así que cuando hay más cómputo real por proceso, ese
costo fijo pesa proporcionalmente menos. Esta es la razón principal por la
que **la práctica pide variar el tamaño del problema**: un clúster real solo
demuestra su valor cuando el trabajo por nodo es lo bastante grande para
justificar el costo de la red.

### 10.4 Comparación entre arquitecturas de paralelismo

Hilos y Procesos (con hasta 20 trabajadores en esta máquina) logran mayor
speedup absoluto que MPI con 4–6 procesos (x6–x7 contra x3.8–x5.2) — pero
**no es una comparación justa de arquitecturas**, porque usan más
trabajadores. Lo que sí es comparable es la **eficiencia por trabajador**:
MPI con 6 procesos alcanza 86.98 % de eficiencia (1000×20), superior a la de
Hilos o Procesos si se normalizara por sus 20 trabajadores. MPI es, además,
la **única** de las 4 arquitecturas capaz de escalar más allá de una sola
máquina — Hilos y Procesos están limitados a los núcleos de un único
computador (GIL/`fork` locales), mientras que MPI ya demostró escalar sobre
3 nodos físicos/virtuales distintos.

### 10.5 El tamaño del problema no es solo "estaciones × ciclos"

Un detalle observado al comparar 200×100 (Ts=226.149 s) contra 1000×20
(Ts≈130–153 s) — ambos casos con el **mismo total** de 20 000
"estación-ciclos" — es que el tiempo secuencial **no** es igual. La razón es
que `indice_ambiental` recalcula sobre una **ventana móvil** de historial
(hasta 60 valores) que crece con los ciclos: con más ciclos por estación
(100), la ventana pasa más tiempo llena (cerca de 60 valores) que con pocos
ciclos (20), donde la ventana rara vez llega a su tamaño máximo. Esto hace
que el costo de CPU por estación-ciclo dependa también de **cuántos ciclos**
tiene cada estación, no solo del total de mediciones — un matiz importante
al justificar los tamaños de prueba elegidos.

---

## 11. Conclusiones

1. El modelo SPMD con reparto round-robin de estaciones resultó simple y
   efectivo: garantiza que cada proceso reciba trabajo distinto (nunca
   duplicado) y que la distribución sea automáticamente pareja incluso al
   variar el número de procesos.
2. Las operaciones **punto a punto** (`send`/`recv`) y **colectivas**
   (`gather`, `reduce`, `bcast`) usadas cubren, con justificación clara, el
   mínimo pedido por la práctica, siguiendo los mismos patrones que enseña
   el Capítulo 4 del *Python Parallel Programming Cookbook*.
3. El aceleramiento y la eficiencia medidos son consistentes con la teoría:
   casi lineales con pocos procesos y problemas chicos, y con eficiencia
   creciente al aumentar el tamaño del problema frente al costo fijo de
   comunicación — efecto que se hizo más visible en el clúster real (red
   física) que en 1 sola máquina (memoria compartida).
4. Ejecutar en un clúster real de 3 nodos (1 físico + 2 VMs) introdujo
   problemas típicos de sistemas distribuidos (SSH, permisos, intérpretes de
   Python distintos entre máquinas, interfaces de red múltiples) que no
   aparecen al simular "varios procesos en una sola computadora", y que se
   documentaron junto con su solución.
5. Comparar MPI contra Hilos/Procesos mostró que, para justicia en la
   comparación, hay que igualar el número de trabajadores — de lo contrario
   se compara la arquitectura equivocada. Aun así, MPI demostró una ventaja
   que ninguna de las otras dos tiene: la capacidad de escalar más allá de
   los núcleos de una sola máquina.

---

## 12. Anexos

### 12.1 Estructura del proyecto

```
Practica-MPI/
├── main.py                 # Punto de entrada SPMD (--modo secuencial|hilos|procesos|mpi, o --gui)
├── nucleo/                 # Diseño POO + lógica MPI
│   ├── dominio.py          # Variable, Medicion (con ciclo y proceso MPI), AlertaAmbiental
│   ├── estacion.py         # EstacionAmbiental
│   ├── analizador.py       # AnalizadorDatos: resumen local + consolidación global
│   ├── config.py           # Definición de estaciones de Cuenca
│   ├── coordinador.py      # CoordinadorMPI (secuencial, paralelo, punto a punto, colectivas)
│   └── mpi_runner.py       # Backend MPI usado por la GUI
├── monitoreo/               # Versión no distribuida (secuencial/hilos/procesos) para comparar arquitecturas
├── UserInterface/           # Interfaz gráfica Tkinter (rank 0)
├── cluster/
│   ├── hosts.txt            # Hostfile del clúster
│   └── benchmark.sh         # Script de pruebas con 1, 2 y 4 procesos
└── README.md                 # Guía de instalación, ejecución y configuración del clúster
```

### 12.2 Comandos de referencia

```bash
# Comparar los 4 modos con el mismo tamaño de problema (sin MPI, 1 proceso python):
python3.14t main.py --modo secuencial --estaciones 1000 --ciclos 20 --carga 600
python3.14t main.py --modo hilos       --estaciones 1000 --ciclos 20 --carga 600
python3.14t main.py --modo procesos    --estaciones 1000 --ciclos 20 --carga 600

# MPI en 1 sola máquina:
mpiexec -n 6 python3.14t main.py --estaciones 1000 --ciclos 20 --carga 600

# MPI en el clúster real (3 nodos, 6 procesos):
mpiexec -n 6 -hostfile cluster/hosts.txt --mca btl_tcp_if_include 192.168.56.0/24 \
  python3.14t main.py --estaciones 200 --ciclos 100 --carga 600

# GUI con backend MPI:
mpiexec -n 4 python3.14t main.py --gui --estaciones 8 --ciclos 20
```

Ver [README.md](README.md) para la guía completa de instalación, configuración
del clúster paso a paso y más ejemplos de ejecución.
