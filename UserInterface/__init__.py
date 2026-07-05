"""Interfaz gráfica (Tkinter) del sistema de monitoreo ambiental — versión MPI.

Reutiliza el diseño visual de la práctica de hilos/procesos, adaptado para
mostrar el proceso MPI que atiende cada estación. La ventana vive en el rank 0;
los demás procesos calculan y envían resultados por MPI.
"""
