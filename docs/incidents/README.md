# Incidentes y límites conocidos

Este directorio conserva fallos reproducibles encontrados durante pruebas de
carga. Cada documento separa los hechos observados de las hipótesis pendientes
de confirmar.

## Incidentes resueltos

- [Reservation lost durante una prueba extrema de Fazpi](2026-09-14-reservation-lost-under-load.md)
  — resuelto con finalizaciones terminales idempotentes y aislamiento de la
  pérdida de reserva para que no detenga la réplica completa.
