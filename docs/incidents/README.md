# Incidentes y límites conocidos

Este directorio conserva fallos reproducibles encontrados durante pruebas de
carga. Cada documento separa los hechos observados de las hipótesis pendientes
de confirmar.

## Corregidos, pendientes de prueba de aceptación completa

- [Pool Redis compartido bloquea al publicador durante una hora real](2026-09-14-shared-redis-pool-starves-publisher.md)
  — el código ya aísla los pools y la regresión con 250 slots pasó. Falta repetir
  el escenario original de 2.050.000 mensajes durante una hora.

## Incidentes resueltos

- [Reservation lost durante una prueba extrema de Fazpi](2026-09-14-reservation-lost-under-load.md)
  — resuelto con finalizaciones terminales idempotentes y aislamiento de la
  pérdida de reserva para que no detenga la réplica completa.
