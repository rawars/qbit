# Incidentes y límites conocidos

Este directorio conserva fallos reproducibles encontrados durante pruebas de
carga. Cada documento separa los hechos observados de las hipótesis pendientes
de confirmar.

## Corregidos, pendientes de prueba de aceptación completa

- [Pool Redis compartido bloquea al publicador durante una hora real](2026-09-14-shared-redis-pool-starves-publisher.md)
  — el código ya aísla los pools y la regresión con 250 slots pasó. Falta repetir
  el escenario original de 2.050.000 mensajes durante una hora.

- [Un error transitorio de Redis detenía la réplica completa](2026-09-14-transient-redis-errors-stop-worker.md)
  — el worker ahora recupera red, pool y failover con backoff sin retirar los
  demás slots. Pasaron las regresiones con fallos inyectados; falta repetir la
  carga Fazpi completa.

- [Error de publicación oculto como `STOPPED`](2026-09-14-publisher-error-hidden-as-stopped.md)
  — el laboratorio conserva el primer error y termina en `FAILED`. Falta
  comprobar visualmente el caso durante la carga completa.

## Incidentes resueltos

- [Reservas vencidas mostradas como activas y scrapes costosos](2026-09-14-expired-reservations-and-expensive-stats.md)
  — `active` cuenta únicamente leases vigentes, las reservas vencidas se
  exponen por separado y las tasas usan agregados acotados en vez de recorrer
  hasta 10.000 eventos por scrape.

- [Reservation lost durante una prueba extrema de Fazpi](2026-09-14-reservation-lost-under-load.md)
  — resuelto con finalizaciones terminales idempotentes y aislamiento de la
  pérdida de reserva para que no detenga la réplica completa.
