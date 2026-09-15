# `reservation lost` durante una prueba extrema de Fazpi

- **Fecha:** 2026-09-14
- **Estado:** resuelto en código y validado con pruebas de integración
- **Severidad histórica:** alta para preparación de producción
- **Entorno:** laboratorio local con Qbit y Redis en Docker

## Resumen

Una simulación comprimió una hora de tráfico de Fazpi en 60 segundos. Qbit
publicó los 503.500 mensajes, pero luego uno de los workers informó:

```text
worker replica 4: qbit: complete job
"run-dlf6l03u5i0s-p00001-t000057162-m000004": qbit: reservation lost
```

El laboratorio marcó la ejecución como `FAILED` y apagó sus workers. En ese
momento todavía quedaban 127.966 mensajes esperando, por lo que el backlog dejó
de drenar.

Esto **no demuestra que el mensaje indicado se haya perdido**. La inspección
posterior confirmó que ese job estaba en estado `completed` en Redis. El fallo
es una confirmación ambigua: el trabajo terminó, pero el worker no pudo
confirmar de forma segura que su operación de finalización había sido aceptada.

## Analogía con Fazpi

Un worker es como un asesor que toma temporalmente una conversación de la fila.
Redis le entrega una ficha exclusiva, llamada **reservation token**, que prueba
que ese asesor sigue siendo el dueño de ese mensaje.

Al terminar, el asesor entrega la respuesta junto con la ficha. Redis registra
el trabajo como completado y devuelve un `ACK`, equivalente a decir “recibido y
archivado”. En este incidente, Redis alcanzó a registrar el resultado, pero la
respuesta tardó demasiado o no llegó claramente al worker. Cuando el worker
intentó verificar o repetir la operación, la ficha ya no existía porque el job
ya había terminado. Qbit respondió `reservation lost`.

La protección de la ficha es correcta: evita que un worker atrasado confirme el
trabajo de otro. La implementación anterior no podía distinguir entre:

1. “perdí la reserva antes de completar el trabajo”; y
2. “sí lo completé, pero perdí o excedí el tiempo de espera del `ACK`”.

Además, el worker administrado trataba ese caso aislado como fatal y detenía la
réplica completa. Por eso miles de mensajes quedaron esperando aunque Redis
seguía disponible.

## Escenario reproducido

| Dato | Valor |
|---|---:|
| Cola | `fazpi-sim-112507` |
| Ejecución | `dlf6l03u5i0s` |
| Cuentas | 4 |
| Agentes | 5 |
| Conversaciones | 100.700 |
| Mensajes publicados | 503.500 |
| Minutos virtuales | 60 |
| Duración real de publicación | 60 s |
| Workers | 4 réplicas × 25 slots = 100 slots |
| Procesamiento simulado | 50 ms + hasta 25 ms de variación |
| Fallo transitorio | 5 % |
| Fallo permanente | 1 % |
| Publicaciones duplicadas | 2 % |
| SLA de espera | 2 s |

La demanda comprimida estimada era de **8.391,7 mensajes/s**, mientras la
capacidad teórica configurada era de **1.600 mensajes/s**. El laboratorio
estimó 525 slots necesarios y sólo había 100: un margen de aproximadamente
`0,2×`. Generar backlog era esperado; detener todos los workers no lo era.

## Estado visible al fallar

| Métrica | Valor |
|---|---:|
| Tiempo transcurrido | 5 min 9 s |
| Publicados | 503.500 |
| Terminales | 375.533 |
| Completados | 371.534 |
| Permanentes | 3.999 |
| Esperando | 127.966 |
| Activos | 1 |
| Throughput de completado | 423,3/s |
| Espera promedio | 264.032,9 ms |

El publicador ya había terminado. El backlog permaneció porque los workers se
apagaron después del error, no porque siguieran entrando mensajes.

## Hechos confirmados

- Redis no se reinició durante la prueba.
- Redis no expulsó claves y no rechazó conexiones.
- El job mencionado por el error estaba en estado `completed`, con un solo
  intento y 61 ms de procesamiento.
- Su evento de finalización apareció aproximadamente 5,16 segundos después de
  su inicio, aunque el procesamiento registrado fue de sólo 61 ms.
- Qbit usa por defecto una reserva de 30 segundos, renovación cada 10 segundos
  y un timeout de 5 segundos para finalizar un job.
- Redis registró comandos lentos de hasta aproximadamente 1,54 segundos.
- Durante la carga, Redis realizó reescrituras de AOF y guardados RDB repetidos.
- Redis avisó que el `fsync` asíncrono de AOF estaba tardando demasiado porque
  el disco estaba ocupado.
- El AOF había crecido a aproximadamente 311 MB al momento del diagnóstico.
- El laboratorio cuenta el handler como completado antes de recibir el `ACK`
  final de Qbit. Por eso “handler ejecutado” y “finalización confirmada” no son
  exactamente la misma métrica durante una respuesta ambigua.

## Condición que activó el fallo

La explicación más consistente es saturación de Redis o del disco local durante
la persistencia. Esa presión habría retrasado la ejecución o la respuesta del
script de finalización hasta alcanzar el timeout de cinco segundos.

Es probable que el primer intento de finalizar sí haya cambiado el job a
`completed`, pero su respuesta haya llegado tarde o se haya perdido para el
cliente. Una comprobación o repetición posterior encontró que la reserva ya no
existía y devolvió `reservation lost`.

Esta conclusión es una **inferencia**, apoyada por el estado final del job, los
61 ms de procesamiento, la separación cercana a cinco segundos entre eventos y
las advertencias de persistencia. Una expiración o transferencia genuina de la
reserva es menos consistente con un handler de 61 ms y un TTL de 30 segundos,
pero debe conservarse como alternativa hasta reproducir el caso con trazas del
comando de finalización. La corrección no depende de confirmar cuál de estas
condiciones de infraestructura ocurrió: ambas quedan cubiertas por la misma
garantía de idempotencia y fencing.

## Impacto

- El mensaje señalado quedó completado; no hay evidencia de pérdida en ese job.
- El resultado del `ACK` quedó ambiguo para el worker.
- El error aislado canceló los workers del laboratorio.
- 127.966 mensajes quedaron pendientes y la ejecución no pudo validar el
  drenaje completo de la cola.
- Un handler real con efectos externos podría repetir una acción si intenta
  recuperarse sin idempotencia de negocio.

## Resolución implementada

La corrección conserva el token único de la reserva como `finished_token` en el
hash durable del job:

1. El primer `Complete` o `Fail` válido aplica la transición terminal, guarda
   `finished_token`, libera el grupo e incrementa eventos y contadores una sola
   vez.
2. Si se repite exactamente la misma operación con el mismo token y estado, el
   script devuelve éxito idempotente. No vuelve a liberar el grupo ni duplica
   métricas.
3. Si el token o el estado solicitado son diferentes, Qbit mantiene
   `ErrReservationLost`. Por tanto, la solución no convierte una pérdida real
   de propiedad en un éxito falso.
4. Cuando un worker administrado recibe una pérdida real de reserva, abandona
   únicamente ese job. El mensaje queda en manos de su dueño actual o de la
   recuperación por expiración, y el slot continúa procesando otros grupos.
5. Los errores de infraestructura o Redis que no sean `ErrReservationLost`
   continúan siendo fatales y visibles para la réplica.

La identidad idempotente es el propio token criptográficamente aleatorio de la
reserva. Esto evita agregar una identidad menos fuerte y enlaza la confirmación
directamente con el worker que realmente recibió el job.

Para medir throughput puro puede ejecutarse una línea base local sin AOF, pero
eso elimina durabilidad y **no es una corrección de producción**. La validación
equivalente a producción debe mantener persistencia y observar latencia de
disco, duración de reescrituras y política de crecimiento del AOF.

## Validación automatizada

Se agregaron pruebas de integración que confirman:

- repetir `Complete` con el mismo token devuelve éxito y cuenta un solo
  completado;
- repetir `Fail` con el mismo token devuelve éxito y cuenta un solo fallo;
- otro token continúa recibiendo `ErrReservationLost`;
- una pérdida real de reserva no detiene el worker y el mismo slot completa un
  job de otro grupo.

## Revalidación de carga recomendada

Usar siempre una cola nueva; no mezclar la cola fallida con una validación
limpia.

1. Ejecutar primero 60 minutos virtuales en 3.600 segundos reales.
2. Repetirlos en 600 segundos para una carga acelerada moderada.
3. Ejecutar finalmente la prueba extrema de 60 segundos.
4. En cada nivel, registrar latencia del `ACK`, latencia Redis, backlog, workers
   vivos, renovaciones y operaciones terminales ambiguas.

La cola del incidente conserva sus datos. Para drenarla hay que conectar
workers intencionalmente; no debe borrarse mientras se necesite para análisis.

## Criterios de aceptación

- [x] Reintentar la misma finalización devuelve el mismo resultado exitoso.
- [x] El replay no duplica eventos ni contadores terminales.
- [x] Un token diferente conserva el fencing y es rechazado.
- [x] Una pérdida real de reserva no detiene los demás slots sanos.
- [x] Las pruebas existentes de FIFO y exclusión mutua por thread siguen
  pasando.
- [ ] Repetir el escenario completo de 503.500 mensajes y comprobar que el
  backlog vuelva a cero. Esta es una revalidación operativa, no una tarea de
  código pendiente para cerrar la incidencia.

## Componentes relacionados

- [`packages/go/qbit/worker.go`](../../packages/go/qbit/worker.go)
- [`packages/go/qbit/queue.go`](../../packages/go/qbit/queue.go)
- [`protocol/redis/scripts/finish.lua`](../../protocol/redis/scripts/finish.lua)
- [`protocol/redis/scripts/renew.lua`](../../protocol/redis/scripts/renew.lua)
- [Laboratorio de carga de Fazpi](../../apps/fazpi-loadtest/README.md)
