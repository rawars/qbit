# Incidencia: un error transitorio de Redis detenía la réplica completa

- **Fecha:** 2026-09-14
- **Estado:** Resuelta en código; regresiones y suite completa aprobadas, pendiente de la prueba Fazpi de carga completa
- **Componente:** SDK Go de Qbit, worker administrado

## Síntoma

Una demora temporal de Redis durante `Reserve`, `Renew`, `Complete`, `Fail` o
`Retry` llegaba a `reportFatal`. El worker cancelaba su contexto de ejecución y
todos los slots de esa réplica dejaban de consumir, aunque Redis pudiera volver
a responder segundos después.

En una carga grande, el efecto se amplificaba: una sola operación afectada por
latencia de disco, failover o agotamiento momentáneo del pool podía retirar una
réplica completa y reducir abruptamente la capacidad de drenaje.

## Causa

El worker sólo diferenciaba `ErrNoJob`, una cola pausada y una reserva perdida.
Todos los demás errores de infraestructura se trataban igual que un defecto
permanente de protocolo o configuración.

Además, la renovación cancelaba el handler con el primer error, sin aprovechar
el tiempo restante del TTL para recuperar la conexión.

## Corrección

El núcleo del worker ahora:

1. Clasifica como transitorios los timeouts, cortes de red, EOF, saturación del
   pool y estados recuperables de Redis/failover (`LOADING`, `READONLY`,
   `TRYAGAIN`, `MASTERDOWN`, `CLUSTERDOWN`, `MOVED`, `ASK` y máximo de clientes).
2. Reintenta esas operaciones con backoff exponencial con jitter, desde 50 ms
   hasta 2 s, hasta que Redis responda o se cancele el worker. Incluso un
   backoff personalizado tiene un mínimo de 10 ms para impedir un bucle ocupado
   que agrave una caída de Redis.
3. Respeta inmediatamente la cancelación durante el backoff, de modo que el
   apagado ordenado no queda bloqueado.
4. Reintenta `Reserve` sin retirar el slot y vuelve a registrar el mismo worker
   si su heartbeat se interrumpe o su lease de registro vence.
5. Reintenta `Complete`, `Fail` y `Retry` dentro de una ventana acotada de cinco
   segundos. Si Redis no puede confirmar la transición, se considera incierta
   la propiedad de esa reserva y se deja que la recuperación por TTL la recoja;
   no se apagan los demás slots.
6. Reintenta `Renew` sólo mientras la reserva todavía puede pertenecer al
   worker. Si no obtiene confirmación antes del TTL, cancela ese handler y
   entrega el mensaje a la recuperación normal, evitando procesamiento sin
   fencing válido.

La aplicación puede personalizar el backoff mediante
`WorkerOptions.RedisRetryBackoff`.

## Errores que continúan siendo fatales

La corrección no oculta errores que necesitan intervención o indican un defecto:

- credenciales o permisos incorrectos;
- Redis sin memoria (`OOM`) o con escrituras deshabilitadas por configuración;
- cliente Redis cerrado;
- scripts Lua inválidos, respuestas de protocolo inválidas y errores de datos.

Esos errores siguen cancelando la réplica y se devuelven desde `Worker.Run`,
para que el proceso supervisor la reinicie y la causa permanezca observable.

## Validación

Se agregaron pruebas unitarias para la clasificación, recuperación, errores
permanentes y cancelación durante el backoff. La prueba de integración inyecta
timeouts reales en llamadas de script de reserva, renovación y finalización, y
comprueba que el mismo worker termina el trabajo sin detenerse.

La validación operativa pendiente consiste en repetir el escenario Fazpi de
2.050.000 mensajes y provocar latencia o una interrupción breve de Redis. La
réplica debe conservar sus slots, recuperar el throughput y no perder el orden
por grupo.

La suite completa se ejecutó contra un Redis efímero aislado y terminó sin
fallos. Esto evita confundir una regresión con la presión que aún conservaba el
Redis de la ejecución anterior.
