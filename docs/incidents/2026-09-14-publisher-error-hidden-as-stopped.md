# Error de publicación oculto como `STOPPED`

## Estado

**Resuelto en código; regresiones y suite completa aprobadas.** Falta repetir
la prueba de aceptación de carga completa en el laboratorio.

## Síntoma

Cuando Redis rechazaba o interrumpía una publicación, la ejecución podía
terminar como `STOPPED` sin enseñar la causa original. Esto hacía parecer que
alguien había detenido el escenario, aunque el productor realmente hubiera
fallado.

## Causa

El publicador cancelaba primero el contexto compartido. Después devolvía el
error al coordinador, pero este evitaba registrarlo porque el contexto ya
estaba cancelado. El monitor recibía la cancelación sin un error guardado y la
clasificaba como una detención normal.

El flujo defectuoso era:

```text
Redis devuelve error → cancelar escenario → intentar registrar error
                                             └─ se omite porque ya está cancelado
```

## Corrección

El publicador ahora llama a la finalización por fallo antes de abandonar su
goroutine. Esa operación guarda de forma idempotente el **primer error real** y
después cancela el resto del escenario:

```text
Redis devuelve error → guardar primer error → cancelar escenario → FAILED
```

Los errores posteriores, incluido `context canceled`, no sobrescriben la causa
original. El panel muestra además un bloque rojo llamado **Error original** con
el texto recibido de Redis/Qbit.

Las pruebas `TestSimulationFailureKeepsTheOriginalError` y
`TestPublishFailureIsExposedBeforeCancellation` verifican que:

1. el primer error queda guardado;
2. una cancelación secundaria no lo reemplaza;
3. el contexto del escenario sí se cancela.

La segunda prueba recorre específicamente la ruta del publicador con un cliente
Redis cerrado y comprueba que la causa visible coincide con la devuelta por la
publicación.

## Resultado esperado al repetir la prueba

Si Redis llega a `maxmemory`, pierde conectividad o rechaza una publicación, el
laboratorio debe terminar en `FAILED` y mostrar el error original. `STOPPED`
queda reservado para una detención solicitada por el usuario.
