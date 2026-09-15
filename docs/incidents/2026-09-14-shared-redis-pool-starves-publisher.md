# Pool Redis compartido bloquea al publicador durante una hora real

## Estado

**Corregido en código el 14 de septiembre de 2026. La prueba de regresión con
250 slots pasó; queda pendiente repetir la prueba de aceptación completa de
2.050.000 mensajes durante 3.600 segundos.**

Este incidente es independiente del fallo `reservation lost` ya resuelto. En
esta ejecución no se observó esa excepción: los workers continuaron vivos, pero
el publicador no pudo alimentar la cola a la velocidad prevista.

## Resumen

El escenario debía publicar 2.050.000 mensajes durante 3.600 segundos. Después
de aproximadamente 65 minutos sólo había publicado cerca de 95.000 mensajes y
la ejecución seguía en estado `RUNNING`.

La causa era que el laboratorio compartía un solo cliente Redis entre el
publicador, el monitor y las diez réplicas de workers simuladas. Cada slot de un
worker que espera trabajo usa una operación bloqueante de Redis y retiene una
conexión. Al configurar 250 slots, esas esperas ocuparon el pool disponible y
dejaron al publicador esperando una conexión libre.

Una analogía sencilla: Redis es un edificio y las conexiones del pool son sus
puertas. El laboratorio puso 250 mensajeros esperando pedidos en las puertas,
pero sólo había 160. Las 160 puertas quedaron ocupadas por mensajeros que
esperaban; el camión que debía entregar nuevos paquetes —el publicador— tuvo
que esperar a que alguna puerta quedara libre. Había workers suficientes, pero
no recibían trabajo con la rapidez planeada.

## Ejecución afectada

| Dato | Valor |
| --- | ---: |
| Cola | `fazpi-4x-real-hour-08596491` |
| Ejecución | `dlf8fxr11dt4` |
| Conversaciones | 410.000 |
| Mensajes planeados | 2.050.000 |
| Ventana real | 3.600 s |
| Demanda promedio planeada | 569,4 msg/s |
| Réplicas simuladas | 10 |
| Slots por réplica | 25 |
| Slots totales | 250 |
| Publicadores concurrentes | 8 |
| Tasa real observada | aproximadamente 25–27 msg/s |
| Mensajes publicados tras unos 65 minutos | aproximadamente 95.000 |
| Errores de publicación | 0 |

## Evidencia de Redis

Durante la ejecución Redis informó:

| Métrica | Valor |
| --- | ---: |
| `connected_clients` | 168 |
| `blocked_clients` | 160 |
| `clients_in_timeout_table` | 160 |
| `maxclients` | 10.000 |
| conexiones rechazadas | 0 |
| claves expulsadas | 0 |

El cliente Go de Redis usa por defecto un pool de
`10 * runtime.GOMAXPROCS(0)`. En esta máquina `GOMAXPROCS` es 16, por lo que el
pool predeterminado es de 160 conexiones. La coincidencia exacta entre las 160
conexiones del pool y los 160 clientes bloqueados confirma la saturación del
pool compartido. No fue un límite de `maxclients` del servidor Redis.

## Por qué no terminó de publicar al cumplirse la hora

La ventana de 3.600 segundos define el ritmo objetivo de llegadas, no una fecha
límite que descarte mensajes. El planificador intenta entregar tareas a los
publicadores según la curva configurada. Cuando éstos no consiguen conexiones
Redis con suficiente rapidez, el canal interno se llena y el planificador
también espera.

Por diseño, los mensajes atrasados no se pierden: continúan publicándose
después de la hora. Por eso la ejecución seguía en `RUNNING`. Este comportamiento
protege los datos, pero también demuestra que la prueba no reprodujo la tasa de
una hora real y su resultado no sirve para validar la capacidad deseada.

## ¿Cómo se trasladaría a Fazpi?

No se conoce ni se presupone aquí el modelo de despliegue de Fazpi. El principio
que sí aplica, independientemente de si se ejecuta en servidores, contenedores,
servicios o procesos, es no permitir que las reservas bloqueantes de los
workers agoten las conexiones que necesitan los publicadores.

La separación recomendada es:

- El componente o proceso que recibe mensajes y los publica en Qbit usa un
  cliente productor propio.
- Cada proceso consumidor usa un cliente propio, dimensionado según su
  concurrencia.
- El componente que consulta métricas usa otro cliente para no competir con
  las reservas bloqueantes.
- Si un mismo proceso produce y consume, debe usar dos clientes separados: uno
  para publicar y otro para reservar/procesar.

Todos los clientes pueden apuntar al mismo Redis y a la misma cola. Separar los
pools no duplica los jobs ni cambia su orden; sólo evita que una función agote
las conexiones necesarias para otra.

En el laboratorio, una “réplica” es una goroutine dentro del mismo proceso. Para
evitar que las diez réplicas simuladas compitan dentro de un único pool, cada
una debe recibir su propio cliente Redis. Esto corrige el laboratorio, pero no
define por sí mismo cuántos clientes necesita Fazpi: esa decisión debe hacerse
después de identificar sus procesos productores, consumidores y su
concurrencia real.

## Corrección implementada en código

La corrección principal quedó implementada en `apps/fazpi-loadtest/main.go`:

1. Crear un cliente exclusivo para los publicadores.
2. Crear un cliente por réplica de worker simulada.
3. Crear un cliente independiente para el monitor.
4. Definir explícitamente el tamaño de cada pool en lugar de depender del valor
   predeterminado.
5. Cerrar todos los clientes al finalizar la ejecución.
6. Medir el retraso del planificador y mostrar una alerta y un fallo de validación
   cuando no logre publicar el escenario dentro de la ventana elegida.
7. Exponer en la interfaz la configuración y el uso de conexiones para que el
   cuello de botella sea visible antes y durante una prueba.

El SDK incorporó `Client.PoolStats()` para consultar conexiones reales,
esperas acumuladas y timeouts sin exponer el cliente interno de Redis. El
laboratorio agrega esos datos por responsabilidad: productor, workers y
monitor.

Para reproducir este caso localmente, una base razonable es:

| Función | Clientes | Pool sugerido |
| --- | ---: | ---: |
| Publicación | 1 | 64 conexiones |
| Workers | 1 por réplica | 35 conexiones por réplica de 25 slots |
| Monitor | 1 | 16 conexiones |

Los diez pools de workers sumarían como máximo 350 conexiones, más publicación
y monitoreo. El Redis local permite 10.000 clientes, por lo que esta separación
cabe dentro de su configuración actual. Estos valores son un punto de partida
para medir, no una regla universal de producción.

En Fazpi, el mismo arreglo combina código y configuración: el pool de cada
proceso consumidor debe ser mayor que su concurrencia bloqueante y conservar
margen para confirmaciones, reintentos y operaciones administrativas. Para
proponer cantidades exactas primero se debe revisar cómo está desplegado Fazpi.
Aumentar el pool no corrige por sí solo una saturación real de CPU, memoria, red
o disco de Redis; esos límites se deben medir aparte. La guía para trasladar el
arreglo a producción está en [`docs/redis-connection-pools.md`](../redis-connection-pools.md).

## Mitigación utilizada antes del arreglo

Reducir el total de slots a menos de 160 permitía evitar la contención más grave
con el cliente compartido anterior. Era una ayuda diagnóstica, no una
validación de capacidad ni el arreglo definitivo, porque publicación y consumo
todavía competían por el mismo pool.

No se corrige cambiando Grafana, reiniciando el navegador o aumentando
`maxclients`: Grafana sólo visualiza métricas y Redis todavía tenía capacidad
para aceptar conexiones adicionales.

## Validación del arreglo

Se añadió la prueba de integración
`TestApplicationIsolatesRedisPoolsUnderWorkerLoad`. Contra Redis local ejecuta:

| Dato | Valor |
| --- | ---: |
| Mensajes | 1.000 |
| Ventana de publicación | 1 s |
| Réplicas simuladas | 10 |
| Concurrencia por réplica | 25 |
| Slots totales | 250 |
| Pool productor | 64 |
| Pool por réplica | 35 |
| Pool monitor | 16 |

La prueba terminó correctamente en aproximadamente cuatro segundos incluyendo
publicación, procesamiento, drenaje, cancelación de workers y cierre. El
productor cumplió la ventana de un segundo más dos segundos de tolerancia. Con
el diseño anterior, los 250 slots competían dentro del pool único de 160 y esta
misma condición bloqueaba al productor.

## Criterios de cierre

- [x] Los publicadores, cada réplica simulada y el monitor tienen pools aislados.
- [ ] El escenario de 2.050.000 mensajes termina de publicarse dentro de la ventana
  de 3.600 segundos, con una tolerancia documentada.
- [ ] La tasa observada sigue la curva configurada y promedia aproximadamente
  569,4 msg/s.
- [x] La prueba de regresión no presenta errores de publicación ni agotamiento
  del pool productor.
- [x] Los 250 slots pueden reservar trabajo sin impedir nuevas publicaciones.
- [x] La interfaz distingue retraso de publicación, backlog Qbit y uso del pool.
- [ ] En la prueba completa, todos los mensajes alcanzan un estado terminal y
  el backlog regresa a cero.
