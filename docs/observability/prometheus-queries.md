# Consultas Prometheus para Qbit

Esta guía contiene expresiones PromQL para inspeccionar una o varias colas Qbit. Abre
Prometheus en `http://127.0.0.1:9090`, entra en **Query**, pega una expresión y
pulsa **Execute**. Los ejemplos usan la cola `whatsapp`; cambia el valor de la
etiqueta `queue` cuando observes otra cola.

## Estado de la observabilidad

Comprueba que Prometheus puede consultar el exportador de Qbit:

```promql
up{job="qbit"}
```

Un valor de `1` indica que el exportador responde. Un valor de `0` indica que
Prometheus no puede consultarlo.

Cantidad de colas descubiertas en el scrape actual:

```promql
qbit_exporter_discovered_queues
```

## Estado actual de la cola

Trabajos esperando ser reservados:

```promql
qbit_jobs_waiting{queue="whatsapp"}
```

Trabajos reservados y actualmente en procesamiento:

```promql
qbit_jobs_active{queue="whatsapp"}
```

Grupos que tienen trabajo disponible:

```promql
qbit_ready_groups{queue="whatsapp"}
```

Estado de pausa administrativa (`1` pausada, `0` activa):

```promql
qbit_queue_paused{queue="whatsapp"}
```

`qbit_jobs_active` no cuenta procesos worker conectados. Si la cola está vacía,
su valor será cero aunque los workers continúen ejecutándose y esperando.

## Workers registrados

Cantidad de procesos o réplicas worker con un heartbeat vigente:

```promql
qbit_worker_replicas_active{queue="whatsapp"}
```

Concurrencia total disponible entre todas las réplicas:

```promql
qbit_worker_concurrency_slots{queue="whatsapp"}
```

Lista cada réplica y su concurrencia. Cambia la visualización de Prometheus a
**Table** para leer las etiquetas `worker_id` y `worker_instance`:

```promql
qbit_worker_concurrency{queue="whatsapp"}
```

Lista la información de presencia de cada réplica:

```promql
qbit_worker_info{queue="whatsapp"}
```

Segundos transcurridos desde el último heartbeat de cada réplica:

```promql
time() - qbit_worker_last_heartbeat_timestamp_seconds{queue="whatsapp"}
```

Una réplica desaparece de estas consultas aproximadamente 15 segundos después
de detenerse o perder su conexión. El demo mantiene el heartbeat de forma
automática. Una réplica puede contener varios workers concurrentes, por eso se
exponen por separado el número de réplicas y el total de slots.

## Tasas por segundo

Trabajos publicados por segundo durante el último minuto:

```promql
rate(qbit_jobs_published_total{queue="whatsapp"}[1m])
```

Trabajos reservados por segundo:

```promql
rate(qbit_jobs_reserved_total{queue="whatsapp"}[1m])
```

Trabajos completados correctamente por segundo:

```promql
rate(qbit_jobs_completed_total{queue="whatsapp"}[1m])
```

Trabajos fallidos por segundo:

```promql
rate(qbit_jobs_failed_total{queue="whatsapp"}[1m])
```

Este contador representa intentos de procesamiento fallidos, incluidos los que
Qbit vuelve a intentar. Para separar el flujo de reintentos y recuperaciones:

```promql
rate(qbit_jobs_retried_total{queue="whatsapp"}[1m])
```

```promql
rate(qbit_jobs_recovered_total{queue="whatsapp"}[1m])
```

Un trabajo recuperado es uno que falló, volvió a la cola y después terminó
correctamente. En la demo puede generarse este flujo con `-failure-rate 10`.

Reservas vencidas y recuperadas por segundo:

```promql
rate(qbit_jobs_stalled_total{queue="whatsapp"}[1m])
```

Usa una ventana más larga, como `[5m]`, cuando el tráfico sea irregular. Una
ventana corta reacciona más rápido, pero produce una señal más variable.

## Latencia

Duración promedio de procesamiento observada durante la ventana reciente:

```promql
qbit_processing_duration_seconds{queue="whatsapp"}
```

Tiempo promedio que los trabajos reservados estuvieron esperando en cola:

```promql
qbit_queue_wait_seconds{queue="whatsapp"}
```

La espera puede continuar aumentando mientras el backlog baja porque los
workers están alcanzando trabajos cada vez más antiguos.

## Diagnóstico de capacidad

Velocidad neta de crecimiento del backlog:

```promql
rate(qbit_jobs_published_total{queue="whatsapp"}[1m])
+
rate(qbit_jobs_retried_total{queue="whatsapp"}[1m])
-
rate(qbit_jobs_completed_total{queue="whatsapp"}[1m])
-
rate(qbit_jobs_failed_total{queue="whatsapp"}[1m])
```

- Un resultado positivo significa que la cola está creciendo.
- Un resultado cercano a cero significa que entrada y salida están equilibradas.
- Un resultado negativo significa que la cola se está vaciando.

Tiempo estimado para vaciar el backlog, expresado en segundos:

```promql
qbit_jobs_waiting{queue="whatsapp"}
/
clamp_min(
  rate(qbit_jobs_completed_total{queue="whatsapp"}[1m])
  +
  rate(qbit_jobs_failed_total{queue="whatsapp"}[1m])
  -
  rate(qbit_jobs_retried_total{queue="whatsapp"}[1m]),
  0.001
)
```

Utilización de los espacios de procesamiento configurados:

```promql
100
*
qbit_jobs_active{queue="whatsapp"}
/
clamp_min(qbit_worker_concurrency_slots{queue="whatsapp"}, 1)
```

Porcentaje de reintentos que posteriormente se recuperan:

```promql
100
*
rate(qbit_jobs_recovered_total{queue="whatsapp"}[5m])
/
clamp_min(rate(qbit_jobs_retried_total{queue="whatsapp"}[5m]), 0.001)
```

Porcentaje de trabajos que terminan con fallo:

```promql
100
*
rate(qbit_jobs_failed_total{queue="whatsapp"}[5m])
/
clamp_min(
  rate(qbit_jobs_completed_total{queue="whatsapp"}[5m])
  +
  rate(qbit_jobs_failed_total{queue="whatsapp"}[5m]),
  0.001
)
```

## Contadores acumulados

```promql
qbit_jobs_published_total{queue="whatsapp"}
```

```promql
qbit_jobs_reserved_total{queue="whatsapp"}
```

```promql
qbit_jobs_completed_total{queue="whatsapp"}
```

```promql
qbit_jobs_failed_total{queue="whatsapp"}
```

```promql
qbit_jobs_stalled_total{queue="whatsapp"}
```

Estos valores son contadores monotónicos compartidos por todos los productores
y workers que usan la misma cola.

## Varias colas

Para comparar el backlog de todas las colas exportadas:

```promql
sum by (queue) (qbit_jobs_waiting)
```

Para comparar el throughput completado:

```promql
sum by (queue) (rate(qbit_jobs_completed_total[1m]))
```

Una sola instancia de `qbit-metrics` descubre automáticamente todas las colas
registradas en el mismo Redis. No es necesario desplegar un servidor por cola.
Cada serie conserva la etiqueta `queue`, y el selector del dashboard de Grafana
permite elegir una, varias o todas.

Para limitar deliberadamente el exportador, configura una lista separada por
comas, por ejemplo `QBIT_QUEUES=whatsapp,emails`.

## Alertas básicas

Backlog mayor de 50 trabajos:

```promql
qbit_jobs_waiting{queue="whatsapp"} > 50
```

Reservas vencidas durante los últimos cinco minutos:

```promql
increase(qbit_jobs_stalled_total{queue="whatsapp"}[5m]) > 0
```

Fallos sostenidos:

```promql
rate(qbit_jobs_failed_total{queue="whatsapp"}[5m]) > 0
```

Configura un periodo pendiente de uno o dos minutos en Grafana para evitar
notificaciones causadas por picos breves.

## Métricas que aún no están disponibles

Las métricas de CPU, memoria, conexiones y latencia interna de Redis requieren
un exportador específico de Redis.
