# Redis dedicado al transporte de Qbit

Esta configuración trata Redis como el motor de colas de Qbit, separado de
caché, sesiones y datos de negocio. Los threads, mensajes y estados propios de
Fazpi continúan en su base de datos; Redis mantiene únicamente el trabajo en
espera, activo o en reintento y la telemetría acotada de la cola.

## Perfil local incluido

El `docker-compose.yml` de la raíz aplica estos valores:

| Ajuste | Valor inicial | Motivo |
|---|---:|---|
| AOF | activado | Recuperar la cola después de reiniciar Redis. |
| `appendfsync` | `everysec` | Equilibrio entre durabilidad y throughput; ante una caída abrupta puede perderse aproximadamente el último segundo todavía no sincronizado. |
| snapshots RDB automáticos | desactivados | Evitar que un `BGSAVE` compita por CPU, memoria y disco durante una prueba de transporte. |
| `maxmemory` | `4gb` | Dejar un límite explícito y espacio fuera de Redis para Docker y el sistema. |
| política de memoria | `noeviction` | Nunca expulsar silenciosamente jobs de la cola; al llegar al límite, el productor recibe un error visible. |
| reescritura AOF | desde 256 MB y 200 % | Reducir reescrituras demasiado frecuentes durante cargas grandes. |
| liberación de memoria | lazy para `DEL` y expiraciones | Sacar del hilo principal parte del costo de liberar objetos grandes. |
| límite del contenedor | `5g` | Mantener aproximadamente 1 GB de margen respecto a `maxmemory`. |

Los límites son configurables sin editar el Compose:

```powershell
$env:QBIT_REDIS_MAXMEMORY = "4gb"
$env:QBIT_REDIS_CONTAINER_MEMORY = "5g"
docker compose up -d redis
```

En una máquina con menos memoria deben bajarse ambos valores conservando margen
entre ellos. En una máquina mayor se pueden subir después de medir el tamaño
real por job y el pico de backlog. `noeviction` convierte una falta de memoria
en un fallo explícito; no reemplaza las alertas ni el dimensionamiento.

## Limpieza del laboratorio

La interfaz ofrece **Limpiar datos** cuando la ejecución actual ya terminó. La
operación:

- sólo acepta el nombre exacto escrito y pide confirmarlo junto con la
  dirección Redis;
- se bloquea mientras haya productores o workers activos;
- rechaza servidores que no estén en loopback —salvo el puente local
  `host.docker.internal`— para impedir que una herramienta local borre por
  accidente una cola remota;
- recorre únicamente `qbit:{cola}:*` con `SCAN`;
- usa `UNLINK` para liberar las claves fuera del hilo principal de Redis;
- elimina el nombre de esa cola del registro de métricas;
- nunca ejecuta `FLUSHDB` ni toca otras colas o datos.

Esto permite vaciar Redis local al terminar las pruebas sin detenerlo. No debe
usarse este patrón para borrar diariamente producción: allí las retenciones de
Qbit eliminan los jobs terminales y los jobs pendientes se conservan hasta ser
procesados o reconstruidos de forma controlada desde la fuente de verdad.

## Regla para producción

El proceso de Redis dedicado debe ejecutarse como un servicio persistente con
volumen durable y reinicio automático. La forma concreta puede ser Docker,
`systemd` o el servicio administrado que use Fazpi; no requiere Kubernetes.
Antes de migrar tráfico real se deben definir:

1. memoria máxima calculada con el backlog de peor caso más margen;
2. alertas antes de 70 %, 80 % y 90 % de `maxmemory`;
3. alerta si `aof_last_write_status` deja de ser `ok`;
4. monitoreo de latencia, conexiones, comandos rechazados y crecimiento AOF;
5. procedimiento de reinicio y recuperación probado;
6. réplica o respaldo externo si el RPO exige más que AOF `everysec`;
7. retenciones cortas para completados y fallidos, acordes con que la historia
   definitiva vive en la base de Fazpi.

Los pools de productores, workers y observabilidad deben permanecer separados,
aunque todos apunten a este mismo Redis dedicado. Separar pools evita que una
reserva bloqueante consuma las conexiones del productor; el límite de memoria y
la persistencia resuelven responsabilidades distintas.
