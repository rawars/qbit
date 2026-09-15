# Reservas vencidas mostradas como activas y scrapes costosos

## Estado

Resuelta el 14 de septiembre de 2026.

## Incidencia observada

La ejecución `fazpi-4x-half-hour-500-580875` terminó con el laboratorio en
estado `STOPPED`. El panel mostraba `696` trabajos activos aunque la prueba
tenía solamente `500` slots configurados y ya no había ningún worker
registrado.

Al mismo tiempo, tanto el laboratorio como el exportador consultaban
estadísticas periódicamente. Cada llamada a `Stats` podía descargar y procesar
hasta 10.000 entradas del stream de eventos de la cola. Bajo carga sostenida,
esas lecturas competían en Redis con `Publish`, `Reserve`, renovaciones y ACK.

## Causa

El indicador `Active` utilizaba `HLEN` sobre el mapa de grupos activos. Ese
mapa conserva el dueño de una reserva hasta que Qbit recupera el trabajo. Si
los workers se detienen después de vencer los leases, los registros continúan
allí aunque ya no exista procesamiento real. Por eso el valor podía superar la
cantidad de slots: acumulaba reservas abandonadas de distintos instantes.

Las tasas y latencias se reconstruían recorriendo el stream de eventos en cada
consulta. El límite de 10.000 evitaba una lectura ilimitada, pero no era un
costo aceptable para un endpoint consultado cada pocos segundos.

## Corrección

Qbit ahora usa el índice ordenado de reservas para calcular dos valores
independientes con operaciones `ZCOUNT`:

- `active`: reservas cuyo vencimiento todavía está en el futuro;
- `expired_reservations`: reservas cuyo lease venció y esperan recuperación.

Las reservas vencidas permanecen fuera de `waiting` hasta que la recuperación
las devuelve atómicamente a su grupo. Así una misma unidad de trabajo no se
cuenta dos veces.

Las transiciones de la cola también actualizan agregados de cinco segundos.
Cada agregado contiene contadores, suma y cantidad de muestras de espera, y
suma y cantidad de muestras de procesamiento. Los hashes expiran después de
20 minutos y `Stats` lee como máximo los últimos 15 minutos. El número de
lecturas queda acotado por tiempo y ya no crece con la cantidad de mensajes.

El stream de eventos se mantiene para diagnósticos explícitos mediante
`RecentEvents`. `Stats` puede leer una sola entrada antigua como metadato de
compatibilidad, pero dejó de recorrer el stream para calcular tasas y
latencias. Prometheus continúa calculando tasas históricas desde los contadores
monotónicos.

## Compatibilidad

Las colas creadas antes de esta corrección conservan sus totales, backlog,
workers y estado de pausa. No se recorre retrospectivamente su stream para
reconstruir buckets, porque eso reintroduciría la sobrecarga corregida. Hasta
que lleguen nuevas transiciones, sus tasas y latencias recientes pueden ser
cero y `window_may_be_event_truncated` permanece activo. Después del primer
evento nuevo, la ventana se completa progresivamente.

Una ventana solicitada puede ampliarse hacia atrás menos de cinco segundos
para incluir el bucket inicial completo. `window_seconds` informa el intervalo
efectivamente agregado. Las solicitudes superiores a 15 minutos se limitan a
15 minutos y se marcan truncadas; el histórico de mayor duración pertenece a
Prometheus.

## Validación

La regresión cubre los siguientes casos:

1. una reserva vigente y otra vencida se informan como `active=1` y
   `expired_reservations=1`;
2. el backlog no duplica la reserva vencida;
3. las tasas y latencias siguen disponibles aunque se elimine el stream de
   eventos;
4. una solicitud de 24 horas genera un conjunto acotado de buckets y queda
   marcada como truncada;
5. Prometheus exporta `qbit_reservations_expired` por cola.

Las regresiones focalizadas y la suite completa pasaron contra un Redis
efímero aislado. La validación de aceptación pendiente es repetir el escenario
Fazpi grande y comparar latencia de comandos y duración de scrapes bajo carga.
