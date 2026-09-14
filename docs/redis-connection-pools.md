# Pools de conexiones Redis para implementar Qbit en Fazpi

Esta guía traduce el incidente encontrado en el laboratorio a reglas de
implementación para Fazpi. No presupone servidores, contenedores ni ningún
orquestador concreto: las unidades relevantes son los procesos o componentes
que producen, consumen y observan mensajes.

## Regla principal

Las operaciones bloqueantes de los workers no deben compartir un pool capaz de
agotar las conexiones utilizadas por los publicadores.

Cada llamada `ReserveBlocking` puede conservar una conexión Redis mientras
espera trabajo. Si hay `C` slots concurrentes, un proceso consumidor necesita
como mínimo más de `C` conexiones. Las conexiones adicionales permiten
registrar y renovar workers, confirmar trabajos, reintentar y cerrar de forma
ordenada.

## Separación recomendada

| Responsabilidad | Cliente Qbit | Pool Redis |
| --- | --- | --- |
| Recibir y publicar mensajes | cliente productor | propio |
| Reservar y procesar trabajos | cliente consumidor por proceso | propio |
| Leer estadísticas o controlar la cola | cliente de monitoreo | propio |

Si un mismo proceso cumple dos responsabilidades, debe crear dos clientes
Qbit. Ambos apuntan al mismo Redis y pueden abrir la misma cola: los datos no se
duplican y las garantías de FIFO, grupos e idempotencia no cambian.

## Dimensionamiento inicial

Para un consumidor con `C` slots:

```text
pool_consumidor >= C + margen
```

El laboratorio exige por seguridad al menos `C + 2` y usa `C + 10` en el
escenario conocido. El margen definitivo debe decidirse con mediciones de
esperas, timeouts y carga real; no es una constante universal.

Para productores, el pool debe ser por lo menos igual a la concurrencia máxima
de publicación. Para monitoreo, un pool pequeño e independiente evita que las
consultas compitan con el flujo de mensajes.

Configuración inicial del escenario validado:

| Función | Cantidad | Pool por cliente | Capacidad máxima |
| --- | ---: | ---: | ---: |
| Productor | 1 | 64 | 64 |
| Consumidores simulados | 10 | 35 para 25 slots | 350 |
| Monitor | 1 | 16 | 16 |
| **Total del laboratorio** | | | **430** |

`PoolSize` representa un máximo por cliente. Redis no abre necesariamente todas
esas conexiones al iniciar.

## Ejemplo en Go

```go
producer, err := qbit.NewClient(qbit.ClientOptions{
    Redis: qbit.RedisOptions{
        Address:  redisAddress,
        PoolSize: 64,
    },
})
if err != nil {
    return err
}
defer producer.Close()

workerConcurrency := 25
consumer, err := qbit.NewClient(qbit.ClientOptions{
    Redis: qbit.RedisOptions{
        Address:  redisAddress,
        PoolSize: workerConcurrency + 10,
    },
})
if err != nil {
    return err
}
defer consumer.Close()
```

El productor debe crear su `Queue` desde `producer`; el worker debe crear otra
referencia a la misma cola desde `consumer`.

## Presupuesto del servidor Redis

Antes de desplegar, sumar los máximos de todos los procesos que pueden estar
activos simultáneamente:

```text
total = pools_productores
      + pools_consumidores
      + pools_monitoreo
      + otros_clientes_Redis
      + reserva_operativa
```

El total debe quedar por debajo de `maxclients` y también debe ser sostenible
en memoria, CPU, red y descriptores de archivo. Tener conexiones disponibles
no demuestra por sí solo que Redis soporte el throughput requerido.

## Observabilidad obligatoria

Qbit expone `Client.PoolStats()` para leer por cliente:

- conexiones totales, inactivas y, por diferencia, conexiones en uso;
- `WaitCount` y tiempo acumulado esperando una conexión;
- misses y timeouts del pool;
- conexiones descartadas por estar obsoletas.

Se deben alertar incrementos sostenidos de `WaitCount`, cualquier timeout y una
ocupación cercana al máximo. Las métricas deben diferenciar productor,
consumidor y monitoreo; agregarlas en un único número escondería nuevamente el
origen de la contención.

El laboratorio también mide el retraso entre el instante planeado por la curva
de tráfico y la publicación real. Una ejecución falla la validación
**Publicación dentro de la ventana** si el productor termina fuera del tiempo
objetivo o si el retraso máximo supera la tolerancia. La tolerancia es 1 % de la
ventana, con un mínimo de 2 segundos y un máximo de 30 segundos.

## Lista de verificación para Fazpi

1. Identificar qué procesos publican, cuáles consumen y cuáles consultan
   métricas.
2. Registrar la concurrencia máxima de cada proceso consumidor.
3. Crear clientes separados cuando una misma instancia produce y consume.
4. Configurar `PoolSize` explícitamente; no depender del valor automático ligado
   a los núcleos de la máquina.
5. Calcular el presupuesto agregado y compararlo con `maxclients` y los demás
   consumidores de Redis.
6. Exportar estadísticas de cada pool y retraso real de publicación.
7. Probar picos y valles, fallos transitorios, reintentos y apagado ordenado.
8. Aumentar instancias o concurrencia sólo después de recalcular conexiones y
   repetir la prueba de carga.

## Lo que este arreglo no resuelve

El aislamiento de pools corrige la inanición del publicador causada por
reservas bloqueantes. No resuelve una saturación de CPU, memoria, disco o red,
ni distribuye automáticamente una sola cola caliente entre varios nodos Redis.
Esos límites requieren métricas y pruebas independientes.

