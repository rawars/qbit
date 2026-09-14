# Fazpi Qbit Load Lab

Aplicación local para simular cuentas, agentes, threads y mensajes de Fazpi
sobre Qbit. Cada agente puede tener una tasa de personas por hora y una cantidad
de mensajes por persona diferentes. Todos producen tráfico simultáneamente
sobre la misma cola. El panel permite dibujar picos y valles en ventanas de
cinco minutos, observar presión y throughput, controlar la cola y revisar
validaciones.

## Requisitos

- Redis disponible, por defecto en `127.0.0.1:6379`.
- Go con la versión declarada por el módulo.
- Opcional: el stack de Grafana y Prometheus de `docs/observability`.

## Ejecutar

Desde la raíz del repositorio:

```powershell
$env:QBIT_REDIS_ADDR = "127.0.0.1:6379"
go run ./apps/fazpi-loadtest
```

Abre `http://127.0.0.1:8080`.

Para usar otro puerto:

```powershell
go run ./apps/fazpi-loadtest -listen 127.0.0.1:8085
```

Cada escenario debe usar una cola vacía. El formulario genera un nombre nuevo
por defecto para evitar que métricas o jobs de ejecuciones anteriores alteren
las validaciones.

## Escenario de hora pico de Fazpi

El botón **Cargar hora pico Fazpi** prepara este caso:

- Casur / Kata en línea: 100.000 personas por hora.
- Casur / Agente 2: tasa editable porque todavía no se conoce el dato.
- Pascual / Agente principal: 500 personas por hora.
- Cuenta 3 / Agente principal: 100 personas por hora.
- Cuenta 4 / Agente principal: 100 personas por hora.

Con 60 minutos virtuales y 60 segundos reales, el laboratorio publica el
tráfico de una hora en un minuto. Cada fila permite indicar los mensajes por
persona de ese agente para simular conversaciones de distinta intensidad. Una
duración real menor genera más presión; 3.600 segundos reproduce la tasa real
durante una hora completa.

Las personas no aparecen todas juntas. El laboratorio reparte sus llegadas en
12 ventanas de cinco minutos según la curva seleccionada. **Hora de pagos**
incluye un pico principal y valles; **Uniforme** mantiene la misma intensidad;
**Doble pico** modela dos oleadas. Los 12 factores también son editables: subir
un factor mueve más personas hacia esa ventana sin cambiar el total por hora.

El resultado por cuenta y agente muestra publicaciones, terminales, backlog
actual, pico de backlog y espera en cola. Grafana muestra el comportamiento
agregado del Redis/Qbit compartido. El estimador de capacidad es orientativo:
la validación definitiva es que no existan violaciones, los agentes pequeños
continúen avanzando y el backlog vuelva a cero en un tiempo aceptable.
Configura **SLA espera máx.** para que el laboratorio marque automáticamente si
algún agente, aunque sea pequeño, supera el tiempo de espera aceptable.

## Validaciones

- FIFO por thread.
- Máximo un handler simultáneo por thread.
- Publicaciones duplicadas reconocidas como el mismo job.
- Ausencia de errores de publicación.
- Todos los mensajes únicos llegan a un estado terminal.
- Backlog y jobs activos regresan a cero.
- Espera máxima de cada agente dentro del SLA configurado.

Los fallos transitorios son determinísticos y sólo ocurren en el primer
intento. Los fallos permanentes también son determinísticos y no se solapan con
los transitorios.

## Incidentes resueltos

La prueba extrema de 503.500 mensajes permitió reproducir un resultado ambiguo
al confirmar un job: Redis lo dejó completado, pero el worker recibió
`reservation lost` y detuvo la ejecución con backlog pendiente. Qbit ahora
confirma idempotentemente una transición terminal ya aplicada y una pérdida
real de reserva no detiene todos los workers. El análisis, el fix y sus pruebas
están en
[`docs/incidents/2026-09-14-reservation-lost-under-load.md`](../../docs/incidents/2026-09-14-reservation-lost-under-load.md).
