# K3s en un solo nodo: control plane y worker en la misma máquina

Esta guía instala un clúster Kubernetes ligero con **K3s** en una sola máquina Linux. En K3s, el proceso `server` también incluye un agente (`kubelet` y runtime de contenedores), por lo que el mismo nodo administra el clúster y ejecuta los Pods sin configuración adicional.

> Esta topología es ideal para desarrollo, laboratorios, edge y cargas pequeñas. No ofrece alta disponibilidad: si el nodo falla, tanto el control plane como las aplicaciones quedan fuera de servicio.

## Arquitectura

```text
+--------------------------------------------------+
| Nodo Linux                                      |
|                                                  |
|  K3s server / control plane                      |
|  - Kubernetes API                               |
|  - scheduler y controller-manager               |
|  - datastore SQLite                             |
|                                                  |
|  K3s agent / worker                             |
|  - kubelet                                      |
|  - containerd                                   |
|  - Pods de las aplicaciones                     |
+--------------------------------------------------+
```

## Requisitos

- Una máquina Linux moderna, por ejemplo Ubuntu Server 22.04/24.04 o Debian 12.
- Arquitectura `x86_64`, `arm64/aarch64` o `armhf`.
- Acceso con `sudo`.
- Hostname único y conectividad a Internet para la instalación estándar.
- Como punto de partida práctico: 2 CPU, 2 GB de RAM y 20 GB de disco. Suma los recursos que necesiten tus aplicaciones.
- Si administrarás el clúster de forma remota, permite TCP `6443` únicamente desde redes o IPs de confianza. No expongas UDP `8472` (VXLAN) a Internet.
- Los puertos `80` y `443` deben estar libres si usarás Traefik, instalado por defecto.

Comprueba el host:

```bash
hostnamectl
uname -m
free -h
df -h /
```

### Comprobar que los puertos estén disponibles

Ejecuta esta comprobación **antes de instalar K3s**. En un clúster de un solo nodo, TCP `6443` es utilizado por la API de Kubernetes; TCP `80` y `443` son utilizados por Traefik y ServiceLB, instalados de forma predeterminada. UDP `8472` corresponde a Flannel VXLAN y no debe estar ocupado por otro proceso.

```bash
for spec in tcp:6443 tcp:80 tcp:443 udp:8472; do
  protocol=${spec%%:*}
  port=${spec##*:}

  if [ "$protocol" = tcp ]; then
    listeners=$(sudo ss -H -lntp "sport = :${port}")
  else
    listeners=$(sudo ss -H -lnup "sport = :${port}")
  fi

  if [ -n "$listeners" ]; then
    echo "OCUPADO    ${protocol}/${port}"
    printf '%s\n' "$listeners"
  else
    echo "DISPONIBLE ${protocol}/${port}"
  fi
done
```

Si `ss` no está instalado en Ubuntu o Debian:

```bash
sudo apt-get update
sudo apt-get install -y iproute2
```

También puedes comprobar un puerto individual e identificar el proceso que lo usa:

```bash
sudo ss -lntp 'sport = :6443'
sudo ss -lnup 'sport = :8472'
sudo lsof -nP -iTCP:80 -sTCP:LISTEN
```

Una salida vacía significa que ningún proceso está escuchando en ese puerto. Si aparece un proceso, detenlo o reconfigúralo antes de instalar K3s. Los puertos `80` y `443` pueden estar ocupados si decides desactivar Traefik y usar otro mecanismo de ingreso; en ese caso instala con `--disable=traefik`.

Esta prueba detecta conflictos locales, pero no valida reglas de firewall. Si administrarás el clúster desde otra máquina, después de instalar comprueba desde esa máquina que la API sea alcanzable:

```bash
nc -vz IP_O_DNS_DEL_SERVIDOR 6443
```

No expongas `6443` a todo Internet: limita el origen a tu red o IP de administración. En una topología futura con varios nodos también deberás validar la conectividad entre ellos para UDP `8472` y TCP `10250`.

## 1. Preparar el sistema

En Ubuntu o Debian:

```bash
sudo apt-get update
sudo apt-get install -y curl ca-certificates
```

Si UFW está activo y solo usarás el clúster desde el propio nodo, no necesitas publicar la API. Para administrarlo desde otra máquina, limita el acceso a tu red de gestión; por ejemplo:

```bash
sudo ufw allow from 192.168.1.0/24 to any port 6443 proto tcp
```

Sustituye `192.168.1.0/24` por tu red real. Las reglas para instalaciones multinodo son distintas; consulta los requisitos de red oficiales antes de añadir nodos.

## 2. Instalar K3s

Ejecuta en la máquina Linux:

```bash
curl -sfL https://get.k3s.io | sh -
```

El instalador:

- registra y arranca el servicio `k3s` con systemd u OpenRC;
- instala `kubectl`, `crictl` y `ctr`;
- instala y configura containerd, Flannel, CoreDNS, Traefik, ServiceLB y almacenamiento local;
- crea el kubeconfig en `/etc/rancher/k3s/k3s.yaml`.

No instales `k3s agent` en esta misma máquina: el servicio `k3s server` ya ejecuta las funciones de worker.

## 3. Verificar el clúster

```bash
sudo systemctl status k3s --no-pager
sudo k3s kubectl get nodes -o wide
sudo k3s kubectl get pods -A
```

El único nodo debe aparecer como `Ready`. La columna `ROLES` normalmente mostrará `control-plane,master`; eso no impide que se programen cargas allí. Puedes confirmarlo:

```bash
sudo k3s kubectl describe node "$(hostname)" | grep -i '^Taints'
```

El resultado esperado es `Taints: <none>`. A diferencia de muchas instalaciones de Kubernetes, K3s no aplica por defecto un taint que bloquee las cargas en el nodo servidor.

## 4. Desplegar una aplicación de prueba

```bash
sudo k3s kubectl create deployment web --image=nginx:alpine
sudo k3s kubectl expose deployment web --type=NodePort --port=80
sudo k3s kubectl rollout status deployment/web
sudo k3s kubectl get pods -o wide
sudo k3s kubectl get service web
```

Obtén el puerto asignado y prueba el servicio desde el nodo:

```bash
NODE_PORT=$(sudo k3s kubectl get service web \
  -o jsonpath='{.spec.ports[0].nodePort}')
curl "http://127.0.0.1:${NODE_PORT}"
```

El Pod debe figurar sobre el mismo nodo que aloja el control plane. Elimina la prueba cuando termines:

```bash
sudo k3s kubectl delete service web
sudo k3s kubectl delete deployment web
```

## 5. Usar kubectl sin sudo (opcional)

### En el mismo servidor

```bash
mkdir -p "$HOME/.kube"
sudo cp /etc/rancher/k3s/k3s.yaml "$HOME/.kube/config"
sudo chown "$(id -u):$(id -g)" "$HOME/.kube/config"
chmod 600 "$HOME/.kube/config"
kubectl get nodes
```

Este kubeconfig contiene credenciales administrativas: no lo subas a Git ni lo compartas.

### Desde otra computadora

1. Copia de forma segura `/etc/rancher/k3s/k3s.yaml` desde el servidor.
2. En la copia local, cambia `server: https://127.0.0.1:6443` por la IP o el DNS del servidor.
3. Guarda el archivo con permisos restrictivos y úsalo con `KUBECONFIG`.
4. Asegúrate de que el certificado incluya el nombre o la IP usados. Para una instalación nueva puedes añadir `--tls-san`, por ejemplo:

```bash
curl -sfL https://get.k3s.io | sh -s - server \
  --tls-san k3s.ejemplo.com
```

## Operación básica

```bash
# Estado y logs
sudo systemctl status k3s
sudo journalctl -u k3s -f

# Parar, iniciar y reiniciar
sudo systemctl stop k3s
sudo systemctl start k3s
sudo systemctl restart k3s

# Información del clúster
sudo k3s kubectl cluster-info
sudo k3s kubectl get all -A
```

K3s guarda el estado local principalmente bajo `/var/lib/rancher/k3s`. Para una instalación importante, define y prueba una estrategia de respaldo antes de ponerla en producción.

## Actualizar K3s y Kubernetes

Kubernetes viene integrado en K3s: para actualizar Kubernetes se actualiza K3s. En un clúster de un solo nodo habrá una interrupción breve del control plane y posiblemente de las aplicaciones. Programa una ventana de mantenimiento.

### 1. Revisar el estado y las versiones

```bash
sudo k3s --version
sudo k3s kubectl version
sudo k3s kubectl get nodes -o wide
sudo k3s kubectl get pods -A
sudo systemctl cat k3s
sudo cat /etc/rancher/k3s/config.yaml 2>/dev/null || true
```

Guarda los argumentos y variables usados en la instalación. Al volver a ejecutar el instalador debes proporcionar nuevamente cualquier opción original; de lo contrario, el servicio puede perderla. Es preferible conservar la configuración persistente en `/etc/rancher/k3s/config.yaml`.

No saltes versiones menores de Kubernetes. Por ejemplo, actualiza de `v1.32` a `v1.33` y después a `v1.34`, respetando la política de version skew y revisando las notas de cada versión.

### 2. Crear un respaldo antes de actualizar

La instalación de un solo servidor usa SQLite de forma predeterminada. Este procedimiento detiene K3s para obtener una copia consistente:

```bash
BACKUP_DIR="/var/backups/k3s/$(date +%Y%m%d-%H%M%S)"
sudo mkdir -p "$BACKUP_DIR"

sudo systemctl stop k3s
sudo cp -a /var/lib/rancher/k3s/server/db "$BACKUP_DIR/"
sudo cp -a /var/lib/rancher/k3s/server/token "$BACKUP_DIR/server-token"
sudo cp -a /etc/rancher/k3s "$BACKUP_DIR/config"
sudo cp -a /usr/local/bin/k3s "$BACKUP_DIR/k3s-binary"
sudo systemctl start k3s

sudo find "$BACKUP_DIR" -maxdepth 2 -type f -ls
sudo k3s kubectl get nodes
```

Protege el respaldo: el token cifra información confidencial del datastore y es obligatorio para restaurarlo. Copia además los datos persistentes de las aplicaciones; respaldar SQLite no respalda automáticamente el contenido de todos los PersistentVolumes.

### 3. Elegir la versión

Para producción, usa el canal `stable` o fija una versión concreta. Consulta las versiones disponibles en las notas oficiales de K3s antes de continuar.

Actualizar a la versión estable vigente:

```bash
curl -sfL https://get.k3s.io | INSTALL_K3S_CHANNEL=stable sh -
```

Actualizar a una versión concreta —reemplaza el ejemplo por la versión elegida—:

```bash
curl -sfL https://get.k3s.io | \
  INSTALL_K3S_VERSION='v1.34.5+k3s1' sh -
```

Si la instalación original incluyó argumentos, vuelve a proporcionarlos. Por ejemplo:

```bash
curl -sfL https://get.k3s.io | \
  INSTALL_K3S_VERSION='v1.34.5+k3s1' \
  sh -s - server --tls-san k3s.ejemplo.com
```

No uses el canal `latest` en producción sin haber probado antes la versión. El instalador descarga el nuevo binario, actualiza la unidad de servicio y reinicia K3s; no ejecuta `cordon` ni `drain` automáticamente.

### 4. Validar después de actualizar

```bash
sudo systemctl status k3s --no-pager
sudo k3s --version
sudo k3s kubectl get nodes -o wide
sudo k3s kubectl get pods -A
sudo k3s kubectl get events -A \
  --sort-by=.lastTimestamp | tail -n 30
sudo journalctl -u k3s --since '15 minutes ago' --no-pager
```

Prueba también las aplicaciones, Ingress, DNS y volúmenes persistentes. Conserva el respaldo anterior hasta completar la validación.

### Sobre `cordon` y `drain`

En un clúster multinodo se drena cada servidor antes de actualizarlo. En este clúster de un solo nodo, `drain` desalojaría cargas que no tienen otro nodo donde ejecutarse, por lo que normalmente se programa la interrupción y se actualiza directamente. Para impedir que se creen Pods nuevos durante una intervención:

```bash
sudo k3s kubectl cordon fazpi
# Realiza la intervención.
sudo k3s kubectl uncordon fazpi
```

Sustituye `fazpi` por el nombre mostrado por `kubectl get nodes`.

### Rollback

Volver a una versión menor requiere restaurar tanto el binario anterior como un respaldo del datastore generado con esa versión. No basta con reinstalar un binario antiguo. Si no existe un respaldo válido previo a la actualización, no hay un rollback seguro. Sigue el procedimiento oficial de rollback correspondiente a SQLite y prueba la restauración antes de depender de ella.

## Tareas de administración

### Estado de nodos, cargas y recursos

```bash
sudo k3s kubectl get nodes -o wide
sudo k3s kubectl get pods -A -o wide
sudo k3s kubectl get deployments,statefulsets,daemonsets -A
sudo k3s kubectl get svc,ingress -A
sudo k3s kubectl top node
sudo k3s kubectl top pods -A --sort-by=memory
```

### Investigar una carga con problemas

```bash
sudo k3s kubectl describe pod NOMBRE_POD -n NAMESPACE
sudo k3s kubectl logs NOMBRE_POD -n NAMESPACE --all-containers --tail=200
sudo k3s kubectl logs NOMBRE_POD -n NAMESPACE \
  --all-containers --previous --tail=200
sudo k3s kubectl get events -n NAMESPACE \
  --sort-by=.lastTimestamp
```

### Escalar y reiniciar aplicaciones

```bash
sudo k3s kubectl scale deployment NOMBRE -n NAMESPACE --replicas=2
sudo k3s kubectl rollout restart deployment/NOMBRE -n NAMESPACE
sudo k3s kubectl rollout status deployment/NOMBRE -n NAMESPACE
sudo k3s kubectl rollout history deployment/NOMBRE -n NAMESPACE
```

### Revisar almacenamiento

```bash
sudo k3s kubectl get storageclass
sudo k3s kubectl get pv
sudo k3s kubectl get pvc -A
sudo du -sh /var/lib/rancher/k3s
df -h
df -i
```

No borres manualmente archivos dentro de `/var/lib/rancher/k3s`. Antes de eliminar un PVC, comprueba su política de retención y respalda los datos.

### Revisar red e Ingress

```bash
sudo k3s kubectl get svc,endpoints,endpointslices -A
sudo k3s kubectl get ingress -A
sudo k3s kubectl get pods -n kube-system -o wide
sudo ss -lntup
```

Traefik y ServiceLB pueden reservar `80` y `443` mediante reglas de red o `hostPort`; por eso no siempre aparecerá un proceso escuchando en esos puertos con `ss`.

## Tareas de mantenimiento

### Comprobación diaria o semanal

```bash
sudo systemctl is-active k3s
sudo k3s kubectl get nodes
sudo k3s kubectl get pods -A \
  --field-selector=status.phase!=Running,status.phase!=Succeeded
sudo k3s kubectl get events -A \
  --field-selector=type=Warning --sort-by=.lastTimestamp
df -h
df -i
free -h
```

Revisa reinicios crecientes, Pods que no estén `Running`, eventos `Warning`, presión de memoria y disco, y errores repetidos en los logs.

### Logs del servicio

```bash
sudo journalctl -u k3s --since today --no-pager
sudo journalctl -u k3s -p warning --since '7 days ago' --no-pager
sudo journalctl --disk-usage
```

Si journald ocupa demasiado espacio, configura límites persistentes en `/etc/systemd/journald.conf`; evita borrar logs antes de investigar y conservar la evidencia necesaria.

### Certificados

Comprueba periódicamente su vencimiento:

```bash
sudo k3s certificate check --output table
```

Los certificados cliente y servidor próximos a vencer se renuevan al iniciar K3s. Una rotación manual genera una interrupción y debe realizarse con respaldo y ventana de mantenimiento:

```bash
sudo systemctl stop k3s
sudo k3s certificate rotate
sudo systemctl start k3s
sudo k3s certificate check --output table
```

La rotación de las autoridades certificadoras (CA) es un procedimiento distinto y más delicado; sigue la documentación oficial de `rotate-ca` y no sobrescribas directamente los archivos TLS activos.

### Respaldos periódicos

- Respalda el datastore SQLite y el token juntos.
- Respalda por separado los datos de PersistentVolumes y bases de datos de las aplicaciones.
- Cifra la copia, almacénala fuera del nodo y aplica una política de retención.
- Comprueba integridad y espacio disponible.
- Practica una restauración periódica en un entorno aislado; un respaldo no probado es solo una esperanza.

### Calendario recomendado

| Frecuencia | Tareas |
|---|---|
| Diaria | Estado del nodo, Pods fallidos, alertas y capacidad de disco. |
| Semanal | Eventos, reinicios, logs de K3s, consumo de CPU/RAM y estado de volúmenes. |
| Mensual | Respaldo completo probado, certificados, actualizaciones del SO y revisión de versiones K3s. |
| Antes de cambios | Respaldo de SQLite, token, configuración, binario y datos de aplicaciones. |
| Trimestral | Prueba de restauración, revisión del firewall y eliminación de accesos obsoletos. |

## Desinstalar

Antes de continuar, respalda cualquier dato que necesites. La desinstalación elimina el clúster local y sus datos:

```bash
sudo /usr/local/bin/k3s-uninstall.sh
```

## Solución rápida de problemas

```bash
sudo systemctl status k3s --no-pager
sudo journalctl -u k3s --since '15 minutes ago' --no-pager
sudo k3s kubectl get nodes
sudo k3s kubectl get pods -A -o wide
sudo k3s kubectl get events -A --sort-by=.lastTimestamp
```

- Nodo `NotReady`: revisa los logs de `k3s`, espacio en disco, cgroups y módulos de red.
- Pods en `Pending`: ejecuta `kubectl describe pod ...` y comprueba recursos, taints y volúmenes.
- Fallos DNS: revisa los Pods de CoreDNS en `kube-system` y la resolución DNS del host.
- No abre una aplicación: revisa el Service, el puerto del firewall y si el Pod está escuchando en el puerto declarado.

## Referencias oficiales

- [K3s: Quick-Start Guide](https://docs.k3s.io/quick-start)
- [K3s: Requirements](https://docs.k3s.io/installation/requirements)
- [K3s: Manual Upgrades](https://docs.k3s.io/upgrades/manual)
- [K3s: Backup and Restore](https://docs.k3s.io/datastore/backup-restore)
- [K3s: Rolling Back](https://docs.k3s.io/upgrades/roll-back)
- [K3s: Certificate Management](https://docs.k3s.io/cli/certificate)
- [K3s: Server CLI](https://docs.k3s.io/cli/server)
- [K3s: Networking Services](https://docs.k3s.io/networking/networking-services)

Última revisión: septiembre de 2026.
