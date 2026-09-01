# watsonx.data developer edition (opcional)

Este es un componente **separado** del stack principal de Docker Compose.
No se puede instalar con `docker-compose` porque el instalador de IBM crea su
propio **cluster Kubernetes local (KIND)** — es un lakehouse completo, no un
contenedor suelto.

En el contexto de este proyecto, watsonx.data developer edition **no es
necesario** para el pipeline de mortgage decisioning (Kafka + Flink + agentes
IA + Marquez ya funcionan sin él). Solo tiene sentido si además quieres
explorar/demostrar capacidades de lakehouse (Apache Iceberg, consultas multi-
motor) sobre los datos generados por el pipeline.

## ⚠️ Antes de instalar

- **Recursos**: levanta un cluster Kubernetes completo aparte de tu stack
  actual. IBM recomienda 16GB+ de RAM solo para watsonx.data — súmalo a lo
  que ya usa Kafka/Flink/Connect/Marquez. En una VM compartida, considera
  detener el stack principal (`docker compose stop`) mientras usas
  watsonx.data, o verificar que la VM tenga margen suficiente.
- **Requiere Docker y `kubectl`** instalados en la VM.
- **La descarga del instalador NO se puede automatizar**: requiere una
  cuenta/entitlement de IBM. Descárgalo tú mismo desde:
  https://early-access.ibm.com/software/support/trial/cst/programwebsite.wss?siteId=2309

## Instalación

1. Descarga `watsonx.data-developer-edition-installer.tar` (enlace arriba) y
   colócalo en esta carpeta:
   ```
   watsonx-data/installer/watsonx.data-developer-edition-installer.tar
   ```
2. Corre el script de instalación automatizada:
   ```bash
   bash watsonx-data/scripts/setup-watsonx-data.sh
   ```
   Este script:
   - Extrae el `.tar`
   - Da permisos de ejecución a `installer.sh`
   - Corre el instalador (crea el cluster KIND, despliega el lakehouse)
   - Detecta automáticamente el namespace de Kubernetes usado (varía entre
     `wxd` y `spark` según la versión del instalador)
   - Espera a que todos los pods estén `Running`/`Completed`
   - Expone la consola web con `kubectl port-forward` en el puerto `6443`
   - Imprime la URL y credenciales al terminar

3. Accede a la consola en `https://<tu-VM>:6443/` (o vía túnel SSH:
   `ssh -L 6443:localhost:6443 ...`) con:
   - Usuario: `ibmlhadmin`
   - Contraseña: `password`

## Otros comandos

```bash
# Ver estado (pods, port-forward activo, cluster KIND)
bash watsonx-data/scripts/status-watsonx-data.sh

# Detener el cluster KIND para liberar recursos sin desinstalar
docker stop kind-wxd-control-plane

# Reanudarlo después
docker start kind-wxd-control-plane

# Desinstalar por completo
bash watsonx-data/scripts/teardown-watsonx-data.sh
```

## Nota sobre la discrepancia de namespace

La documentación oficial vigente de IBM usa el namespace `wxd`
(`kubectl get po -n wxd`), pero el material específico de esta descarga
(`watsonx_data_developer_installer.pdf`) referencia el namespace `spark`.
El script `setup-watsonx-data.sh` prueba ambos automáticamente — si tu
versión del instalador usa un namespace distinto a esos dos, el script te lo
indicará para que lo revises manualmente con:
```bash
kubectl get svc --all-namespaces | grep lhconsole
```
