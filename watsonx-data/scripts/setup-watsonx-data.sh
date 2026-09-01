#!/usr/bin/env bash
# ═══════════════════════════════════════════════════════════════════════════
# Instala watsonx.data developer edition de forma automatizada.
#
# Requisito manual previo: descarga watsonx.data-developer-edition-installer.tar
# desde https://early-access.ibm.com (requiere registro/entitlement de IBM) y
# colócalo en watsonx-data/installer/ — no se puede automatizar esa descarga
# porque requiere login.
#
# Este script SÍ automatiza todo lo demás: extraer, dar permisos, correr el
# instalador (que crea un cluster KIND vía Docker), esperar a que los pods
# estén listos, exponer la consola con port-forward, y avisar cuando terminó.
#
# Uso:
#   bash scripts/setup-watsonx-data.sh
# ═══════════════════════════════════════════════════════════════════════════
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(dirname "$SCRIPT_DIR")"
INSTALLER_DIR="$BASE_DIR/installer"

log() { echo -e "\n>> $*"; }
err() { echo -e "\n[ERROR] $*" >&2; }

# ── 1. Verificar prerrequisitos básicos ────────────────────────────────────
log "Verificando prerrequisitos (docker, kubectl)..."
command -v docker >/dev/null 2>&1 || { err "docker no está instalado o no está en el PATH."; exit 1; }
command -v kubectl >/dev/null 2>&1 || { err "kubectl no está instalado o no está en el PATH. Es requerido por el instalador de watsonx.data."; exit 1; }

log "Recordatorio de recursos: watsonx.data developer edition levanta su PROPIO"
echo "   cluster Kubernetes (KIND) además de tu stack de Kafka/Flink/Marquez ya"
echo "   corriendo. Verifica que la VM tenga suficiente RAM libre (IBM recomienda"
echo "   16GB+ solo para watsonx.data) antes de continuar."

# ── 2. Localizar el .tar del instalador ────────────────────────────────────
TAR_FILE=$(find "$INSTALLER_DIR" -maxdepth 1 -iname "*.tar" | head -n1 || true)

if [ -z "$TAR_FILE" ]; then
  err "No se encontró ningún .tar en $INSTALLER_DIR"
  echo ""
  echo "  1. Descarga watsonx.data-developer-edition-installer.tar desde:"
  echo "     https://early-access.ibm.com/software/support/trial/cst/programwebsite.wss?siteId=2309"
  echo "     (requiere cuenta/entitlement de IBM)"
  echo "  2. Colócalo en: $INSTALLER_DIR/"
  echo "  3. Vuelve a correr este script."
  exit 1
fi

log "Instalador encontrado: $TAR_FILE"

# ── 3. Extraer ───────────────────────────────────────────────────────────
cd "$INSTALLER_DIR"
EXTRACT_DIR="watsonx.data-developer-edition-installer"

log "Extrayendo el instalador..."
rm -rf "./$EXTRACT_DIR"
tar -xvf "$(basename "$TAR_FILE")"

if [ ! -d "$EXTRACT_DIR" ]; then
  # Algunas versiones del .tar extraen a un nombre distinto; toma el
  # directorio más reciente que contenga installer.sh
  EXTRACT_DIR=$(find . -maxdepth 1 -type d -newer "$(basename "$TAR_FILE")" -exec test -e "{}/installer.sh" \; -print | head -n1)
fi

if [ -z "$EXTRACT_DIR" ] || [ ! -f "$EXTRACT_DIR/installer.sh" ]; then
  err "No se encontró installer.sh tras extraer el .tar. Revisa el contenido manualmente en $INSTALLER_DIR"
  exit 1
fi

cd "$EXTRACT_DIR"

# ── 4. Permisos y ejecución del instalador ─────────────────────────────────
log "Dando permisos de ejecución a installer.sh..."
chmod 777 ./installer.sh

log "Ejecutando installer.sh — esto puede tardar 15-30+ minutos (crea un"
echo "   cluster KIND, descarga imágenes, despliega el lakehouse). No cierres"
echo "   esta terminal."
./installer.sh

# ── 5. Detectar el namespace real (la documentación varía entre 'wxd' y ────
#      'spark' según la versión del instalador) ────────────────────────────
log "Detectando el namespace donde quedó desplegado watsonx.data..."
NAMESPACE=""
for candidate in wxd spark; do
  if kubectl get svc -n "$candidate" lhconsole-ui-svc >/dev/null 2>&1; then
    NAMESPACE="$candidate"
    break
  fi
done

if [ -z "$NAMESPACE" ]; then
  # último recurso: busca en todos los namespaces
  NAMESPACE=$(kubectl get svc --all-namespaces 2>/dev/null | awk '/lhconsole-ui-svc/ {print $1; exit}')
fi

if [ -z "$NAMESPACE" ]; then
  err "No se pudo detectar el namespace de watsonx.data automáticamente."
  echo "  Revisa manualmente con: kubectl get svc --all-namespaces | grep lhconsole"
  exit 1
fi

log "Namespace detectado: $NAMESPACE"

# ── 6. Esperar a que todos los pods estén Running/Completed ────────────────
log "Esperando a que todos los pods en namespace '$NAMESPACE' estén listos..."
echo "   (esto también puede tardar varios minutos la primera vez)"

MAX_WAIT_SECONDS=1800   # 30 minutos
INTERVAL=15
elapsed=0

while true; do
  NOT_READY=$(kubectl get po -n "$NAMESPACE" --no-headers 2>/dev/null \
    | grep -vE 'Running|Completed' | wc -l | tr -d ' ')
  TOTAL=$(kubectl get po -n "$NAMESPACE" --no-headers 2>/dev/null | wc -l | tr -d ' ')

  if [ "$TOTAL" -gt 0 ] && [ "$NOT_READY" -eq 0 ]; then
    log "Todos los pods ($TOTAL) están Running o Completed."
    break
  fi

  echo "   [$elapsed s] $((TOTAL - NOT_READY))/$TOTAL pods listos..."
  sleep "$INTERVAL"
  elapsed=$((elapsed + INTERVAL))

  if [ "$elapsed" -ge "$MAX_WAIT_SECONDS" ]; then
    err "Tiempo de espera agotado ($MAX_WAIT_SECONDS s). Revisa manualmente:"
    echo "  kubectl get po -n $NAMESPACE"
    exit 1
  fi
done

# ── 7. Exponer la consola vía port-forward ──────────────────────────────────
log "Exponiendo la consola de watsonx.data (puerto 6443)..."
# Mata cualquier port-forward previo a este servicio para evitar duplicados
pkill -f "port-forward -n $NAMESPACE service/lhconsole-ui-svc" 2>/dev/null || true
sleep 1

export KUBECONFIG="${KUBECONFIG:-$HOME/.kube/config}"
nohup kubectl port-forward -n "$NAMESPACE" service/lhconsole-ui-svc 6443:443 \
  --address 0.0.0.0 > "$BASE_DIR/port-forward-console.log" 2>&1 &

sleep 3

# ── 8. Resumen final ────────────────────────────────────────────────────
cat <<EOF

✅ watsonx.data developer edition está listo.

  Namespace Kubernetes: $NAMESPACE
  Consola:              https://<IP-o-hostname-de-esta-VM>:6443/
  (si accedes vía túnel SSH, agrega: -L 6443:localhost:6443 a tu comando ssh)

  Usuario:  ibmlhadmin
  Password: password

  Log del port-forward: $BASE_DIR/port-forward-console.log

Gestión del cluster KIND (para liberar recursos cuando no lo estés usando):
  docker stop kind-wxd-control-plane    # detener
  docker start kind-wxd-control-plane   # reanudar

Para desinstalar por completo:
  bash $SCRIPT_DIR/teardown-watsonx-data.sh

EOF
