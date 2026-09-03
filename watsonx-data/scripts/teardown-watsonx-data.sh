#!/usr/bin/env bash
# Desinstala watsonx.data developer edition usando el uninstaller.sh oficial
# que viene dentro del directorio extraído del instalador.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(dirname "$SCRIPT_DIR")"
INSTALLER_DIR="$BASE_DIR/installer"

EXTRACT_DIR=$(find "$INSTALLER_DIR" -maxdepth 1 -type d -exec test -e "{}/uninstaller.sh" \; -print | head -n1)

# Detiene cualquier port-forward activo primero, incluyendo el watchdog de
# MinIO con auto-reinicio (ver watsonx-data/scripts/get-iceberg-connection-info.sh)
# — sin este paso, el watchdog se queda reintentando conectarse a un pod que
# ya no existe indefinidamente, llenando de ruido /tmp/wxd-minio-port-forward.log.
pkill -f "port-forward-watchdog-minio" 2>/dev/null || true
pkill -f "port-forward -n wxd service/lhconsole-ui-svc" 2>/dev/null || true
pkill -f "port-forward -n spark service/lhconsole-ui-svc" 2>/dev/null || true
pkill -f "kubectl.*port-forward.*wxd" 2>/dev/null || true
pkill -f "kubectl.*port-forward.*spark" 2>/dev/null || true

if [ -z "$EXTRACT_DIR" ]; then
  echo "No se encontró uninstaller.sh. Deteniendo el cluster KIND directamente..."
  docker stop kind-wxd-control-plane 2>/dev/null || true
  docker rm -f kind-wxd-control-plane 2>/dev/null || true
  echo "Cluster KIND detenido/eliminado. Si quedaron volúmenes Docker huérfanos,"
  echo "revisa con: docker volume ls | grep kind"
  exit 0
fi

echo ">> Corriendo uninstaller.sh en $EXTRACT_DIR ..."
cd "$EXTRACT_DIR"
chmod +x ./uninstaller.sh
./uninstaller.sh

echo "✅ watsonx.data developer edition desinstalado."
echo ""
echo "Recordatorio: si tenías ICEBERG_ENABLED=true en tu .env del stack"
echo "principal (mortgage-ai-streaming-lab), ponlo en false y borra los"
echo "conectores Iceberg antes de que empiecen a fallar contra un watsonx.data"
echo "que ya no existe:"
echo "  curl -X DELETE http://localhost:8083/connectors/iceberg-enriched-sink"
echo "  curl -X DELETE http://localhost:8083/connectors/iceberg-decisions-sink"
