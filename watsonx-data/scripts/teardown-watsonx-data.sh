#!/usr/bin/env bash
# Desinstala watsonx.data developer edition usando el uninstaller.sh oficial
# que viene dentro del directorio extraído del instalador.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(dirname "$SCRIPT_DIR")"
INSTALLER_DIR="$BASE_DIR/installer"

EXTRACT_DIR=$(find "$INSTALLER_DIR" -maxdepth 1 -type d -exec test -e "{}/uninstaller.sh" \; -print | head -n1)

# Detiene cualquier port-forward activo primero
pkill -f "port-forward -n wxd service/lhconsole-ui-svc" 2>/dev/null || true
pkill -f "port-forward -n spark service/lhconsole-ui-svc" 2>/dev/null || true

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
