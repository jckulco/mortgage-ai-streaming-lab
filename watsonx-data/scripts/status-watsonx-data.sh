#!/usr/bin/env bash
# Diagnóstico rápido de watsonx.data developer edition.
set -uo pipefail

echo "── Cluster KIND (Docker) ───────────────────────────────"
docker ps --filter "name=kind-wxd-control-plane" --format "table {{.Names}}\t{{.Status}}" 2>/dev/null || echo "No corriendo"

echo ""
echo "── Namespace detectado ─────────────────────────────────"
NAMESPACE=""
for candidate in wxd spark; do
  if kubectl get svc -n "$candidate" lhconsole-ui-svc >/dev/null 2>&1; then
    NAMESPACE="$candidate"
    break
  fi
done
echo "Namespace: ${NAMESPACE:-no detectado}"

if [ -n "$NAMESPACE" ]; then
  echo ""
  echo "── Pods en '$NAMESPACE' ────────────────────────────────"
  kubectl get po -n "$NAMESPACE"

  echo ""
  echo "── Port-forward de la consola ──────────────────────────"
  pgrep -af "port-forward -n $NAMESPACE service/lhconsole-ui-svc" || echo "No hay port-forward activo. Corre setup-watsonx-data.sh de nuevo o lanza uno manual."
fi
