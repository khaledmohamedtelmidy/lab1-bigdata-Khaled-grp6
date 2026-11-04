#!/usr/bin/env bash
# Lab 10 - partie 1 : deploiement de Trino (Helm), branche sur le catalogue Polaris.
# Usage (racine du repo) : bash lab10_trino/setup_trino.sh
# Prerequis : profil AWS `default` a jour (script "AWS CLI" d'Onyxia) et Polaris deploye (lab 9).
# Prerequis : appliquer d'abord le patch du lab (pod vscode-pyspark limite a 2000Mi), sinon Trino (1Gi)
# depasse le quota memoire du namespace (4Gi). Voir README, section Resultats obtenus.
set -euo pipefail
cd "$(dirname "$0")"

export LAB_BUCKET_NAME="$KUBERNETES_NAMESPACE"
export POLARIS_URI="http://polaris:8181"
export POLARIS_CATALOG="$LAB_BUCKET_NAME"
export API="$POLARIS_URI/api/management/v1"

echo "== Acces S3"
aws s3 --profile default ls "s3://$LAB_BUCKET_NAME/bronze/"

echo "== Credentials Polaris"
set -a; . /home/onyxia/.polaris-credentials.env; set +a
[ -n "${POLARIS_DEV_CREDENTIAL:-}" ] || { echo "POLARIS_DEV_CREDENTIAL manquant"; exit 1; }

echo "== Secrets et configmap"
kubectl create secret generic trino-polaris-oauth \
  --from-literal=POLARIS_DEV_CREDENTIAL="$POLARIS_DEV_CREDENTIAL" \
  --dry-run=client -o yaml | kubectl apply -f -
kubectl create secret generic polaris-s3 \
  --from-literal=AWS_ACCESS_KEY_ID="$(aws configure get aws_access_key_id --profile default)" \
  --from-literal=AWS_SECRET_ACCESS_KEY="$(aws configure get aws_secret_access_key --profile default)" \
  --from-literal=AWS_SESSION_TOKEN="$(aws configure get aws_session_token --profile default)" \
  --dry-run=client -o yaml | kubectl apply -f -
kubectl create configmap trino-polaris-config \
  --from-literal=POLARIS_CATALOG="$POLARIS_CATALOG" \
  --dry-run=client -o yaml | kubectl apply -f -

echo "== Deploiement Helm (2-3 min)"
helm repo add trino https://trinodb.github.io/charts 2>/dev/null || true
helm repo update
helm dependency update deployment
helm upgrade --install trino deployment --wait --timeout 10m

echo "== Verification"
curl -s -o /dev/null -w 'HTTP %{http_code}\n' http://trino:8080/v1/info
curl -s http://trino:8080/v1/info | jq -c '{version: .nodeVersion.version, starting, uptime}'
