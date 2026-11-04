#!/usr/bin/env bash
# Lab 9 - partie 1 : deploiement de Polaris (Helm) sur Onyxia
# Usage (depuis la racine du repo, dans le terminal VS Code Onyxia) :
#   bash lab09_polaris/setup_polaris.sh
set -euo pipefail
cd "$(dirname "$0")"

# 1. Ressources du service vscode-pyspark (cpu 2000m / memory 2500Mi)
STATEFULSET_NAME="$(kubectl get statefulsets.apps -o=jsonpath='{.items[0].metadata.name}' -l app.kubernetes.io/name=vscode-pyspark)"
echo "StatefulSet : $STATEFULSET_NAME"
CURRENT_LIMIT="$(kubectl get statefulset "$STATEFULSET_NAME" -n "$KUBERNETES_NAMESPACE" \
  -o jsonpath='{.spec.template.spec.containers[0].resources.limits.memory}')"
if [ "$CURRENT_LIMIT" != "2500Mi" ]; then
  echo "Patch des ressources (le pod va redemarrer : relance ce script apres le redemarrage)."
  kubectl patch statefulset "$STATEFULSET_NAME" -n "$KUBERNETES_NAMESPACE" --type='strategic' -p='{
    "spec":{"template":{"spec":{"containers":[{"name":"vscode-pyspark","resources":{
      "requests":{"cpu":"50m","memory":"200Mi"},
      "limits":{"cpu":"2000m","memory":"2500Mi"}}}]}}}}'
  kubectl rollout status statefulset/"$STATEFULSET_NAME" -n "$KUBERNETES_NAMESPACE"
  exit 0
fi

# 2. Variables d'environnement
eval "$(aws configure export-credentials --profile default --format env)"
export LAB_BUCKET_NAME="$KUBERNETES_NAMESPACE"
export POLARIS_URI="http://polaris:8181"
export POLARIS_MGMT_URI="http://polaris-mgmt:8182"
export POLARIS_CATALOG="$LAB_BUCKET_NAME"
echo "Bucket / catalogue : $LAB_BUCKET_NAME"

echo "Contenu de la couche bronze :"
aws s3 --profile default ls "s3://$LAB_BUCKET_NAME/bronze/" || true

# 3. ConfigMap + secrets
kubectl create configmap polaris-config \
  --from-literal=POLARIS_CATALOG="$POLARIS_CATALOG" \
  --from-literal=POLARIS_URI="$POLARIS_URI" \
  --from-literal=POLARIS_MGMT_URI="$POLARIS_MGMT_URI" \
  --from-literal=POLARIS_REALM="POLARIS" \
  --from-literal=POLARIS_CLIENT_ID="root" \
  --from-literal=POLARIS_BASE_LOCATION="s3://$LAB_BUCKET_NAME/warehouse/" \
  --from-literal=S3_ENDPOINT="https://s3.seaweedfs.adm.adaltas.cloud/" \
  --dry-run=client -o yaml | kubectl apply -f -

kubectl create secret generic polaris-credential \
  --from-literal=POLARIS_ROOT_SECRET="s3cr3t" \
  --dry-run=client -o yaml | kubectl apply -f -

kubectl create secret generic polaris-s3 \
  --from-literal=AWS_ACCESS_KEY_ID="$AWS_ACCESS_KEY_ID" \
  --from-literal=AWS_SECRET_ACCESS_KEY="$AWS_SECRET_ACCESS_KEY" \
  --from-literal=AWS_SESSION_TOKEN="${AWS_SESSION_TOKEN:-}" \
  --dry-run=client -o yaml | kubectl apply -f -

# 4. Deploiement Helm (2-3 minutes)
helm repo add polaris https://downloads.apache.org/polaris/helm-chart 2>/dev/null || true
helm repo update
helm dependency update deployment
helm upgrade --install polaris deployment --wait --timeout 10m

curl -s "$POLARIS_MGMT_URI/q/health" | jq .status

# 5. Credentials dev / analyst (affiches UNE SEULE FOIS par le job de bootstrap)
CRED_FILE=/home/onyxia/.polaris-credentials.env
kubectl logs job/polaris-bootstrap \
  | grep '"credentials"' \
  | jq -r 'select(.principal == "dev" or .principal == "analyst")
      | "POLARIS_\(.principal | ascii_upcase)_CREDENTIAL=\"\(.credentials.clientId):\(.credentials.clientSecret)\""' \
  > "$CRED_FILE.new" || true
if [ -s "$CRED_FILE.new" ]; then
  mv "$CRED_FILE.new" "$CRED_FILE"
else
  rm -f "$CRED_FILE.new"
  echo "ATTENTION : aucun credential dans les logs (principals deja crees ?)."
  echo "Garde ton ancien $CRED_FILE ou reset les principals (voir README)."
fi
cat "$CRED_FILE"
echo ">> Copie ces credentials en lieu sur : le filesystem Onyxia est ephemere."
