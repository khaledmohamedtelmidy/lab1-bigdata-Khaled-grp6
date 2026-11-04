# Lab 9 - variables d'environnement. A SOURCER (pas executer) :
#   source lab09_polaris/env.sh
eval "$(aws configure export-credentials --profile default --format env)"
export LAB_BUCKET_NAME="$KUBERNETES_NAMESPACE"
export POLARIS_URI="http://polaris:8181"
export POLARIS_MGMT_URI="http://polaris-mgmt:8182"
export POLARIS_CATALOG="$LAB_BUCKET_NAME"
export API="$POLARIS_URI/api/management/v1"
export AWS_REGION="us-east-1"
set -a; . /home/onyxia/.polaris-credentials.env; set +a
echo "Bucket/catalogue : $LAB_BUCKET_NAME"
echo "dev     : ${POLARIS_DEV_CREDENTIAL%%:*}:..."
echo "analyst : ${POLARIS_ANALYST_CREDENTIAL%%:*}:..."
[ -n "${POLARIS_SILVER_READER_CREDENTIAL:-}" ] && echo "silver_reader : ${POLARIS_SILVER_READER_CREDENTIAL%%:*}:..."
true
