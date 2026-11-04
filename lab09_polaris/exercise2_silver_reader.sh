#!/usr/bin/env bash
# Lab 9 - Exercice 2 : role silver_reader limite au namespace silver.
#
#   principal  silver_reader
#     -> principal role  silver_data_analyst
#       -> catalog role  silver_reader   (privileges sur le namespace silver UNIQUEMENT)
#
# Usage : source lab09_polaris/env.sh && bash lab09_polaris/exercise2_silver_reader.sh
set -euo pipefail

CAT="$POLARIS_CATALOG"
CRED_FILE=/home/onyxia/.polaris-credentials.env

TOKEN=$(curl -fsS "$POLARIS_URI/api/catalog/v1/oauth/tokens" \
  -d grant_type=client_credentials -d client_id=root -d client_secret=s3cr3t \
  -d scope=PRINCIPAL_ROLE:ALL | jq -r .access_token)

call() { # call METHOD URL [JSON]
  curl -sS -o /tmp/polaris.out -w '%{http_code}' -X "$1" \
    -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
    ${3:+-d "$3"} "$2"
}

# 1. Catalog role silver_reader
code=$(call POST "$API/catalogs/$CAT/catalog-roles" '{"catalogRole":{"name":"silver_reader"}}')
echo "catalog role silver_reader : HTTP $code (409 = existe deja)"

# 2. Privileges au niveau du namespace silver uniquement
for priv in NAMESPACE_LIST NAMESPACE_READ_PROPERTIES TABLE_LIST TABLE_READ_PROPERTIES TABLE_READ_DATA; do
  code=$(call PUT "$API/catalogs/$CAT/catalog-roles/silver_reader/grants" \
    "{\"grant\":{\"type\":\"namespace\",\"namespace\":[\"silver\"],\"privilege\":\"$priv\"}}")
  echo "grant $priv sur silver : HTTP $code"
done

# 3. Principal role + rattachement du catalog role
code=$(call POST "$API/principal-roles" '{"principalRole":{"name":"silver_data_analyst"}}')
echo "principal role silver_data_analyst : HTTP $code"
code=$(call PUT "$API/principal-roles/silver_data_analyst/catalog-roles/$CAT" '{"catalogRole":{"name":"silver_reader"}}')
echo "silver_reader -> silver_data_analyst : HTTP $code"

# 4. Principal dedie (les credentials ne sont renvoyes qu'a la creation)
code=$(call POST "$API/principals" '{"principal":{"name":"silver_reader"},"credentialRotationRequired":false}')
echo "principal silver_reader : HTTP $code"
if [ "$code" = "201" ] || [ "$code" = "200" ]; then
  CRED=$(jq -r '"\(.credentials.clientId):\(.credentials.clientSecret)"' /tmp/polaris.out)
  sed -i '/^POLARIS_SILVER_READER_CREDENTIAL=/d' "$CRED_FILE"
  echo "POLARIS_SILVER_READER_CREDENTIAL=\"$CRED\"" >> "$CRED_FILE"
  echo "Credential ajoute dans $CRED_FILE"
fi
code=$(call PUT "$API/principals/silver_reader/principal-roles" '{"principalRole":{"name":"silver_data_analyst"}}')
echo "silver_data_analyst -> silver_reader : HTTP $code"

# 5. Verification
echo; echo "Grants de silver_reader :"
curl -fsS -H "Authorization: Bearer $TOKEN" "$API/catalogs/$CAT/catalog-roles/silver_reader/grants" | jq .
echo ">> Recharge l'env : source lab09_polaris/env.sh"
