#!/bin/sh
# Kreira S3 korisnike sa najmanjim privilegijama. Idempotentno - moze da se pokrene vise puta.
set -e
# Bitnami image pri prvom pokretanju nakratko podigne privremeni server (za kreiranje bucket-a),
# pa sacekamo da pravi server bude spreman.
# Privremeni server moze da se ugasi i usred podesavanja, pa se svaki poziv ponavlja dok ne uspe.
retry() {
  for i in $(seq 1 30); do
    "$@" && return 0
    sleep 2
  done
  echo "nije uspelo: $*" >&2
  return 1
}
retry mc alias set local http://minio:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null 2>&1
retry mc admin info local >/dev/null
retry mc mb --ignore-existing local/warehouse

has_policy() {  # attach javlja gresku i kada je politika vec dodeljena, pa se proverava stvarno stanje
  mc admin policy attach local "$2" --user "$1" >/dev/null 2>&1 || true
  mc admin user info local "$1" 2>/dev/null | grep -q "PolicyName: $2"
}

add_user() {  # $1 = korisnik, $2 = lozinka, $3 = politika (readwrite | readonly | explorer-ml)
  retry mc admin user add local "$1" "$2" >/dev/null
  retry has_policy "$1" "$3"
  echo "korisnik $1 -> $3"
}

add_user "$CATALOG_S3_ACCESS_KEY"  "$CATALOG_S3_SECRET_KEY"  readwrite
add_user "$SPARK_S3_ACCESS_KEY"    "$SPARK_S3_SECRET_KEY"    readwrite
retry mc admin policy create local explorer-ml /setup/explorer-policy.json >/dev/null
add_user "$EXPLORER_S3_ACCESS_KEY" "$EXPLORER_S3_SECRET_KEY" explorer-ml
add_user "$TRINO_S3_ACCESS_KEY"    "$TRINO_S3_SECRET_KEY"    readonly
