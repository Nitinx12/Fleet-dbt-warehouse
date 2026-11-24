#!/usr/bin/env bash
set -euo pipefail

_require() {
  local name="$1"
  if [[ -z "${!name:-}" ]]; then
    printf 'deploy: %s is required\n' "$name" >&2
    exit 2
  fi
}

_usage() {
  printf 'usage: %s deploy|smoke\n' "$0" >&2
}

mode="${1:-}"
case "$mode" in
  deploy|smoke) ;;
  *) _usage; exit 2 ;;
esac

_require SSH_PRIVATE_KEY
_require DEPLOY_HOST
_require DEPLOY_USER
_require DEPLOY_PATH

if [[ "$mode" == "deploy" ]]; then
  _require IMAGE_REF
fi

compose_file="${COMPOSE_FILE:-airflow/compose.yml}"
env_file="${ENV_FILE:-.env}"
smoke_url="${SMOKE_URL:-http://localhost:8080/api/v2/monitor/health}"

image_ref="$(printf '%q' "${IMAGE_REF:-}")"
stream_image_ref="$(printf '%q' "${STREAM_IMAGE_REF:-}")"
deploy_path="$(printf '%q' "$DEPLOY_PATH")"
compose_file_q="$(printf '%q' "$compose_file")"
env_file_q="$(printf '%q' "$env_file")"
smoke_url_q="$(printf '%q' "$smoke_url")"

umask 077
key_file="$(mktemp)"
trap 'rm -f "$key_file"' EXIT
printf '%s\n' "$SSH_PRIVATE_KEY" > "$key_file"
chmod 600 "$key_file"

ssh_options=(
  -i "$key_file"
  -o BatchMode=yes
  -o StrictHostKeyChecking=accept-new
  -o UserKnownHostsFile=/dev/null
  -o ConnectTimeout=15
)

if [[ "$mode" == "deploy" ]]; then
  ssh "${ssh_options[@]}" "$DEPLOY_USER@$DEPLOY_HOST" "bash -s" <<REMOTE
set -euo pipefail
export AIRFLOW_IMAGE=$image_ref
export STREAM_IMAGE=$stream_image_ref
cd $deploy_path
compose=(docker compose --env-file $env_file_q -f $compose_file_q)
"\${compose[@]}" pull
"\${compose[@]}" up -d --no-build --remove-orphans
"\${compose[@]}" ps
REMOTE
else
  ssh "${ssh_options[@]}" "$DEPLOY_USER@$DEPLOY_HOST" "bash -s" <<REMOTE
set -euo pipefail
cd $deploy_path
compose=(docker compose --env-file $env_file_q -f $compose_file_q)
"\${compose[@]}" ps
"\${compose[@]}" exec -T airflow-api-server curl --fail --silent --show-error $smoke_url_q
scheduler_host="\$("\${compose[@]}" exec -T airflow-scheduler hostname)"
"\${compose[@]}" exec -T airflow-scheduler airflow jobs check --job-type SchedulerJob --hostname "\$scheduler_host"
REMOTE
fi
