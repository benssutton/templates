#!/bin/sh
# Compose one-shot: idempotent SEMP v2 broker setup.
# ENABLE_AMQP=true    -> PATCH the VPN so AMQP plain text listens on 5672
#                         (the default VPN already does on 10.26).
# PROVISION_QUEUE=true -> exclusive durable SOLACE_QUEUE + SOLACE_TOPIC subscription
#                         (the app provisions the same at startup).
# ALREADY_EXISTS counts as success, so re-running is harmless.
set -eu
SEMP="${SEMP_URL:-http://solace:8080}/SEMP/v2/config/msgVpns/${SOLACE_VPN:-default}"
AUTH="${SEMP_USERNAME:-admin}:${SEMP_PASSWORD:-admin}"

call() { # method path json -> prints "body\nstatus"; fails if SEMP is unreachable
  curl -sS -u "$AUTH" -H 'content-type: application/json' -X "$1" "$SEMP$2" -d "$3" -w '\n%{http_code}'
}

unreachable() {
  echo "solace-init: SEMP unreachable at $SEMP" >&2
  exit 1
}

ensure() { # path json
  out=$(call POST "$1" "$2") || unreachable
  code=$(printf '%s' "$out" | tail -n1)
  if [ "$code" = 200 ]; then
    echo "created $1"
  elif [ "$code" = 400 ] && printf '%s' "$out" | grep -q ALREADY_EXISTS; then
    echo "exists  $1"
  else
    printf 'SEMP POST %s failed:\n%s\n' "$1" "$out" >&2
    exit 1
  fi
}

if [ "${ENABLE_AMQP:-false}" = true ]; then
  out=$(call PATCH "" '{"serviceAmqpPlainTextEnabled":true,"serviceAmqpPlainTextListenPort":5672}') || unreachable
  [ "$(printf '%s' "$out" | tail -n1)" = 200 ] || { printf 'AMQP enable failed:\n%s\n' "$out" >&2; exit 1; }
  echo "AMQP plain text enabled on 5672"
fi

if [ "${PROVISION_QUEUE:-false}" = true ]; then
  q_enc=$(printf '%s' "$SOLACE_QUEUE" | sed 's|/|%2F|g')
  ensure /queues "{\"queueName\":\"$SOLACE_QUEUE\",\"accessType\":\"exclusive\",\"permission\":\"consume\",\"ingressEnabled\":true,\"egressEnabled\":true}"
  ensure "/queues/$q_enc/subscriptions" "{\"subscriptionTopic\":\"$SOLACE_TOPIC\"}"
fi
echo "solace-init done"
