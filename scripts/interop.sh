#!/usr/bin/env bash
# Runs hookgate (Go) and hookgate-nest side by side on ONE database and ONE
# broker, with the pipeline split between them, and checks that they behave
# as one system:
#   - a webhook ingested by one is recognised as a duplicate by the other
#   - events relayed by one implementation are processed by the other, and
#     their messages are consumed exactly once
#   - the status timeline records which implementation did each step
#
#   GO_URL=http://localhost:8080 NEST_URL=http://localhost:3000 \
#   EXPECT_ACTORS="go/relay nest/processor nest/sink" scripts/interop.sh
set -euo pipefail
GO=${GO_URL:-http://localhost:8080}
NEST=${NEST_URL:-http://localhost:3000}
EXPECT_ACTORS=${EXPECT_ACTORS:-}
SRC="interop$RANDOM"

json() { node -e 'let d="";process.stdin.on("data",c=>d+=c).on("end",()=>{let v=JSON.parse(d);for(const k of process.argv[1].split("."))v=v?.[k];console.log(typeof v==="object"?JSON.stringify(v):v)})' "$1"; }
post() { curl -sf -X POST "$1" -H 'Content-Type: application/json' -d "$2"; }

# send <base url> <secret> <webhook id> <body> -> prints HTTP status
send() {
  local t sig
  t=$(date +%s)
  sig=$(printf '%s.%s' "$t" "$4" | openssl dgst -sha256 -hmac "$2" -hex | sed 's/^.* //')
  curl -s -o /dev/null -w '%{http_code}' -X POST "$1/v1/ingest/$SRC" \
    -H 'Content-Type: application/json' -H "Webhook-Id: $3" -H "Hookgate-Signature: t=$t,v1=$sig" -d "$4"
}

# Stats are global, so measure this run as a delta (the database may be shared).
base=$(curl -sf "$GO/v1/stats")
B_PROCESSED=$(echo "$base" | json events.processed)
B_DELIVERED=$(echo "$base" | json messages.delivered)
B_DUPS=$(echo "$base" | json ingest_duplicates)

SECRET=$(post "$NEST/v1/sources" "{\"name\":\"$SRC\"}" | json secret)
post "$GO/v1/routes" "{\"source\":\"$SRC\",\"event_type\":\"order.*\",\"topic\":\"billing\"}" >/dev/null
post "$NEST/v1/routes" "{\"source\":\"$SRC\",\"event_type\":\"*\",\"topic\":\"audit\"}" >/dev/null

N=20
for i in $(seq 1 $N); do
  body="{\"type\":\"order.created\",\"i\":$i}"
  if (( i % 2 )); then first=$GO; second=$NEST; else first=$NEST; second=$GO; fi
  code=$(send "$first" "$SECRET" "evt_$i" "$body")
  [ "$code" = 202 ] || { echo "ingest evt_$i: $code"; exit 1; }
  # The provider retries against the other implementation: must dedupe.
  code=$(send "$second" "$SECRET" "evt_$i" "$body")
  [ "$code" = 200 ] || { echo "cross-implementation dedup failed for evt_$i: $code"; exit 1; }
done
echo "ingest: $N events, $N cross-implementation duplicates rejected"

WANT_PROCESSED=$((B_PROCESSED + N))
WANT_DELIVERED=$((B_DELIVERED + N * 2))
for _ in $(seq 1 60); do
  stats=$(curl -sf "$GO/v1/stats")
  [ "$(echo "$stats" | json messages.delivered)" = "$WANT_DELIVERED" ] && [ "$(echo "$stats" | json events.processed)" = "$WANT_PROCESSED" ] && break
  sleep 1
done
echo "stats: $stats"
[ "$(echo "$stats" | json events.processed)" = "$WANT_PROCESSED" ] || { echo "not all events processed"; exit 1; }
[ "$(echo "$stats" | json messages.delivered)" = "$WANT_DELIVERED" ] || { echo "messages delivered != $WANT_DELIVERED (lost or duplicated)"; exit 1; }
[ "$(echo "$stats" | json ingest_duplicates)" = "$((B_DUPS + N))" ] || { echo "duplicates not counted"; exit 1; }
# Both implementations must report identical stats.
[ "$(curl -sf "$NEST/v1/stats" | json events)" = "$(echo "$stats" | json events)" ] || { echo "Go and NestJS disagree on stats"; exit 1; }

EVENT=$(curl -sf "$NEST/v1/events?source=$SRC&limit=1" | json events.0.id)
timeline=$(curl -sf "$GO/v1/events/$EVENT")
MSG=$(echo "$timeline" | json messages.0.id)
actors="$(echo "$timeline" | json timeline) $(curl -sf "$NEST/v1/messages/$MSG" | json timeline)"
for a in $EXPECT_ACTORS; do
  echo "$actors" | grep -q "\"$a\"" || { echo "timeline is missing actor $a: $actors"; exit 1; }
done
echo "interop OK: pipeline split across Go and NestJS; every message delivered exactly once (actors: ${EXPECT_ACTORS:-any})"
