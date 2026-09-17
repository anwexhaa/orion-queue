#!/usr/bin/env bash
#
# crash-test.sh <image> — stop a worker mid-job and count what was lost.
#
#   ./scripts/crash-test.sh orion-queue:local              # SIGKILL
#   SIGNAL=TERM ./scripts/crash-test.sh orion-queue:local  # graceful stop
#
# Reproduces game day 1 without a cluster. Chaos Mesh's pod-kill used grace
# period 0, which is SIGKILL: no drain, no cleanup, nothing runs. `docker kill`
# sends the same signal.
#
# Starts Redis, the API, a scheduler and one worker (4 threads) on a private
# network, submits slow jobs, kills the worker while it holds a full batch,
# starts a replacement, waits for everything to settle, and then reads every
# job record. Any record still `running` once the queue is empty and no lease
# is held is a job that was accepted and never finished.
#
# Run it against the code before and after a change. A run on the old code
# that loses nothing means the harness did not kill mid-job, and a clean run on
# the new code would then prove nothing.

set -uo pipefail

IMAGE="${1:?usage: crash-test.sh <image>}"
JOBS="${JOBS:-40}"
TASK_SECONDS="${TASK_SECONDS:-2}"
LEASE_SECONDS="${LEASE_SECONDS:-10}"
REAP_INTERVAL="${REAP_INTERVAL:-2}"
TIMEOUT="${TIMEOUT:-120}"
# KILL reproduces game day 1. TERM is a graceful stop, and should reap
# nothing: the worker drains, finishes its jobs and acknowledges them itself.
SIGNAL="${SIGNAL:-KILL}"

net="orion-crash-$$"
redis_c="${net}-redis"

cleanup() {
  docker rm -f "${net}-redis" "${net}-api" "${net}-scheduler" \
    "${net}-worker" "${net}-worker2" >/dev/null 2>&1
  docker network rm "$net" >/dev/null 2>&1
}
trap cleanup EXIT

common=(--network "$net" -e REDIS_HOST=redis -e WORKER_COUNT=4
        -e LEASE_SECONDS="$LEASE_SECONDS" -e REAP_INTERVAL="$REAP_INTERVAL")

rcli() { docker exec "$redis_c" redis-cli "$@" | tr -d '\r'; }

read -r -d '' CENSUS <<'EOF'
local counts = {}
for _, k in ipairs(redis.call('KEYS', 'job:*')) do
  local v = redis.call('GET', k)
  local s = v and string.match(v, '"status": "(%a+)"') or 'missing'
  counts[s] = (counts[s] or 0) + 1
end
local out = {}
for k, v in pairs(counts) do table.insert(out, k .. '=' .. v) end
table.sort(out)
return out
EOF

census() { rcli EVAL "$CENSUS" 0 | tr '\n' ' '; }
queued() { rcli ZCARD task_queue; }
leased() { rcli ZCARD task_queue:processing; }

docker network create "$net" >/dev/null
docker run -d --name "$redis_c" --network "$net" --network-alias redis redis:7-alpine >/dev/null
docker run -d --name "${net}-api" "${common[@]}" -e ORION_ROLE=api "$IMAGE" python api_main.py >/dev/null
docker run -d --name "${net}-scheduler" "${common[@]}" -e ORION_ROLE=scheduler "$IMAGE" python scheduler_main.py >/dev/null
docker run -d --name "${net}-worker" "${common[@]}" -e ORION_ROLE=worker "$IMAGE" python worker_main.py >/dev/null

for _ in $(seq 1 40); do
  docker exec "${net}-api" python -c \
    "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/ready', timeout=2)" \
    >/dev/null 2>&1 && break
  sleep 1
done

docker exec "${net}-api" python -c "
import json, urllib.request
for _ in range($JOBS):
    body = json.dumps({'task_name': 'slow_task', 'payload': {'duration': $TASK_SECONDS}}).encode()
    req = urllib.request.Request('http://127.0.0.1:8000/submit', data=body,
                                 headers={'Content-Type': 'application/json'})
    urllib.request.urlopen(req, timeout=10).read()
"
echo "image           $IMAGE"
echo "submitted       $JOBS jobs of ${TASK_SECONDS}s"

# Let the worker take a full batch before killing it.
sleep 3
echo "before kill     $(census)| leased=$(leased)"

if [ "$SIGNAL" = "TERM" ]; then
  docker stop --time 30 "${net}-worker" >/dev/null
else
  docker kill --signal=KILL "${net}-worker" >/dev/null
fi
echo "worker          SIG${SIGNAL}"
docker run -d --name "${net}-worker2" "${common[@]}" -e ORION_ROLE=worker "$IMAGE" python worker_main.py >/dev/null

start=$(date +%s)
while :; do
  q=$(queued); l=$(leased)
  if [ "${q:-1}" = "0" ] && [ "${l:-1}" = "0" ]; then break; fi
  if [ $(( $(date +%s) - start )) -gt "$TIMEOUT" ]; then echo "timed out waiting to settle"; break; fi
  sleep 1
done
# The replacement may still be finishing jobs it has not yet acknowledged on
# code without leases; give it one task's worth of time.
sleep $(( TASK_SECONDS + 3 ))
settled=$(( $(date +%s) - start ))

final=$(census)
lost=$(echo "$final" | sed -n 's/.*running=\([0-9]*\).*/\1/p')
recovered=$(docker exec "${net}-scheduler" python -c "
import urllib.request
body = urllib.request.urlopen('http://127.0.0.1:8000/metrics', timeout=3).read().decode()
print(next((l.split()[-1] for l in body.splitlines() if l.startswith('orion_leases_expired_total ')), 'n/a'))
" 2>/dev/null)

echo "settled after   ${settled}s"
echo "final           $final"
echo "leases reaped   ${recovered:-n/a}"
echo "LOST            ${lost:-0}"
