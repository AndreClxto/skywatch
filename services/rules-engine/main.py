import json
import logging
import os
import signal
import socket

import redis
import yaml

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("rules-engine")

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
RULES_FILE = os.getenv("RULES_FILE", "rules.yaml")
DEDUP_TTL_SECONDS = int(os.getenv("DEDUP_TTL_SECONDS", str(6 * 3600)))
STREAM_IN = "forecasts"
STREAM_OUT = "alerts"
GROUP = "rules-engine"
CONSUMER = os.getenv("HOSTNAME", socket.gethostname())

OPERATORS = {
    ">=": lambda value, threshold: value >= threshold,
    "<=": lambda value, threshold: value <= threshold,
}

running = True


def handle_sigterm(signum, frame):
    global running
    log.info("Received signal %s, finishing current work and shutting down", signum)
    running = False


def ensure_group(r: redis.Redis) -> None:
    try:
        r.xgroup_create(STREAM_IN, GROUP, id="0", mkstream=True)
        log.info("Created consumer group %s", GROUP)
    except redis.ResponseError as e:
        if "BUSYGROUP" not in str(e):
            raise


def evaluate(forecast: dict, rules: list) -> list:
    hourly = forecast["hourly"]
    times = hourly["time"]
    alerts = []
    for rule in rules:
        values = hourly.get(rule["metric"])
        if not values:
            continue
        for i in range(min(rule["within_hours"], len(values))):
            value = values[i]
            if value is not None and OPERATORS[rule["operator"]](value, rule["threshold"]):
                alerts.append({
                    "rule_id": rule["id"],
                    "message": rule["message"].format(
                        city=forecast["city"], time=times[i][11:16], value=round(value)
                    ),
                })
                break  # first matching hour is enough
    return alerts


def process(r: redis.Redis, slug: str, rules: list) -> None:
    raw = r.get(f"forecast:{slug}")
    if raw is None:
        log.warning("No forecast found for %s (expired?)", slug)
        return
    for alert in evaluate(json.loads(raw), rules):
        dedup_key = f"alerted:{slug}:{alert['rule_id']}"
        if r.set(dedup_key, "1", nx=True, ex=DEDUP_TTL_SECONDS):
            r.xadd(STREAM_OUT, {"slug": slug, **alert}, maxlen=1000, approximate=True)
            log.info("ALERT %s", alert["message"])
        else:
            log.info("Skipping duplicate alert %s for %s", alert["rule_id"], slug)


def main() -> None:
    signal.signal(signal.SIGTERM, handle_sigterm)
    signal.signal(signal.SIGINT, handle_sigterm)

    r = redis.Redis.from_url(REDIS_URL, decode_responses=True)
    with open(RULES_FILE) as f:
        rules = yaml.safe_load(f)["rules"]
    ensure_group(r)
    log.info("Consumer %s listening on stream %s", CONSUMER, STREAM_IN)

    while running:
        response = r.xreadgroup(GROUP, CONSUMER, {STREAM_IN: ">"}, count=10, block=5000)
        for _stream, messages in response or []:
            for msg_id, fields in messages:
                try:
                    process(r, fields["slug"], rules)
                    r.xack(STREAM_IN, GROUP, msg_id)
                except Exception:
                    log.exception("Failed to process event %s; leaving it pending", msg_id)

    log.info("Shutdown complete")


if __name__ == "__main__":
    main()