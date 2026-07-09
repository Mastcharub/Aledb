#!/usr/bin/env python3
from pathlib import Path

def load_env(path=".env"):
    env = {}
    p = Path(path)
    if not p.exists():
        return env
    for line in p.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        env[key.strip()] = value.strip()
    return env

env = load_env()

SHARD_COUNT = int(env.get("SHARD_COUNT", 2))
FOLLOWERS_PER_SHARD = int(env.get("FOLLOWERS_PER_SHARD", 2))
GATEWAY_PORT = env.get("GATEWAY_PORT", "4000")
SEGMENT_MAX_MB = env.get("SEGMENT_MAX_MB", "16")
SHARD_KEY = env.get("SHARD_KEY", "tenant_id")

lines = []

lines.extend([
"x-follower: &follower-base",
"  build:",
"    context: ./aledb",
"  environment:",
"    ROLE: follower",
"    PORT: 3000",
"    SYNC_INTERVAL_SECS: 5",
"    SEGMENT_DIR: /data/segments",
f"    SEGMENT_MAX_MB: {SEGMENT_MAX_MB}",
"  networks:",
"    - aledb",
"",
"services:"
])

leader_urls = []

for shard in range(SHARD_COUNT):
    leader = f"shard{shard}-leader"
    leader_urls.append(f"http://{leader}:3000")
    lines.extend([
        "",
        f"  {leader}:",
        "    build:",
        "      context: ./aledb",
        f"    container_name: {leader}",
        "    environment:",
        "      ROLE: leader",
        "      PORT: 3000",
        "      SEGMENT_DIR: /data/segments",
        f"      SEGMENT_MAX_MB: {SEGMENT_MAX_MB}",
        "      AUTOLOAD_PATH: /data/init.json",
        f"      SHARD_KEY: {SHARD_KEY}",
        f"      SHARD_INDEX: {shard}",
        f"      SHARD_TOTAL: {SHARD_COUNT}",
        "    networks:",
        "      - aledb",
    ])
    for follower in range(1, FOLLOWERS_PER_SHARD + 1):
        follower_name = f"shard{shard}-follower{follower}"
        lines.extend([
            "",
            f"  {follower_name}:",
            "    <<: *follower-base",
            f"    container_name: {follower_name}",
            "    environment:",
            "      ROLE: follower",
            "      PORT: 3000",
            f"      LEADER_URL: http://{leader}:3000",
            "      SYNC_INTERVAL_SECS: 5",
            "      SEGMENT_DIR: /data/segments",
            f"      SEGMENT_MAX_MB: {SEGMENT_MAX_MB}",
            "    depends_on:",
            f"      - {leader}",
        ])

lines.extend([
    "",
    "  gateway:",
    "    build:",
    "      context: ./gateway",
    "      dockerfile: Dockerfile.gateway",
    "    container_name: aledb-gateway",
    "    environment:",
    f"      GATEWAY_PORT: {GATEWAY_PORT}",
    f"      SHARD_KEY: {SHARD_KEY}",
    f"      SHARD_LEADERS: {','.join(leader_urls)}",
    "    volumes:",
    "      - gateway-data:/data",
    "    ports:",
    f'      - "{GATEWAY_PORT}:{GATEWAY_PORT}"',
    "    depends_on:"
])

for shard in range(SHARD_COUNT):
    lines.append(f"      - shard{shard}-leader")

lines.extend([
    "    networks:",
    "      - aledb",
    "",
    "networks:",
    "  aledb:",
    "    driver: bridge",
    "",
    "volumes:",
    "  gateway-data:",
    ""
])

Path("docker-compose.yml").write_text("\n".join(lines))

total = SHARD_COUNT * (1 + FOLLOWERS_PER_SHARD) + 1

print(
    f"Creato docker-compose.yml con "
    f"{SHARD_COUNT} shard, "
    f"{FOLLOWERS_PER_SHARD} follower per shard "
    f"({total} container totali incluso il gateway)."
)