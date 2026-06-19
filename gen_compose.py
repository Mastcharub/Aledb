#!/usr/bin/env python3
"""
Genera docker-compose.yml a partire da .env.

Variabili lette da .env:
  SHARD_COUNT          numero di shard (default: 2)
  FOLLOWERS_PER_SHARD   numero di follower per shard (default: 2)
  GATEWAY_PORT          porta esposta del gateway (default: 4000)
  SEGMENT_MAX_MB        dimensione massima segment (default: 16)
  SHARD_KEY             campo usato per lo sharding (default: tenant_id)

Uso:
  python3 generate_compose.py
  docker compose up --build
"""

import os
from pathlib import Path

def load_env(path=".env"):
    env = {}
    p = Path(path)
    if p.exists():
        for line in p.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            env[k.strip()] = v.strip()
    return env

env = load_env()

SHARD_COUNT          = int(env.get("SHARD_COUNT", 2))
FOLLOWERS_PER_SHARD   = int(env.get("FOLLOWERS_PER_SHARD", 2))
GATEWAY_PORT          = env.get("GATEWAY_PORT", "4000")
SEGMENT_MAX_MB        = env.get("SEGMENT_MAX_MB", "16")
SHARD_KEY             = env.get("SHARD_KEY", "tenant_id")

lines = []

lines.append("x-follower: &follower-base")
lines.append("  build:")
lines.append("    context: ./aledb")
lines.append("  environment:")
lines.append("    ROLE: follower")
lines.append("    PORT: 3000")
lines.append("    SYNC_INTERVAL_SECS: 5")
lines.append("    SEGMENT_DIR: /data/segments")
lines.append(f"    SEGMENT_MAX_MB: {SEGMENT_MAX_MB}")
lines.append("  networks:")
lines.append("    - aledb")
lines.append("")
lines.append("services:")
lines.append("")

leader_urls = []

for s in range(SHARD_COUNT):
    leader_name = f"shard{s}-leader"
    leader_urls.append(f"http://{leader_name}:3000")

    lines.append(f"  # ── Shard {s} ──────────────────────────────────────────────────────────────")
    lines.append("")
    lines.append(f"  {leader_name}:")
    lines.append("    build:")
    lines.append("      context: ./aledb")
    lines.append(f"    container_name: {leader_name}")
    lines.append("    environment:")
    lines.append("      ROLE: leader")
    lines.append("      PORT: 3000")
    lines.append("      SEGMENT_DIR: /data/segments")
    lines.append(f"      SEGMENT_MAX_MB: {SEGMENT_MAX_MB}")
    lines.append("      AUTOLOAD_PATH: /data/init.json")
    lines.append(f"      SHARD_KEY: {SHARD_KEY}")
    lines.append(f"      SHARD_INDEX: {s}")
    lines.append(f"      SHARD_TOTAL: {SHARD_COUNT}")
    lines.append("    networks:")
    lines.append("      - aledb")
    lines.append("")

    for f in range(1, FOLLOWERS_PER_SHARD + 1):
        follower_name = f"shard{s}-follower{f}"
        lines.append(f"  {follower_name}:")
        lines.append("    <<: *follower-base")
        lines.append(f"    container_name: {follower_name}")
        lines.append("    environment:")
        lines.append("      ROLE: follower")
        lines.append("      PORT: 3000")
        lines.append(f"      LEADER_URL: http://{leader_name}:3000")
        lines.append("      SYNC_INTERVAL_SECS: 5")
        lines.append("      SEGMENT_DIR: /data/segments")
        lines.append(f"      SEGMENT_MAX_MB: {SEGMENT_MAX_MB}")
        lines.append("    depends_on:")
        lines.append(f"      - {leader_name}")
        lines.append("")

lines.append("  # ── Gateway ───────────────────────────────────────────────────────────────")
lines.append("")
lines.append("  gateway:")
lines.append("    build:")
lines.append("      context: ./gateway")
lines.append("      dockerfile: Dockerfile.gateway")
lines.append("    container_name: aledb-gateway")
lines.append("    environment:")
lines.append(f"      GATEWAY_PORT: {GATEWAY_PORT}")
lines.append(f"      SHARD_KEY: {SHARD_KEY}")
lines.append(f"      SHARD_LEADERS: {','.join(leader_urls)}")
lines.append("    ports:")
lines.append(f'      - "{GATEWAY_PORT}:{GATEWAY_PORT}"')
lines.append("    depends_on:")
for s in range(SHARD_COUNT):
    lines.append(f"      - shard{s}-leader")
lines.append("    networks:")
lines.append("      - aledb")
lines.append("")
lines.append("networks:")
lines.append("  aledb:")
lines.append("    driver: bridge")
lines.append("")

Path("docker-compose.yml").write_text("\n".join(lines))

total_containers = SHARD_COUNT * (1 + FOLLOWERS_PER_SHARD) + 1
print(f"docker-compose.yml generato: {SHARD_COUNT} shard, {FOLLOWERS_PER_SHARD} follower/shard, "
      f"{total_containers} container totali (gateway incluso)")