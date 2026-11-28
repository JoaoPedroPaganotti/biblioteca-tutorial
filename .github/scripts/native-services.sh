#!/usr/bin/env bash
set -euo pipefail
trap 'redis-server --version || true; ls -la "$EVIDENCE_DIR"; tail -n 60 "$EVIDENCE_DIR"/*.log 2>/dev/null || true; sudo journalctl -u redis-server --no-pager -n 60 || true' ERR

mkdir -p "$EVIDENCE_DIR" "$RUNNER_TEMP/fila-bin" "$RUNNER_TEMP/rustfs-data" "$RUNNER_TEMP/redis-queue"

curl -fsSL https://packages.redis.io/gpg | gpg --dearmor | sudo tee /usr/share/keyrings/redis-archive-keyring.gpg >/dev/null
echo 'deb [signed-by=/usr/share/keyrings/redis-archive-keyring.gpg] https://packages.redis.io/deb noble main' | sudo tee /etc/apt/sources.list.d/redis.list
sudo mkdir -p /usr/share/postgresql-common/pgdg
curl -fsSL https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo tee /usr/share/postgresql-common/pgdg/apt.postgresql.org.asc >/dev/null
echo 'deb [signed-by=/usr/share/postgresql-common/pgdg/apt.postgresql.org.asc] https://apt.postgresql.org/pub/repos/apt noble-pgdg main' | sudo tee /etc/apt/sources.list.d/pgdg.list
sudo apt-get update -qq
REDIS_VERSION=$(apt-cache madison redis-server | awk '$3 ~ /(^|:)8\./ && !found { print $3; found=1 }')
test -n "$REDIS_VERSION"
sudo apt-get install -y postgresql-18 "redis=$REDIS_VERSION" "redis-server=$REDIS_VERSION" "redis-tools=$REDIS_VERSION"

while read -r version name rest; do
  sudo pg_ctlcluster "$version" "$name" stop || true
done < <(pg_lsclusters --no-header)
if [ ! -d /etc/postgresql/18/main ]; then
  sudo pg_createcluster 18 main --port 5432
fi
sudo pg_conftool 18 main set port 5432
sudo pg_ctlcluster 18 main start
sudo -u postgres psql -v ON_ERROR_STOP=1 -c "CREATE ROLE biblioteca LOGIN PASSWORD 'biblioteca';"
sudo -u postgres createdb -O biblioteca biblioteca
sudo -u postgres createdb -O biblioteca biblioteca_test

sudo systemctl restart redis-server
redis-server --bind 127.0.0.1 --port 6380 --daemonize yes --appendonly yes --maxmemory-policy noeviction --dir "$RUNNER_TEMP/redis-queue" --logfile "$EVIDENCE_DIR/fila.log"
for port in 6379 6380; do
  for attempt in $(seq 1 50); do
    if redis-cli -p "$port" PING; then break; fi
    sleep 0.2
  done
  redis-cli -p "$port" PING
done
sudo journalctl -u redis-server --no-pager -n 80 >"$EVIDENCE_DIR/redis.log"
redis-cli -p 6379 MODULE LIST
test -n "$(redis-cli -p 6379 --raw COMMAND INFO FT.CREATE)"
redis-cli -p 6380 PING

curl -fsSL --retry 3 https://github.com/rustfs/rustfs/releases/download/1.0.5/rustfs-linux-x86_64-gnu-v1.0.5.zip -o "$RUNNER_TEMP/rustfs.zip"
unzip -q "$RUNNER_TEMP/rustfs.zip" -d "$RUNNER_TEMP/fila-bin"
chmod +x "$RUNNER_TEMP/fila-bin/rustfs"
RUSTFS_ACCESS_KEY=biblioteca RUSTFS_SECRET_KEY=biblioteca-secreta nohup "$RUNNER_TEMP/fila-bin/rustfs" "$RUNNER_TEMP/rustfs-data" >"$EVIDENCE_DIR/rustfs.log" 2>&1 &
echo $! >"$EVIDENCE_DIR/rustfs.pid"

curl -fsSL --retry 3 https://github.com/axllent/mailpit/releases/download/v1.34.0/mailpit-linux-amd64.tar.gz -o "$RUNNER_TEMP/mailpit.tar.gz"
tar -xzf "$RUNNER_TEMP/mailpit.tar.gz" -C "$RUNNER_TEMP/fila-bin" mailpit
nohup "$RUNNER_TEMP/fila-bin/mailpit" --listen 127.0.0.1:8025 >"$EVIDENCE_DIR/mailpit.log" 2>&1 &
echo $! >"$EVIDENCE_DIR/mailpit.pid"

for endpoint in http://127.0.0.1:9000/health/ready http://127.0.0.1:8025/readyz; do
  ready=false
  for attempt in $(seq 1 60); do
    if curl -fsS "$endpoint" >/dev/null; then ready=true; break; fi
    sleep 1
  done
  if [ "$ready" != true ]; then cat "$EVIDENCE_DIR"/*.log; exit 1; fi
done
for bucket in biblioteca biblioteca-test; do
  AWS_ACCESS_KEY_ID=biblioteca AWS_SECRET_ACCESS_KEY=biblioteca-secreta AWS_DEFAULT_REGION=us-east-1 aws --endpoint-url http://127.0.0.1:9000 s3api create-bucket --bucket "$bucket"
done
printf 'PostgreSQL: '; psql --version
printf 'Redis: '; redis-server --version
"$RUNNER_TEMP/fila-bin/mailpit" --version
printf 'RustFS: 1.0.5\nBun: '; bun --version
