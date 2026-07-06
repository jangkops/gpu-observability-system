#!/usr/bin/env bash
# 진입점: 부팅 1회 수집 → cron(수집 05:30 / reconciliation 05:45 KST) → exporter 상시
set -e
mkdir -p /data
export OUT_FILE="${OUT_FILE:-/data/s3_metrics.prom}"
export OWNER_TSV="${OWNER_TSV:-/data/project_owner.tsv}"
# 부팅 직후 1회 Layer1 수집
/usr/local/bin/python3 /app/collector.py || echo "initial collect failed (cron will retry)"
# cron: 매일 KST 05:30 수집(UTC 20:30), 05:45 reconciliation(UTC 20:45)
cat > /etc/cron.d/s3collect <<EOF
PATH=/usr/local/bin:/usr/bin:/bin
30 20 * * * root cd /app && OUT_FILE=$OUT_FILE OWNER_TSV=$OWNER_TSV /usr/local/bin/python3 /app/collector.py >> /var/log/cron.log 2>&1
45 20 * * * root cd /app && OWNER_TSV=$OWNER_TSV /usr/local/bin/python3 /app/bootstrap_counter.py >> /var/log/cron.log 2>&1
EOF
chmod 0644 /etc/cron.d/s3collect
cron
# exporter (포그라운드)
exec python3 /app/s3_project_exporter.py
