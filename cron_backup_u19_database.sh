#!/usr/bin/env bash
# cron_backup_u19_database.sh
# ---------------------------
# Nightly wrapper around dump_prod.sh:
# - creates a timestamped SQL backup
# - updates a stable "latest" backup filename
# - deletes backups older than a retention window
#
# dump_prod.sh now runs mariadb-dump inside Docker and expects credentials in a
# MySQL client defaults file such as ./.my.cnf or ~/.my.cnf.
#
# Usage:
#   ./cron_backup_u19_database.sh [OPTIONS] [-- DUMP_PROD_ARGS...]
#
# Options:
#   --backup-dir DIR       Directory for SQL backups
#                          (default: $HOME/u19_sql_backups)
#   --retention-days N     Delete timestamped backups older than N days
#                          (default: 14)
#   --latest-name NAME     Stable latest backup filename
#                          (default: backup_u19_database_latest.sql)
#   -h, --help             Show help
#
# Any args after -- are passed directly to dump_prod.sh.
#
# Example cron (nightly at 2:15am):
# 15 2 * * * cd /Users/user/code/U19-pipeline-python && ./cron_backup_u19_database.sh -- --config /Users/ct5868/code/U19-pipeline-python/dj_local_conf.json >> /tmp/u19_backup.log 2>&1
#
# Example .my.cnf:
#   [client]
#   user=
#   password=
#   ssl
#   skip-ssl-verify-server-cert

set -euo pipefail

BACKUP_DIR="${HOME}/u19_sql_backups"
RETENTION_DAYS=14
LATEST_NAME="backup_u19_database_latest.sql"

DUMP_ARGS=()
AFTER_SEP=false

while [[ $# -gt 0 ]]; do
    if ! $AFTER_SEP; then
        case "$1" in
            --backup-dir)
                BACKUP_DIR="$2"
                shift 2
                continue
                ;;
            --retention-days)
                RETENTION_DAYS="$2"
                shift 2
                continue
                ;;
            --latest-name)
                LATEST_NAME="$2"
                shift 2
                continue
                ;;
            --)
                AFTER_SEP=true
                shift
                continue
                ;;
            -h|--help)
                sed -n '/^# Usage:/,/^set -euo pipefail/{ /^#/{ s/^# \{0,1\}//; p } }' "$0"
                exit 0
                ;;
            *)
                # Allow direct passthrough without requiring -- separator.
                DUMP_ARGS+=("$1")
                shift
                continue
                ;;
        esac
    else
        DUMP_ARGS+=("$1")
        shift
    fi
done

log() { echo "$(date '+%Y-%m-%d %H:%M:%S')  INFO     $*"; }
err() { echo "$(date '+%Y-%m-%d %H:%M:%S')  ERROR    $*" >&2; }

if [[ ! "$RETENTION_DAYS" =~ ^[0-9]+$ ]]; then
    err "--retention-days must be a non-negative integer."
    exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DUMP_SCRIPT="${SCRIPT_DIR}/dump_prod.sh"

if [[ ! -x "$DUMP_SCRIPT" ]]; then
    if [[ -f "$DUMP_SCRIPT" ]]; then
        chmod +x "$DUMP_SCRIPT"
    else
        err "dump_prod.sh not found at ${DUMP_SCRIPT}."
        exit 1
    fi
fi

mkdir -p "$BACKUP_DIR"

TIMESTAMP="$(date +%Y%m%d_%H%M%S)"
DATED_FILE="${BACKUP_DIR}/backup_u19_database_${TIMESTAMP}.sql"
LATEST_FILE="${BACKUP_DIR}/${LATEST_NAME}"
TMP_FILE="${DATED_FILE}.tmp"

cleanup_tmp() {
    [[ -f "$TMP_FILE" ]] && rm -f "$TMP_FILE"
}
trap cleanup_tmp EXIT

log "Starting nightly SQL backup."
log "Backup directory: ${BACKUP_DIR}"
log "Retention days: ${RETENTION_DAYS}"

if ((${#DUMP_ARGS[@]})); then
    "$DUMP_SCRIPT" "${DUMP_ARGS[@]}" --out "$TMP_FILE"
else
    "$DUMP_SCRIPT" --out "$TMP_FILE"
fi

mv "$TMP_FILE" "$DATED_FILE"
cp -f "$DATED_FILE" "$LATEST_FILE"

# Delete only timestamped backup files older than retention window.
find "$BACKUP_DIR" \
    -maxdepth 1 \
    -type f \
    -name 'backup_u19_database_*.sql' \
    -mtime "+${RETENTION_DAYS}" \
    -print \
    -delete || true

SIZE="$(du -sh "$DATED_FILE" | cut -f1)"
log "Backup complete: ${DATED_FILE} (${SIZE})"
log "Latest copy: ${LATEST_FILE}"
