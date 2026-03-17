#!/usr/bin/env bash
# dump_prod.sh
# ------------
# Dump the first N rows from each u19_ production schema using mariadb-dump
# executed inside Docker,
# rewrite every DB-name reference from the prod prefix to the test prefix,
# and write the combined result to a single SQL file.
#
# The output file is suitable for direct ingestion by ingest_test.sh.
#
# Requires: docker, jq, and a MySQL client defaults file (.my.cnf)
#
# Example .my.cnf:
#   [client]
#   user=
#   password=
#   ssl
#   skip-ssl-verify-server-cert
#
# Usage:
#   ./dump_prod.sh --out <file> [OPTIONS]
#
# Options:
#   -o, --out PATH           Output SQL file (required)
#   -c, --config PATH        DataJoint JSON config file
#                            (default: dj_local_conf.json, then ~/.datajoint_config.json)
#       --mysql-cnf PATH     MySQL client defaults file to mount into the container
#                            (default: ./.my.cnf, then ~/.my.cnf)
#       --docker-image IMG   Docker image containing MariaDB client tools
#                            (default: mariadb:11)
#   -s, --schemas "a b c"   Space-separated short schema names (default: all)
#   -l, --limit N            Max rows per table (default: 1000)
#       --host HOST          Prod server host (default: database.host from config)
#       --port PORT          Prod server port (default: database.port from config)
#       --prod-prefix PREFIX Override prod DB prefix
#       --test-prefix PREFIX Override test DB prefix
#   -n, --dry-run            Print commands without executing
#   -h, --help               Show this help
#
# Examples:
#   # Dump all schemas
#   ./dump_prod.sh --config dj_local_conf.json --out /tmp/u19_test.sql
#
#   # Dump only lab and subject, 500 rows each
#   ./dump_prod.sh --config dj_local_conf.json \
#       --out /tmp/u19_test.sql --schemas "lab subject" --limit 500
#
#   # Dry run — show the Docker mariadb-dump commands without executing
#   ./dump_prod.sh --config dj_local_conf.json --out /tmp/u19_test.sql --dry-run
#
#   # Use an explicit client defaults file
#   ./dump_prod.sh --config dj_local_conf.json --mysql-cnf "$HOME/.my.cnf" \
#       --out /tmp/u19_test.sql

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------

SCHEMA_NAMES="action acquisition behavior lab subject scheduler task rig_maintenance nwb_production recording"
LIMIT=1000
CONFIG_PATH=""
OUT_FILE=""
DRY_RUN=false
MYSQL_CNF_PATH=""
DOCKER_IMAGE="mariadb:11"

HOST_OVERRIDE=""
PORT_OVERRIDE=""
PROD_PREFIX_OVERRIDE=""
TEST_PREFIX_OVERRIDE=""

# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------

while [[ $# -gt 0 ]]; do
    case "$1" in
        -o|--out)            OUT_FILE="$2";             shift 2 ;;
        -c|--config)         CONFIG_PATH="$2";          shift 2 ;;
           --mysql-cnf)      MYSQL_CNF_PATH="$2";       shift 2 ;;
           --docker-image)   DOCKER_IMAGE="$2";         shift 2 ;;
        -s|--schemas)        SCHEMA_NAMES="$2";         shift 2 ;;
        -l|--limit)          LIMIT="$2";                shift 2 ;;
           --host)           HOST_OVERRIDE="$2";        shift 2 ;;
           --port)           PORT_OVERRIDE="$2";        shift 2 ;;
           --prod-prefix)    PROD_PREFIX_OVERRIDE="$2"; shift 2 ;;
           --test-prefix)    TEST_PREFIX_OVERRIDE="$2"; shift 2 ;;
        -n|--dry-run)        DRY_RUN=true;              shift   ;;
        -h|--help)           sed -n '/^# Usage:/,/^[^#]/{ /^#/{ s/^# \{0,1\}//; p } }' "$0"; exit 0 ;;
        *) echo "Unknown option: $1" >&2; exit 1 ;;
    esac
done

if [[ -z "$OUT_FILE" ]]; then
    echo "ERROR: --out <file> is required." >&2
    exit 1
fi

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

log() { echo "$(date '+%Y-%m-%d %H:%M:%S')  INFO     $*"; }
err() { echo "$(date '+%Y-%m-%d %H:%M:%S')  ERROR    $*" >&2; }

require_binary() {
    if ! command -v "$1" &>/dev/null; then
        err "'$1' not found on PATH."
        err "  Install the missing dependency and retry."
        exit 1
    fi
}

require_binary jq
require_binary docker

# ---------------------------------------------------------------------------
# Load config
# ---------------------------------------------------------------------------

find_config() {
    if [[ -n "$CONFIG_PATH" ]]; then echo "$CONFIG_PATH"; return; fi
    for c in "dj_local_conf.json" "$HOME/.datajoint_config.json"; do
        [[ -f "$c" ]] && { echo "$c"; return; }
    done
    err "No DataJoint config file found. Use --config."
    exit 1
}

CONFIG_FILE="$(find_config)"
log "Config: $CONFIG_FILE"

find_mysql_cnf() {
    local candidate=""

    if [[ -n "$MYSQL_CNF_PATH" ]]; then
        candidate="$MYSQL_CNF_PATH"
    else
        for c in "${SCRIPT_DIR}/.my.cnf" "$HOME/.my.cnf"; do
            if [[ -f "$c" ]]; then
                candidate="$c"
                break
            fi
        done
    fi

    if [[ -n "$candidate" && -f "$candidate" ]]; then
        echo "$(cd "$(dirname "$candidate")" && pwd)/$(basename "$candidate")"
        return
    fi

    err "No MySQL defaults file found. Use --mysql-cnf or create .my.cnf."
    exit 1
}

MYSQL_CNF_FILE="$(find_mysql_cnf)"

HOST="$(    [[ -n "$HOST_OVERRIDE" ]] && echo "$HOST_OVERRIDE" || jq -r '."database.host"    // "127.0.0.1"' "$CONFIG_FILE")"
PORT="$(    [[ -n "$PORT_OVERRIDE" ]] && echo "$PORT_OVERRIDE" || jq -r '."database.port"    // 3306'        "$CONFIG_FILE")"

PROD_PREFIX="$(
    [[ -n "$PROD_PREFIX_OVERRIDE" ]] && echo "$PROD_PREFIX_OVERRIDE" \
    || jq -r '.custom["database.prefix"] // "u19_"' "$CONFIG_FILE"
)"
TEST_PREFIX="$(
    [[ -n "$TEST_PREFIX_OVERRIDE" ]] && echo "$TEST_PREFIX_OVERRIDE" \
    || jq -r '.custom["database.test.prefix"] // "u19_test_"' "$CONFIG_FILE"
)"

docker_mariadb_args() {
    local args=(
        docker run --rm
        -v "${MYSQL_CNF_FILE}:/root/.my.cnf:ro"
        "$DOCKER_IMAGE"
    )

    printf '%s\n' "${args[@]}"
}

run_docker_mariadb() {
    local client_command="$1"
    shift

    local docker_args=()
    while IFS= read -r arg; do
        docker_args+=("$arg")
    done < <(docker_mariadb_args)

    local client_args=(
        "$client_command"
        --protocol=TCP
        --host="$HOST"
        --port="$PORT"
    )

    "${docker_args[@]}" "${client_args[@]}" "$@"
}

sql_escape_literal() {
    echo "$1" | sed "s/'/''/g"
}

sql_query() {
    local query="$1"
    run_docker_mariadb mariadb \
        --batch \
        --raw \
        --skip-column-names \
        -e "$query"
}

build_order_clause() {
    local table_name="$1"
    local schema_order_metadata="$2"
    local matched_col
    local safe_col

    matched_col="$(printf '%s\n' "$schema_order_metadata" | awk -F $'\t' -v table_name="$table_name" '$1 == table_name { print $2; exit }')"

    if [[ -z "$matched_col" ]]; then
        echo ""
        return
    fi

    safe_col="${matched_col//\`/\`\`}"
    echo " ORDER BY \`${safe_col}\` DESC"
}

# Safety guard
if [[ -z "$TEST_PREFIX" ]]; then
    err "Test DB prefix is empty — refusing to run."
    exit 1
fi
if [[ "$TEST_PREFIX" == "$PROD_PREFIX" ]]; then
    err "Test prefix '$TEST_PREFIX' equals prod prefix — this would overwrite production data."
    exit 1
fi

log "Source host: ${HOST}:${PORT}"
log "MySQL defaults file: ${MYSQL_CNF_FILE}"
log "Docker image: ${DOCKER_IMAGE}"
log "Prefixes: prod='${PROD_PREFIX}'  →  test='${TEST_PREFIX}'"
log "Output file: $OUT_FILE"
$DRY_RUN && log "DRY-RUN mode — no commands will be executed."

# Start fresh
if [[ -f "$OUT_FILE" ]]; then
    rm -f "$OUT_FILE"
fi

# ---------------------------------------------------------------------------
# Per-schema dump
# ---------------------------------------------------------------------------

# Rewrite every backtick-quoted identifier that starts with the prod prefix.
# Uses a capture group so it covers all schemas in a single pass, including
# cross-schema FK references like:  REFERENCES `u19_subject`.`subject`
SED_EXPR="s|\`${PROD_PREFIX}\([^\`]*\)\`|\`${TEST_PREFIX}\1\`|g"

# Strip legacy DataJoint cache schema references that may appear in older dumps.
# These names can surface as `.cache` or encoded as `#mysql50#.cache` and are
# not needed for test-schema sync.
CACHE_FILTER_EXPR_1='/#mysql50#\.cache/d'
CACHE_FILTER_EXPR_2='/`\.cache`/d'
CACHE_FILTER_EXPR_3='/\b\.cache\b/d'

ERRORS=()

for SCHEMA in $SCHEMA_NAMES; do
    SCHEMA="${SCHEMA#"$PROD_PREFIX"}"
    SCHEMA="${SCHEMA#"$TEST_PREFIX"}"

    if [[ "$SCHEMA" == *"_old"* ]]; then
        log "Skipping schema '${SCHEMA}' because its name contains '_old'."
        continue
    fi

    PROD_DB="${PROD_PREFIX}${SCHEMA}"
    TEST_DB="${TEST_PREFIX}${SCHEMA}"

    log "================================================================"
    log "  Dumping  ${PROD_DB}  (will be restored as ${TEST_DB})"
    log "================================================================"

    schema_sql="docker run --rm -v ${MYSQL_CNF_FILE}:/root/.my.cnf:ro ${DOCKER_IMAGE} mariadb-dump --protocol=TCP --host=${HOST} --port=${PORT} --add-drop-table --add-drop-database --create-options --no-tablespaces --skip-comments --no-data --databases ${PROD_DB}"
    if $DRY_RUN; then
        log "  [DRY-RUN] ${schema_sql} | sed -e '${SED_EXPR}' -e '${CACHE_FILTER_EXPR_1}' -e '${CACHE_FILTER_EXPR_2}' -e '${CACHE_FILTER_EXPR_3}' >> ${OUT_FILE}"
    else
        {
            run_docker_mariadb mariadb-dump \
                --single-transaction \
                --quick \
                --add-drop-table \
                --add-drop-database \
                --create-options \
                --no-tablespaces \
                --skip-comments \
                --no-data \
                --databases "$PROD_DB"
            } | sed -e "$SED_EXPR" -e "$CACHE_FILTER_EXPR_1" -e "$CACHE_FILTER_EXPR_2" -e "$CACHE_FILTER_EXPR_3" >> "$OUT_FILE" || {
            err "  Schema-only dump for '${SCHEMA}' FAILED."
            ERRORS+=("$SCHEMA")
            continue
        }
    fi

    db_escaped="$(sql_escape_literal "$PROD_DB")"
    TABLE_LIST="$({ sql_query "
        SELECT TABLE_NAME
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA='${db_escaped}'
          AND TABLE_TYPE='BASE TABLE'
                    AND LOCATE('_old', TABLE_NAME) = 0
        ORDER BY TABLE_NAME;
    "; } || true)"

        DATE_ORDER_METADATA="$({ sql_query "
                SELECT TABLE_NAME, COLUMN_NAME
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA='${db_escaped}'
                    AND LOCATE('_old', TABLE_NAME) = 0
                    AND (
                            LOWER(COLUMN_NAME) LIKE '%date%'
                            OR LOWER(COLUMN_NAME) LIKE '%dob%'
                    )
                ORDER BY
                    TABLE_NAME,
                    CASE WHEN LOWER(COLUMN_NAME) LIKE '%dob%' THEN 0 ELSE 1 END,
                    CASE
                            WHEN LOWER(COLUMN_NAME) = 'date'
                                OR LOWER(COLUMN_NAME) LIKE '%_date'
                                OR LOWER(COLUMN_NAME) LIKE 'date_%'
                            THEN 0
                            ELSE 1
                    END,
                    ORDINAL_POSITION;
        "; } || true)"

    if [[ -z "$TABLE_LIST" ]]; then
        log "  No base tables found in ${PROD_DB}."
        continue
    fi

    SCHEMA_FAILED=false
    while IFS= read -r TABLE_NAME; do
        [[ -z "$TABLE_NAME" ]] && continue

        ORDER_CLAUSE="$(build_order_clause "$TABLE_NAME" "$DATE_ORDER_METADATA")"
        WHERE_EXPR="1${ORDER_CLAUSE} LIMIT ${LIMIT}"

        if [[ -n "$ORDER_CLAUSE" ]]; then
            log "  Table '${TABLE_NAME}': applying ORDER BY date/dob DESC before LIMIT ${LIMIT}."
        fi

        if $DRY_RUN; then
            dump_sql="docker run --rm -v ${MYSQL_CNF_FILE}:/root/.my.cnf:ro ${DOCKER_IMAGE} mariadb-dump --protocol=TCP --host=${HOST} --port=${PORT}"
            dump_sql+=" --no-create-info --skip-triggers --where=\"${WHERE_EXPR}\" ${PROD_DB} ${TABLE_NAME}"
            log "  [DRY-RUN] ${dump_sql} | sed -e '${SED_EXPR}' -e '${CACHE_FILTER_EXPR_1}' -e '${CACHE_FILTER_EXPR_2}' -e '${CACHE_FILTER_EXPR_3}' >> ${OUT_FILE}"
            continue
        fi

        {
            run_docker_mariadb mariadb-dump \
                --single-transaction \
                --quick \
                --no-tablespaces \
                --skip-comments \
                --no-create-info \
                --skip-triggers \
                --where="$WHERE_EXPR" \
                "$PROD_DB" "$TABLE_NAME"
            } | sed -e "$SED_EXPR" -e "$CACHE_FILTER_EXPR_1" -e "$CACHE_FILTER_EXPR_2" -e "$CACHE_FILTER_EXPR_3" >> "$OUT_FILE" || {
            err "  Data dump failed for table '${PROD_DB}.${TABLE_NAME}'."
            SCHEMA_FAILED=true
            break
        }
    done <<< "$TABLE_LIST"

    if $SCHEMA_FAILED; then
        ERRORS+=("$SCHEMA")
    else
        log "  Schema '${SCHEMA}' dumped."
    fi
done

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------

echo ""
if [[ ${#ERRORS[@]} -gt 0 ]]; then
    err "Dump completed with ${#ERRORS[@]} error(s): ${ERRORS[*]}"
    exit 1
fi

if ! $DRY_RUN; then
    SIZE="$(du -sh "$OUT_FILE" | cut -f1)"
    log "Dump complete → $OUT_FILE  ($SIZE)"
else
    log "Dry run complete."
fi
