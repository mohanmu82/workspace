#!/usr/bin/env bash
#
# copy_oracle_table.sh
#
# Copies a single Oracle table from a source DB (e.g. PROD) to a target DB
# (e.g. UAT) via Data Pump export/import through an intermediate dump file.
#
# Flow:
#   1. expdp the table on the SOURCE db to a dump file (parallel, multi-file).
#   2. Transfer the dump file(s) to the TARGET db server (scp), unless
#      SAME_FS=true (both DIRECTORY objects point at shared storage).
#   3. impdp the table on the TARGET db.
#   4. Compare row counts on both sides.
#
# Prerequisites (must already exist, this script does not create them):
#   - An Oracle DIRECTORY object on the SOURCE db (SRC_DUMP_DIR) whose
#     filesystem path is SRC_DUMP_PATH, writable by the export user.
#   - An Oracle DIRECTORY object on the TARGET db (TGT_DUMP_DIR) whose
#     filesystem path is TGT_DUMP_PATH, writable by the import user.
#   - The export/import DB users have the privileges described below.
#   - sqlplus, expdp, impdp on PATH. scp/ssh available if SAME_FS=false.
#
# Required privileges:
#   SOURCE user: SELECT on the table (or SELECT ANY TABLE), READ on SRC_DUMP_DIR.
#   TARGET user: CREATE TABLE + tablespace quota (or DROP ANY TABLE if the
#                table already exists and TABLE_EXISTS_ACTION=REPLACE),
#                READ/WRITE on TGT_DUMP_DIR. Add IMP_FULL_DATABASE if the
#                target schema differs from the connecting user (REMAP_SCHEMA).
#
# Usage:
#   ./copy_oracle_table.sh SCHEMA.TABLE_NAME [PARALLEL_DEGREE]
#
# Required environment variables:
#   SRC_DB_USER, SRC_DB_PASS, SRC_DB_TNS   - source DB connection
#   SRC_DUMP_DIR, SRC_DUMP_PATH            - source Oracle DIRECTORY name + fs path
#   TGT_DB_USER, TGT_DB_PASS, TGT_DB_TNS   - target DB connection
#   TGT_DUMP_DIR, TGT_DUMP_PATH            - target Oracle DIRECTORY name + fs path
#
# Optional environment variables:
#   SAME_FS=true|false        (default: false) skip scp if dirs are shared storage
#   TGT_HOST                  scp/ssh target host (required if SAME_FS=false)
#   TGT_SSH_USER              ssh user for TGT_HOST (default: current user)
#   SSH_KEY                   path to an ssh identity file (optional)
#   REMAP_SCHEMA              e.g. PROD_SCHEMA:UAT_SCHEMA (optional)
#   TABLE_EXISTS_ACTION       SKIP|APPEND|TRUNCATE|REPLACE (default: REPLACE)
#   CLEANUP_DUMP=true|false   (default: true) remove dump files after a
#                             successful import
#
# Example:
#   export SRC_DB_USER=app_ro SRC_DB_PASS='...' SRC_DB_TNS=PROD_TNS
#   export SRC_DUMP_DIR=PROD_DP_DIR SRC_DUMP_PATH=/u01/oradata/dpdump/prod
#   export TGT_DB_USER=app_owner TGT_DB_PASS='...' TGT_DB_TNS=UAT_TNS
#   export TGT_DUMP_DIR=UAT_DP_DIR TGT_DUMP_PATH=/u01/oradata/dpdump/uat
#   export TGT_HOST=uat-db01.internal
#   ./copy_oracle_table.sh APP.ORDERS 8

set -euo pipefail

# ---------------------------------------------------------------------------
# Args
# ---------------------------------------------------------------------------
TABLE="${1:?Usage: $0 SCHEMA.TABLE_NAME [PARALLEL_DEGREE]}"
PARALLEL="${2:-4}"
TABLE_EXISTS_ACTION="${TABLE_EXISTS_ACTION:-REPLACE}"
SAME_FS="${SAME_FS:-false}"
CLEANUP_DUMP="${CLEANUP_DUMP:-true}"

for v in SRC_DB_USER SRC_DB_PASS SRC_DB_TNS SRC_DUMP_DIR SRC_DUMP_PATH \
         TGT_DB_USER TGT_DB_PASS TGT_DB_TNS TGT_DUMP_DIR TGT_DUMP_PATH; do
  if [ -z "${!v:-}" ]; then
    echo "ERROR: required environment variable $v is not set" >&2
    exit 1
  fi
done

if [ "$SAME_FS" != "true" ] && [ -z "${TGT_HOST:-}" ]; then
  echo "ERROR: TGT_HOST must be set when SAME_FS is not true" >&2
  exit 1
fi

TS="$(date +%Y%m%d_%H%M%S)"
SAFE_TABLE="${TABLE//\./_}"
JOB_NAME="cp_${SAFE_TABLE}_${TS}"
DUMPFILE_PATTERN="${SAFE_TABLE}_${TS}_%U.dmp"
EXP_LOGFILE="${SAFE_TABLE}_${TS}_export.log"
IMP_LOGFILE="${SAFE_TABLE}_${TS}_import.log"

WORKDIR="$(mktemp -d)"
trap 'rm -rf "$WORKDIR"' EXIT

echo "== Copying table ${TABLE} from SRC (${SRC_DB_TNS}) to TGT (${TGT_DB_TNS}) =="
echo "   job name   : ${JOB_NAME}"
echo "   parallel   : ${PARALLEL}"
echo "   dump files : ${SRC_DUMP_PATH}/${DUMPFILE_PATTERN}"

# ---------------------------------------------------------------------------
# Step 1: export from SOURCE
# ---------------------------------------------------------------------------
EXP_PARFILE="${WORKDIR}/export.par"
cat > "$EXP_PARFILE" <<PAR
userid=${SRC_DB_USER}/${SRC_DB_PASS}@${SRC_DB_TNS}
directory=${SRC_DUMP_DIR}
dumpfile=${DUMPFILE_PATTERN}
logfile=${EXP_LOGFILE}
tables=${TABLE}
job_name=${JOB_NAME}_exp
parallel=${PARALLEL}
compression=all
PAR
chmod 600 "$EXP_PARFILE"

echo "-- Running expdp..."
expdp parfile="$EXP_PARFILE"

# ---------------------------------------------------------------------------
# Step 2: transfer dump file(s) to TARGET, unless shared storage
# ---------------------------------------------------------------------------
if [ "$SAME_FS" = "true" ]; then
  echo "-- SAME_FS=true, copying locally from SRC_DUMP_PATH to TGT_DUMP_PATH"
  cp "${SRC_DUMP_PATH}/${SAFE_TABLE}_${TS}_"*.dmp "${TGT_DUMP_PATH}/"
else
  SSH_OPTS=()
  [ -n "${SSH_KEY:-}" ] && SSH_OPTS+=(-i "$SSH_KEY")
  SCP_TARGET="${TGT_SSH_USER:-$USER}@${TGT_HOST}:${TGT_DUMP_PATH}/"
  echo "-- Transferring dump file(s) to ${SCP_TARGET}"
  scp "${SSH_OPTS[@]}" "${SRC_DUMP_PATH}/${SAFE_TABLE}_${TS}_"*.dmp "$SCP_TARGET"
fi

# ---------------------------------------------------------------------------
# Step 3: import into TARGET
# ---------------------------------------------------------------------------
IMP_PARFILE="${WORKDIR}/import.par"
{
  echo "userid=${TGT_DB_USER}/${TGT_DB_PASS}@${TGT_DB_TNS}"
  echo "directory=${TGT_DUMP_DIR}"
  echo "dumpfile=${DUMPFILE_PATTERN}"
  echo "logfile=${IMP_LOGFILE}"
  echo "tables=${TABLE}"
  echo "job_name=${JOB_NAME}_imp"
  echo "parallel=${PARALLEL}"
  echo "table_exists_action=${TABLE_EXISTS_ACTION}"
  [ -n "${REMAP_SCHEMA:-}" ] && echo "remap_schema=${REMAP_SCHEMA}"
} > "$IMP_PARFILE"
chmod 600 "$IMP_PARFILE"

echo "-- Running impdp..."
impdp parfile="$IMP_PARFILE"

# ---------------------------------------------------------------------------
# Step 4: verify row counts
# ---------------------------------------------------------------------------
TGT_TABLE_FOR_COUNT="$TABLE"
if [ -n "${REMAP_SCHEMA:-}" ]; then
  TGT_TABLE_FOR_COUNT="${REMAP_SCHEMA#*:}.${TABLE#*.}"
fi

SRC_COUNT="$(sqlplus -s "${SRC_DB_USER}/${SRC_DB_PASS}@${SRC_DB_TNS}" <<SQL | tr -d '[:space:]'
set heading off feedback off pagesize 0
select count(*) from ${TABLE};
exit;
SQL
)"

TGT_COUNT="$(sqlplus -s "${TGT_DB_USER}/${TGT_DB_PASS}@${TGT_DB_TNS}" <<SQL | tr -d '[:space:]'
set heading off feedback off pagesize 0
select count(*) from ${TGT_TABLE_FOR_COUNT};
exit;
SQL
)"

echo "-- Row counts: source=${SRC_COUNT} target=${TGT_COUNT}"
if [ "$SRC_COUNT" != "$TGT_COUNT" ]; then
  echo "WARNING: row counts do not match, check ${EXP_LOGFILE} / ${IMP_LOGFILE}" >&2
fi

# ---------------------------------------------------------------------------
# Cleanup dump files
# ---------------------------------------------------------------------------
if [ "$CLEANUP_DUMP" = "true" ]; then
  echo "-- Cleaning up dump files"
  rm -f "${SRC_DUMP_PATH}/${SAFE_TABLE}_${TS}_"*.dmp
  if [ "$SAME_FS" != "true" ]; then
    SSH_OPTS=()
    [ -n "${SSH_KEY:-}" ] && SSH_OPTS+=(-i "$SSH_KEY")
    ssh "${SSH_OPTS[@]}" "${TGT_SSH_USER:-$USER}@${TGT_HOST}" \
      "rm -f '${TGT_DUMP_PATH}/${SAFE_TABLE}_${TS}_'*.dmp"
  else
    rm -f "${TGT_DUMP_PATH}/${SAFE_TABLE}_${TS}_"*.dmp
  fi
fi

echo "== Done. Export log: ${SRC_DUMP_PATH}/${EXP_LOGFILE}, Import log: ${TGT_DUMP_PATH}/${IMP_LOGFILE} =="
