# Sourced, not executed. JSON-only logging for the scripts whose output is exported.
#
# `export_logs` deletes a file only when it parsed every line, so one plain-text line from cargo or
# from a leg's own report would keep the file undeletable -- re-read and re-uploaded every night,
# forever. The reader keeps only objects carrying a timestamp and a level, so a bare
# "TIMESTAMP message" is counted unparsable and dropped, which would lose exactly the lines that
# explain a bad run.
#
# A caller sets JSON_LOG_TARGET to its own name, calls `open_the_json_log <service>` to redirect
# into the dated file the export collects, and wraps each leg in `run_wrapped <target> <command>`.
#
# One copy rather than one per script: the archiver and the researcher both export their logs, and
# two hand-maintained escapers would agree only by coincidence.

json_line() {
  local level="$1" target="$2" message="$3"
  # Backslash first, then quote: escaping the quote first would re-escape the backslash it inserts.
  # Control characters are stripped rather than encoded -- a raw byte makes the whole line invalid
  # JSON, and the exporter drops an invalid line silently.
  message="${message//\\/\\\\}"
  message="${message//\"/\\\"}"
  message="$(printf '%s' "$message" | tr -d '\000-\037')"
  printf '{"timestamp":"%s","level":"%s","target":"%s","fields":{"message":"%s"}}\n' \
    "$(date -u +"%Y-%m-%dT%H:%M:%SZ")" "$level" "$target" "$message"
}

log() {
  json_line INFO "${JSON_LOG_TARGET:?set JSON_LOG_TARGET before logging}" "$1"
}

# Redirects this script's output into `<date>.<service>.log`, which is the only name the export
# collects: `split_log_file_name` wants a date and a service, so an undated file is invisible to it.
open_the_json_log() {
  local service="$1"
  LOG_DIRECTORY="${FUND_LOG_DIRECTORY:-/var/log/fund}"
  mkdir -p "$LOG_DIRECTORY"
  STATUS_FILE="$(mktemp)"
  exec >> "${LOG_DIRECTORY}/$(date -u +%F).${service}.log" 2>&1
}

# Every line a child writes, wrapped so the dated log stays JSON-only.
run_wrapped() {
  local target="$1"; shift
  local status=0
  # The pipeline's first element decides the status; `pipefail` alone would let the wrapper's zero
  # exit mask a failed child.
  {
    "$@" 2>&1 || echo "__EXIT__$?"
  } | while IFS= read -r line; do
    case "$line" in
      __EXIT__*) status="${line#__EXIT__}"; printf '%s' "$status" > "$STATUS_FILE" ;;
      *) json_line INFO "$target" "$line" ;;
    esac
  done
  if [[ -s "$STATUS_FILE" ]]; then
    status="$(cat "$STATUS_FILE")"
    : > "$STATUS_FILE"
  fi
  return "$status"
}
