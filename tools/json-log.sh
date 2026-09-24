# Sourced, not executed. JSON-only logging for the scripts whose output is exported.
#
# A caller sets JSON_LOG_TARGET, calls `open_the_json_log <service>`, and wraps each leg in
# `run_wrapped <target> <command>`. `export_logs` keeps a file whose every line did not parse, so a
# single plain-text line makes it permanent.

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
#
# The status travels through a file rather than a sentinel line, because a child printing one would
# be believed; and `read` returns false on a final line with no newline, so that line is logged by
# the second half of the condition rather than dropped.
run_wrapped() {
  local target="$1"; shift
  : > "$STATUS_FILE"
  {
    "$@" 2>&1
    printf '%s' "$?" > "$STATUS_FILE"
  } | while IFS= read -r line || [[ -n "$line" ]]; do
    json_line INFO "$target" "$line"
  done
  # The pipeline's own status is the `while` loop's, which is always zero, so the child's is read
  # back here. An empty file means the child died without reaching the line that writes it.
  local status
  status="$(cat "$STATUS_FILE")"
  [[ -n "$status" ]] || status=1
  return "$status"
}

# The status a run reports: the first leg that failed, or zero.
#
# One place rather than one per script, because both run scripts have several legs and the rule --
# a failed export is a failed run, even after a clean fold -- has to be the same in both.
first_failure() {
  local status
  for status in "$@"; do
    if [[ "$status" -ne 0 ]]; then
      printf '%s' "$status"
      return
    fi
  done
  printf '0'
}
