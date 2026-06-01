#!/usr/bin/env bash
set -euo pipefail

tmp_csv="$(mktemp)"
trap 'rm -f "$tmp_csv"' EXIT

echo "worker,db_conns,throughput,file" >"$tmp_csv"
matrix_report="bench/results/scenario-4-matrix.md"
if [[ -f "$matrix_report" ]]; then
  awk -F'|' -v f="$matrix_report" '
    {
      for (i = 1; i <= NF; i++) {
        gsub(/^[[:space:]]+|[[:space:]]+$/, "", $i)
      }
    }
    $2 ~ /^[0-9]+$/ && $3 ~ /^[0-9]+$/ {
      # workers, db_conns, throughput, source file
      print $2 "," $3 "," $8 "," f
    }
  ' "$matrix_report" >>"$tmp_csv"
else
  shopt -s nullglob
  files=(bench/results/scenario-4-w*-c*.md)
  if [[ ${#files[@]} -eq 0 ]]; then
    echo "no scenario-4 matrix files found: bench/results/scenario-4-matrix.md or bench/results/scenario-4-w*-c*.md"
    exit 1
  fi

  for file in "${files[@]}"; do
    base="$(basename "$file")"
    if [[ ! "$base" =~ ^scenario-4-w([0-9]+)-c([0-9]+)\.md$ ]]; then
      continue
    fi
    worker="${BASH_REMATCH[1]}"
    conns="${BASH_REMATCH[2]}"

    throughput="$(
      awk -F'|' -v w="$worker" '
        {
          for (i = 1; i <= NF; i++) {
            gsub(/^[[:space:]]+|[[:space:]]+$/, "", $i)
          }
        }
        $2 ~ /^[0-9]+$/ && $2 == w {
          print $6
          exit
        }
      ' "$file"
    )"

    if [[ -z "${throughput:-}" ]]; then
      echo "warn: could not find worker row ${worker} in ${file}" >&2
      continue
    fi
    echo "${worker},${conns},${throughput},${file}" >>"$tmp_csv"
  done
fi

if [[ "$(wc -l <"$tmp_csv")" -le 1 ]]; then
  echo "no parseable matrix rows found"
  exit 1
fi

echo "Scenario 4 matrix summary"
echo
echo "Top 10 cells by throughput:"
{
  head -n 1 "$tmp_csv"
  tail -n +2 "$tmp_csv" | sort -t, -k3,3nr | head -n 10
} | column -s, -t
echo

best_line="$(tail -n +2 "$tmp_csv" | sort -t, -k3,3nr | head -n 1)"
best_worker="$(echo "$best_line" | cut -d, -f1)"
best_conns="$(echo "$best_line" | cut -d, -f2)"
best_tp="$(echo "$best_line" | cut -d, -f3)"
best_file="$(echo "$best_line" | cut -d, -f4-)"

echo "Global best:"
echo "  workers=${best_worker} db_conns=${best_conns} throughput=${best_tp} file=${best_file}"
echo

# Best per worker, then knee: smallest worker within 95% of max best-per-worker throughput.
tmp_best_per_worker="$(mktemp)"
trap 'rm -f "$tmp_csv" "$tmp_best_per_worker"' EXIT

awk -F',' '
  NR == 1 { next }
  {
    w = $1
    t = $3 + 0
    if (!(w in best) || t > best[w]) {
      best[w] = t
      row[w] = $0
    }
  }
  END {
    for (w in row) print row[w]
  }
' "$tmp_csv" | sort -t, -k1,1n >"$tmp_best_per_worker"

max_best_tp="$(awk -F',' 'BEGIN{m=0} { if (($3+0) > m) m = $3+0 } END { print m }' "$tmp_best_per_worker")"
knee_threshold="$(awk -v m="$max_best_tp" 'BEGIN { printf "%.4f", m * 0.95 }')"
knee_line="$(awk -F',' -v th="$knee_threshold" '($3+0) >= th { print; exit }' "$tmp_best_per_worker")"

echo "Best cell per worker:"
{
  echo "worker,db_conns,throughput,file"
  cat "$tmp_best_per_worker"
} | column -s, -t
echo

if [[ -n "${knee_line:-}" ]]; then
  knee_worker="$(echo "$knee_line" | cut -d, -f1)"
  knee_conns="$(echo "$knee_line" | cut -d, -f2)"
  knee_tp="$(echo "$knee_line" | cut -d, -f3)"
  echo "Recommended knee (smallest worker within 95% of best-per-worker max):"
  echo "  workers=${knee_worker} db_conns=${knee_conns} throughput=${knee_tp} threshold=${knee_threshold}"
else
  echo "Could not compute knee from parsed data."
fi
