#!/usr/bin/env bash
# Reproducible thread-pool profile baseline + before/after comparison.
#
# Usage:
#   tools/profile_pool.sh baseline      # run the matrix, write docs/perf/baseline-<date>.md
#   tools/profile_pool.sh compare       # re-run after a change, write docs/perf/compare-<date>.md
#
# Matrix: workers {1,2,4,8} x tasks {1k,10k,100k}, taskset-pinned to 8
# logical CPUs, 3 timed rounds (median).  perf stat captures
# cycles/instructions/cache-references/cache-misses/context-switches/
# cpu-migrations/branch-misses per round; perf record -g (baseline mode
# only) writes per-cell worker_entry attribution data to /tmp/lw-prof/
# for later `perf annotate`.
#
# Raw perf output stays in /tmp; the T1-T6 summary is committed under
# docs/perf/.  The perf gate in tools/bench_compare.py is untouched.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BENCH="$ROOT/build/examples/bench"

WORKERS=(1 2 4 8)
TASKS=(1000 10000 100000)
ROUNDS=3
PIN_CPUS="0-7"

# perf -x, field order: <count>,<stddev>,<name>,<period>,<enabled%>,<run%>
stat_field() { # $1=event name  $2=perf.err file
    awk -F, -v ev="$1" '
        $3 == ev || $3 == ev ":u" { gsub(",", "", $1); print $1; found=1; exit }
        END { if (!found) print "NA" }' "$2" 2>/dev/null
}

run_cell() {
    # $1=workers  $2=tasks  $3=round (0=warm-up, 1..N timed)
    local w="$1" t="$2" r="$3"
    local out="/tmp/lw-prof/w${w}_t${t}_r${r}"
    mkdir -p "$out"

    taskset -c "$PIN_CPUS" \
        perf stat -x, \
        -e cycles:u,instructions:u,cache-references:u,cache-misses:u,context-switches,cpu-migrations,branch-misses:u \
        -- \
        "$BENCH" --json --iterations 20 --tasks "$t" --workers "$w" \
        > "$out/bench.out" 2> "$out/perf.err" || echo "  bench_failed w=${w} t=${t} r=${r}" >&2
}

run_matrix() {
    local mode="$1"
    local stamp report
    stamp="$(date +%Y-%m-%d)"
    report="$ROOT/docs/perf/${mode}-${stamp}.md"
    mkdir -p "$ROOT/docs/perf"

    {
        echo "# ${mode} report ${stamp}"
        echo "# host: $(uname -m) $(uname -r)"
        echo "# pinning: taskset -c ${PIN_CPUS}; ${ROUNDS} timed rounds (median)"
        echo
    } > "$report"

    for w in "${WORKERS[@]}"; do
        for t in "${TASKS[@]}"; do
            run_cell "$w" "$t" 0   # warm-up (untimed)

            local wall_csv=""
            for ((r = 1; r <= ROUNDS; r++)); do
                local start end wall
                start="$(date +%s.%N)"
                run_cell "$w" "$t" "$r"
                end="$(date +%s.%N)"
                wall="$(awk -v a="$start" -v b="$end" 'BEGIN { printf "%.3f", b - a }')"
                wall_csv+="${wall},"
            done

            local mid=$(( (ROUNDS + 1) / 2 ))
            local bench_out="/tmp/lw-prof/w${w}_t${t}_r${mid}/bench.out"

            {
                echo "w=${w} t=${t}:"
                echo "  wall_s (median of ${ROUNDS}): $(printf '%s\n' "${wall_csv%,}" | tr ',' '\n' | sort -n | sed -n "${mid}p")"
                echo "  submit_latency_avg_ns: $(awk -F': ' '/"submit_latency_avg_ns"/ { print $2; exit }' "$bench_out")"
                echo "  throughput_tps: $(grep -o '"throughput_tps": [0-9.e+-]*' "$bench_out" | grep -o '[0-9.e+-]*')"
                echo "  tail_latency_ns: $(grep -o '"tail_latency_ns": {[^}]*}' "$bench_out")"
                echo "  fairness_resp_ns: $(grep -o '"fairness_resp_ns": {[^}]*}' "$bench_out")"
                echo "  dag_throughput_tps: $(grep -o '"dag_throughput_tps": [0-9.e+-]*' "$bench_out" | grep -o '[0-9.e+-]*')"
                echo "  dag_latency_ns: $(grep -o '"dag_latency_ns": {[^}]*}' "$bench_out")"
                echo
                for r in $(seq 1 "$ROUNDS"); do
                    local f="/tmp/lw-prof/w${w}_t${t}_r${r}/perf.err"
                    local cyc ins cref cmis csw mig brm
                    cyc="$(stat_field cycles "$f")"
                    ins="$(stat_field instructions "$f")"
                    cref="$(stat_field cache-references "$f")"
                    cmis="$(stat_field cache-misses "$f")"
                    csw="$(stat_field context-switches "$f")"
                    mig="$(stat_field cpu-migrations "$f")"
                    brm="$(stat_field branch-misses "$f")"
                    echo "  round ${r}:"
                    echo "    cycles=${cyc}  instructions=${ins}  cache-references=${cref}  cache-misses=${cmis}"
                    echo "    context-switches=${csw}  cpu-migrations=${mig}  branch-misses=${brm}"
                done
                echo
            } >> "$report"

            if [[ "$mode" == "baseline" ]]; then
                local pbin="/tmp/lw-prof/w${w}_t${t}_r${mid}/worker.data"
                taskset -c "$PIN_CPUS" \
                    perf record -g -o "$pbin" -- \
                    "$BENCH" --json --iterations 20 --tasks "$t" --workers "$w" \
                    > /dev/null 2> "/tmp/lw-prof/w${w}_t${t}_r${mid}/record.err" \
                    || echo "  perf record failed w=${w} t=${t}" >&2
            fi
        done
    done

    echo "Report: $report"
    echo "Raw perf data: /tmp/lw-prof/"
    if [[ "$mode" == "baseline" ]]; then
        echo "Next: perf report -i /tmp/lw-prof/wW_tT_r2/worker.data --symbols worker_entry for hotspot attribution."
    else
        echo "Manual keep/revert decision: diff this report's cells against docs/perf/baseline-*.md using the section-3 acceptance rules."
    fi
}

case "${1:-}" in
    baseline)
        [[ -f "$BENCH" ]] || { echo "build/examples/bench not found; run: cmake -S . -B build && cmake --build build --target bench" >&2; exit 1; }
        command -v perf >/dev/null || { echo "perf not found" >&2; exit 1; }
        command -v taskset >/dev/null || { echo "taskset not found" >&2; exit 1; }
        run_matrix baseline
        ;;
    compare)
        [[ -f "$BENCH" ]] || { echo "build/examples/bench not found" >&2; exit 1; }
        run_matrix compare
        ;;
    usage|"")
        sed -n '2,20p' "$0"
        ;;
    *)
        echo "unknown mode: $1 (try: baseline | compare)" >&2
        exit 2
        ;;
esac
