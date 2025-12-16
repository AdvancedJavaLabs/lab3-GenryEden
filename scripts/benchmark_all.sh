#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

FILE_GROUPS="${FILE_GROUPS:-0.csv=0.csv;1.csv=1.csv;2.csv=2.csv;3.csv=3.csv;4.csv=4.csv;5.csv=5.csv;6.csv=6.csv;7.csv=7.csv;all=0.csv,1.csv,2.csv,3.csv,4.csv,5.csv,6.csv,7.csv}"

INPUT_BASE="${INPUT_BASE:-/data/input-groups}"
OUTPUT_BASE="${OUTPUT_BASE:-/data/output-bench-groups}"
JAR_PATH="${JAR_PATH:-/tmp/app.jar}"
MAP_THREADS="${MAP_THREADS:-1 2 4 8}"
REDUCERS="${REDUCERS:-1 2 4 8}"
RESULTS_FILE="${RESULTS_FILE:-benchmarks/results_all.csv}"
OUTPUT_LOCAL_DIR="${OUTPUT_LOCAL_DIR:-benchmarks/outputs}"

mkdir -p "${ROOT_DIR}/benchmarks"
mkdir -p "${ROOT_DIR}/${OUTPUT_LOCAL_DIR}"
echo "dataset,map_threads,reducers,duration_ms" > "${ROOT_DIR}/${RESULTS_FILE}"

IFS=';' read -r -a GROUP_ENTRIES <<< "${FILE_GROUPS}"

for entry in "${GROUP_ENTRIES[@]}"; do
  label="${entry%%=*}"
  files_raw="${entry#*=}"
  IFS=',' read -r -a files <<< "${files_raw}"

  INPUT_PATH="${INPUT_BASE}/${label}"
  echo "Uploading group '${label}' -> ${INPUT_PATH}"
  docker exec yarn hdfs dfs -rm -r -f "${INPUT_PATH}" >/dev/null 2>&1 || true
  docker exec yarn hdfs dfs -mkdir -p "${INPUT_PATH}"

  for file in "${files[@]}"; do
    host_path="${ROOT_DIR}/${file}"
    if [[ ! -f "${host_path}" ]]; then
      echo "WARN: ${host_path} not found, skipping."
      continue
    fi
    echo "  -> ${file}"
    docker cp "${host_path}" yarn:/tmp/"${file}"
    docker exec yarn hdfs dfs -put -f /tmp/"${file}" "${INPUT_PATH}"/
  done

  for m in ${MAP_THREADS}; do
    for r in ${REDUCERS}; do
      OUT_PATH="${OUTPUT_BASE}-${label}-m${m}-r${r}"
      echo "Running dataset=${label} m=${m} r=${r} -> ${OUT_PATH}"
      docker exec yarn hdfs dfs -rm -r -f "${OUT_PATH}" >/dev/null 2>&1 || true
      start_ms=$(( $(date +%s%N) / 1000000 ))
      docker exec yarn hadoop jar "${JAR_PATH}" \
        --input "${INPUT_PATH}" \
        --output "${OUT_PATH}" \
        --map-threads "${m}" \
        --reducers "${r}" >/dev/null
      end_ms=$(( $(date +%s%N) / 1000000 ))
      duration_ms=$((end_ms - start_ms))
      echo "${label},${m},${r},${duration_ms}" | tee -a "${ROOT_DIR}/${RESULTS_FILE}"

      if [[ "${r}" -eq 1 ]]; then
        docker exec yarn hdfs dfs -cat "${OUT_PATH}/part-r-00000" > "${ROOT_DIR}/${OUTPUT_LOCAL_DIR}/${label}_m${m}_r${r}.txt"
      fi
    done
  done
done



