#!/usr/bin/env bash
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

set -e

data_dir="/Users/mtm/pdev/taylormonacelli/eachload/data"

cd ~/pdev/taylormonacelli/downcan/
make && ./downcan --data-dir $data_dir --verbose --verbose

cd $script_dir

paths=()
while IFS= read -r -d '' file; do
    paths+=("$file")
done < <(find $data_dir -type f -name "*scl_electric_usage_interval_data*.csv" -print0)

python main.py "${paths[@]}"

rm -f all.jsonl
rm -f daily_import_usage.jsonl

echo sorting all.jsonl
cat *.jsonl | sort -u >all.jsonl

jq -s '.' all.jsonl >all.json

python main2.py
