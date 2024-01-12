#!/usr/bin/env bash
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

rm -f all.jsonl

data_dir="/Users/mtm/pdev/taylormonacelli/eachload/data"

cd ~/pdev/taylormonacelli/downcan/
make && ./downcan --data-dir --verbose --verbose $data_dir

cd $script_dir

find $data_dir -type f -name "*scl_electric_usage_interval_data*.csv" -exec sh -c '
    for file do
        python main.py --inpath "${file}"
    done
' sh {} +

python main2.py

cat *.jsonl | sort >all.jsonl

jq -s '.' all.jsonl >all.json

