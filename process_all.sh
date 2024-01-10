#!/usr/bin/evn bash

rm -f all.jsonl

find /Users/mtm/pdev/taylormonacelli/eachload/data -type f -name "*scl_electric_usage_interval_data*.csv" -exec sh -c '
    for file do
        python main.py --inpath "${file}"
    done
' sh {} +
cat *.jsonl | sort >all.jsonl
jq -s '.' all.jsonl >all.json
