#!/usr/bin/env bash

data_dir=/Users/mtm/pdev/taylormonacelli/eachload/data

rm -f all.jsonl

# unzip files to same dir:
find $data_dir -type f -exec sh -c '
    for file do
        dir=$(dirname "${file}")
        unzip -o "${file}" -d "${dir}"
    done
' sh {} +

# transform files to current dir:
find $data_dir -type f -name "*scl_electric_usage_interval_data*.csv" -exec sh -c '
    for file do
        python main.py --inpath "${file}"
    done
' sh {} +
cat *.jsonl | sort >all.jsonl
jq -s '.' all.jsonl >all.json
