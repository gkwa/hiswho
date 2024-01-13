#!/usr/bin/env bash
set -e

data_dir="/Users/mtm/pdev/taylormonacelli/eachload/data"

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

cd ~/pdev/taylormonacelli/downcan/
make && ./downcan --data-dir $data_dir --verbose --verbose

cd $script_dir
python main.py $data_dir
python main2.py
