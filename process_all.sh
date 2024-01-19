#!/usr/bin/env bash
set -e

data_dir="/Users/mtm/pdev/taylormonacelli/eachload/data"

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

cd ~/pdev/taylormonacelli/downcan/
make && ./downcan --data-dir $data_dir --verbose

cd $script_dir
python main.py -vv $data_dir
