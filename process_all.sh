#!/usr/bin/env bash
set -e

data_dir=../eachload/data

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

cd ../downcan
make && ./downcan --data-dir $data_dir --verbose

cd $script_dir
python main.py -vv $data_dir
