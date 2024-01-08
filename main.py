import csv
import dataclasses
import io
import json
import pathlib
import re

import pandas as pd


@dataclasses.dataclass
class FilePaths:
    input_file1: str = "data.csv"
    output_file1: str = "normalized_data.csv"
    output_file2: str = "normalized_data.json"
    output_file3: str = "normalized_data.jsonl"

    def clean(self):
        for field in dataclasses.fields(self):
            if field.name.startswith("output_file"):
                file_path = getattr(self, field.name)
                pathlib.Path(str(field)).unlink(missing_ok=True)


fp = FilePaths()
fp.clean()

header = "TYPE,DATE,START TIME,END TIME,IMPORT (KWh),EXPORT (KWh),NOTES"


def assert_file_contains_expected_line(file_path, expected_line):
    regex_pattern = re.compile(rf"^{re.escape(expected_line)}$")

    with open(file_path, "r") as file:
        for line_number, line in enumerate(file, start=1):
            if regex_pattern.match(line.strip()):
                break
        else:
            raise AssertionError(
                "Expected line not found in the file "
                f"'{file_path}' at line {line_number}"
            )


try:
    assert_file_contains_expected_line(fp.input_file1, header)
except AssertionError as e:
    print(f"AssertionError: {e}")
    print(f"Expected line: {header}")
    raise e

with open(fp.input_file1, "r") as file:
    for line in file:
        if line.startswith(header):
            break

    data = file.read()

data = header + "\n" + data

csv_string_io = io.StringIO(data)

with open(fp.output_file1, "w", newline="") as outfile:
    reader = csv.reader(csv_string_io)
    writer = csv.writer(outfile)

    for row in reader:
        if len(row) == 6:
            row.append("")
        writer.writerow(row)

header_names = header.split(",")

df = pd.read_csv(
    io.StringIO(data),
    header=None,
    names=header_names,
)

df = pd.read_csv(io.StringIO(data), sep=",")
df["start_time_epoch"] = (
    pd.to_datetime(df["DATE"] + " " + df["START TIME"]).astype(int) // 10**9
)
df["end_time_epoch"] = (
    pd.to_datetime(df["DATE"] + " " + df["END TIME"]).astype(int) // 10**9
)

rolling_avg = df["IMPORT (KWh)"].rolling(window=4).mean()
df["rolling_avg"] = rolling_avg

json_data = df.to_json(orient="records")
with open(fp.output_file2, "w") as json_file:
    json_file.write(json_data)

records = json.loads(json_data)

with open(fp.output_file3, "w") as jsonl_file:
    for record in records:
        jsonl_file.write(json.dumps(record) + "\n")


print(df)
