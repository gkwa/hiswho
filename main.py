import csv
import dataclasses
import io
import json
import pathlib
import re
import os

import pandas as pd


@dataclasses.dataclass
class FilePaths:
    input_file1: str = "data.csv"
    output_file1: str = "normalized_data1.csv"
    output_file2: str = "normalized_data2.json"
    output_file3: str = "normalized_data3.jsonl"
    output_file4: str = "normalized_data4.txt"
    output_file5: str = "normalized_data5.txt"
    output_file7: str = "normalized_data7.txt"
    output_file8: str = "normalized_data8.txt"
    output_file9: str = "normalized_data9.txt"
    output_file10: str = "normalized_data10.txt"
    output_file11: str = "normalized_data11.json"
    output_file12: str = "normalized_data12.jsonl"
    output_file13: str = "normalized_data13.json"

    def clean(self):
        for field in dataclasses.fields(self):
            if field.name.startswith("output_file"):
                file_path = getattr(self, field.name)
                pathlib.Path(str(field)).unlink(missing_ok=True)


fp = FilePaths()
fp.clean()

header1 = "TYPE,DATE,START TIME,END TIME,IMPORT (KWh),EXPORT (KWh),NOTES"
header2 = "type,date,start_time,end_time,import_kwh,export_kwh,notes"


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
    assert_file_contains_expected_line(fp.input_file1, header1)
except AssertionError as e:
    print(f"AssertionError: {e}")
    print(f"Expected line: {header1}")
    raise e


with open(fp.input_file1, "r") as infile:
    data = infile.read()


converted_data = data.replace(header1, header2)

with open(fp.output_file5, "w") as outfile:
    outfile.write(converted_data)

pathlib.Path(fp.output_file8).write_text(converted_data)

with open(fp.output_file8, "r") as file:
    for line in file:
        if line.startswith(header2):
            break

    data5 = file.read()


data5 = header2 + "\n" + data5
pathlib.Path(fp.output_file7).write_text(data5)


csv_string_io = io.StringIO(data5)

with open(fp.output_file10, "w", newline="") as outfile:
    reader = csv.reader(csv_string_io)
    writer = csv.writer(outfile)

    for row in reader:
        if len(row) == 6:
            row.append("")
        writer.writerow(row)

header_names = header2.split(",")

df = pd.read_csv(
    fp.output_file10,
    names=header_names,
)

df = pd.read_csv(fp.output_file10, sep=",")
df["start_time_epoch"] = (
    pd.to_datetime(df["date"] + " " + df["start_time"]).astype(int) // 10**9
)
df["end_time_epoch"] = (
    pd.to_datetime(df["date"] + " " + df["end_time"]).astype(int) // 10**9
)

rolling_avg = df["import_kwh"].rolling(window=4).mean()
df["rolling_avg"] = rolling_avg

json_data = df.to_json(orient="records")
with open(fp.output_file11, "w") as json_file:
    json_file.write(json_data)

records = json.loads(json_data)

y = json.dumps(records, indent=2)
with open(fp.output_file13, "w") as json_file:
    json_file.write(y)

with open(fp.output_file12, "w") as jsonl_file:
    for record in records:
        jsonl_file.write(json.dumps(record) + "\n")


print(df)
