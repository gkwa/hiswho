import csv
import io
import re

import pandas

in1 = "data.csv"
out1 = "normalized_data.csv"

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
    assert_file_contains_expected_line(in1, header)
except AssertionError as e:
    print(f"AssertionError: {e}")
    print(f"Expected line: {header}")
    raise e


with open(in1, "r") as file:
    for line in file:
        if line.startswith(header):
            break

    data = file.read()

data = header + "\n" + data

csv_string_io = io.StringIO(data)


with open(out1, "w", newline="") as outfile:
    reader = csv.reader(csv_string_io)
    writer = csv.writer(outfile)

    for row in reader:
        if len(row) == 6:
            row.append("")
        writer.writerow(row)


header_names = header.split(",")

df = pandas.read_csv(
    io.StringIO(data),
    header=None,
    names=header_names,
)

df = pandas.read_csv(io.StringIO(data), sep=",")
df["datetime"] = pandas.to_datetime(df["DATE"] + " " + df["START TIME"])
df.set_index("datetime", inplace=True)
rolling_avg = df["IMPORT (KWh)"].rolling(window=4).mean()
df["rolling_avg"] = rolling_avg

print(df[["DATE", "START TIME", "IMPORT (KWh)", "rolling_avg"]])
