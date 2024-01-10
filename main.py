import argparse
import csv
import io
import pathlib

import pandas

header1 = "TYPE,DATE,START TIME,END TIME,IMPORT (KWh),EXPORT (KWh),NOTES"
header2 = "type,date,start_time,end_time,import_kwh,export_kwh,notes"
header3 = "type,start_time,end_time,import_kwh,export_kwh,notes"


def report_completion(
    content: io.StringIO, original_path: str, processed_path: str
) -> None:
    path1 = pathlib.Path(original_path)
    path2 = pathlib.Path(processed_path)
    print(f"Processed {path1.name} into {path2.name}")

    return content


def delete_processed_file(file_path: str) -> None:
    pathlib.Path(file_path).unlink(missing_ok=True)


def read_file(file_path: str) -> io.StringIO:
    with open(file_path, "r") as file:
        return io.StringIO(file.read())


def write_file(content: io.StringIO, file_path: str) -> io.StringIO:
    content.seek(0)
    with open(file_path, "w") as file:
        file.write(content.getvalue())

    return content


def append_notes_column(content: io.StringIO) -> io.StringIO:
    """transform this:
    type,start_time,end_time,import_kwh,export_kwh,notes
    Electric usage,1689033600,1689034440,0.01,0.00
    Electric usage,1689033600,1689034440,0.01,0.00,some note recarding this row

    into this:
    type,start_time,end_time,import_kwh,export_kwh,notes
    Electric usage,1689033600,1689034440,0.01,0.00,
    Electric usage,1689033600,1689034440,0.01,0.00,some note recarding this row

    which is the same, but with an extra comma at the end for records
    without notes.
    """
    content.seek(0)
    csv_reader = csv.reader(content)

    new_content = io.StringIO()
    writer = csv.writer(new_content)

    for row in csv_reader:
        if len(row) == 6:
            row.append("")
        writer.writerow(row)

    return new_content


def delete_lines_until_header(content: io.StringIO, header: str) -> io.StringIO:
    """loop over header lines and throw out data that is not header
    or data.
    """
    content.seek(0)
    content_str = content.getvalue()

    lines = content_str.split("\n")
    for i, line in enumerate(lines):
        if header in line:
            new_content = "\n".join(lines[i:])
            return io.StringIO(new_content)

    # Header not found, return the original StringIO object
    return content


def assert_column_headers(content: io.StringIO, header: str) -> io.StringIO:
    content.seek(0)
    csv_reader = csv.reader(content)
    actual_header = next(csv_reader)
    assert actual_header == header.split(","), "Header does not exist."
    return content


def modify_header(content: io.StringIO, new_header: str) -> io.StringIO:
    content.seek(0)
    csv_reader = csv.reader(content)

    data = list(csv_reader)

    data[0] = new_header.split(",")

    new_content = io.StringIO()
    csv_writer = csv.writer(new_content)
    csv_writer.writerows(data)

    return new_content


def add_epoch_timestamps(content: io.StringIO) -> io.StringIO:
    content.seek(0)
    csv_reader = csv.reader(content)

    df = pandas.DataFrame(csv_reader, columns=next(csv_reader))
    df["start_time"] = (
        pandas.to_datetime(df["date"] + " " + df["start_time"]).astype(int) // 10**9
    )
    df["end_time"] = (
        pandas.to_datetime(df["date"] + " " + df["end_time"]).astype(int) // 10**9
    )

    buffer = io.StringIO()
    df.to_csv(buffer, index=False)
    return buffer


def remove_date_column(content: io.StringIO) -> io.StringIO:
    content.seek(0)
    csv_reader = csv.reader(content)

    df = pandas.DataFrame(csv_reader, columns=next(csv_reader))
    df_dropped = df.drop("date", axis=1)

    buffer = io.StringIO()
    df_dropped.to_csv(buffer, index=False)
    return buffer


def convert_to_jsonl(content: io.StringIO) -> io.StringIO:
    content.seek(0)
    csv_reader = csv.reader(content)

    df = pandas.DataFrame(csv_reader, columns=next(csv_reader))

    buffer = io.StringIO()
    buffer.write(df.to_json(orient="records", lines=True))
    buffer.seek(0)

    return buffer


def process_file(original_path: str, processed_path: str) -> io.StringIO:
    delete_processed_file(processed_path)
    content = read_file(original_path)
    content = delete_lines_until_header(content, header1)
    content = assert_column_headers(content, header1)
    content = modify_header(content, header2)
    content = append_notes_column(content)
    content = add_epoch_timestamps(content)
    content = remove_date_column(content)
    content = assert_column_headers(content, header3)
    content = convert_to_jsonl(content)
    content = write_file(content, processed_path)
    content = report_completion(content, original_path, processed_path)

    return content


def main():
    parser = argparse.ArgumentParser(
        description="Transform utility data into a format "
        "that can be imported into grafana."
    )

    parser.add_argument("--inpath", required=True, help="Input path")
    parser.add_argument("--outpath", default=None, help="Output path")

    args = parser.parse_args()

    inpath = pathlib.Path(args.inpath)
    outpath = args.outpath

    if outpath is None:
        outpath = f"{inpath.stem}-processed.jsonl"

    process_file(inpath, outpath)


if __name__ == "__main__":
    main()
