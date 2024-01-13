import argparse
import csv
import io
import logging
import pathlib

import pandas


def setup_logger():
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)

    file_handler = logging.FileHandler("hiswho.log")
    file_handler.setLevel(logging.DEBUG)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)

    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    # logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger


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

    which is the same as the first, but with an extra comma at the end
    for records without notes.

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

    lines = content.getvalue().split("\n")
    for i, line in enumerate(lines):
        if header in line:
            new_content = "\n".join(lines[i:])
            return io.StringIO(new_content)

    # Header not found, return the original StringIO object
    return content


def assert_column_headers(content: io.StringIO, header: str) -> io.StringIO:
    """if headers change from utility company, i want to know about
    it.

    """
    content.seek(0)
    csv_reader = csv.reader(content)
    actual_header = next(csv_reader)
    assert actual_header == header.split(","), "Header does not exist."
    return content


def modify_header(content: io.StringIO, new_header: str) -> io.StringIO:
    """convert header to something more easily managed, so instead of
    IMPORT (KWh), use import_kwh.

    """
    content.seek(0)
    csv_reader = csv.reader(content)

    data = list(csv_reader)

    data[0] = new_header.split(",")

    new_content = io.StringIO()
    csv_writer = csv.writer(new_content)
    csv_writer.writerows(data)

    return new_content


def add_epoch_timestamps(content: io.StringIO) -> io.StringIO:
    """dataframe prevents us from having to do slow loop over each
    record

    """
    content.seek(0)
    csv_reader = csv.reader(content)

    df = pandas.DataFrame(csv_reader, columns=next(csv_reader))
    df["start_time"] = pandas.to_datetime(
        df["date"] + " " + df["start_time"], format="%Y-%m-%d %H:%M"
    )
    df["end_time"] = pandas.to_datetime(
        df["date"] + " " + df["end_time"], format="%Y-%m-%d %H:%M"
    )

    column_types = df.dtypes
    print(column_types)

    new_content = io.StringIO()
    df.to_csv(new_content, index=False)
    return new_content


def remove_date_column(content: io.StringIO) -> io.StringIO:
    """we converted start_time and end_time to epoch so no need for
    date field anymore.

    """
    content.seek(0)
    csv_reader = csv.reader(content)

    df = pandas.DataFrame(csv_reader, columns=next(csv_reader))
    df_dropped = df.drop("date", axis=1)

    new_content = io.StringIO()
    df_dropped.to_csv(new_content, index=False)
    return new_content


def convert_to_jsonl(content: io.StringIO) -> io.StringIO:
    content.seek(0)
    csv_reader = csv.reader(content)

    df = pandas.DataFrame(csv_reader, columns=next(csv_reader))

    new_content = io.StringIO()
    # pandas makes converting dataframe to jsonl super easy:
    new_content.write(df.to_json(orient="records", lines=True))

    return new_content


def process_file(original_path: str, processed_path: str) -> io.StringIO:
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


def process_dir(directory_path: str, args: argparse.Namespace) -> None:
    matching_files = find_matching_files(directory_path)
    process_files(matching_files, args)


def process_files(paths: list, args: argparse.Namespace) -> None:
    outpaths = []
    for path in paths:
        inpath = pathlib.Path(path)

        outpath = f"{inpath.stem}-processed.jsonl"
        outpaths.append(outpath)

        if args.no_cache:
            delete_processed_file(outpath)
            process_file(inpath, outpath)
        else:
            if pathlib.Path(outpath).exists():
                logger.debug(f"skip creating {inpath} because it already exists.")
                continue
            process_file(inpath, outpath)

    lines = []
    for path in outpaths:
        for line in open(path, "r"):
            lines.append(line)

    unique_sorted_lines = sorted(set(lines))

    with open("all.jsonl", "w") as output_file:
        output_file.writelines(unique_sorted_lines)


def find_matching_files(directory_path):
    pattern = "scl_electric_usage_interval_data*.csv"
    directory = pathlib.Path(directory_path)
    matching_files = list(directory.rglob(pattern))
    return [str(file) for file in matching_files]


def main():
    parser = argparse.ArgumentParser(
        description="Transform utility data into a format "
        "that can be imported into grafana."
    )

    parser.add_argument("basedir", help="Paths to input files")
    parser.add_argument("--no-cache", action="store_true", default=False)

    args = parser.parse_args()
    process_dir(args.basedir, args)


if __name__ == "__main__":
    logger = setup_logger()
    main()
