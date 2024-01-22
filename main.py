import argparse
import csv
import io
import json
import logging
import pathlib

import jsonlines
import pandas


def setup_logger(verbosity, logfile="hiswho.log"):
    logger = logging.getLogger(__name__)

    log_levels = [logging.WARN, logging.INFO, logging.DEBUG]
    log_level = log_levels[min(verbosity, 2)]  # Cap at DEBUG level
    logger.setLevel(log_level)

    file_handler = logging.FileHandler(logfile)
    file_handler.setLevel(log_level)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)

    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    # logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger


header1 = "TYPE,DATE,START TIME,END TIME,IMPORT (KWh),EXPORT (KWh),NOTES"
header2 = "type,date,start_time,end_time,import_kwh,export_kwh,notes"
header3 = "type,start_time,end_time,import_kwh,export_kwh,notes"
header4 = (
    "type,start_time,end_time,import_kwh,export_kwh,notes,rolling_average_import_kwh"
)


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


def add_column_notes(content: io.StringIO) -> io.StringIO:
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


def assert_column_headers(content: io.StringIO, expected_header: str) -> io.StringIO:
    """if headers change from utility company, i want to know about
    it.

    """
    content.seek(0)
    csv_reader = csv.reader(content)
    actual_header = next(csv_reader)

    expected_header_list = expected_header.split(",")

    if expected_header_list != actual_header:
        raise ValueError(
            f"Header is not as expected. Expected: {expected_header_list}, Actual: {actual_header}"
        )

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


def add_columns_start_end_time(content: io.StringIO) -> io.StringIO:
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


def delete_column_date(content: io.StringIO) -> io.StringIO:
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


def add_column_rolling_average(content: io.StringIO) -> io.StringIO:
    """add a rolling average column to the data"""
    content.seek(0)
    csv_reader = csv.reader(content)

    logger.debug(f"append_rolling_average_column")

    df = pandas.DataFrame(csv_reader, columns=next(csv_reader))

    # Convert 'import_kwh' column to numeric
    df["import_kwh"] = pandas.to_numeric(df["import_kwh"], errors="coerce")

    # Calculate rolling average for 'import_kwh' with adjustable window size
    window_size = 4
    df["rolling_average_import_kwh"] = (
        df["import_kwh"].rolling(window=window_size).mean()
    )
    print(df)

    new_content = io.StringIO()
    df.to_csv(new_content, index=False)
    return new_content


def jsonl_to_json(inpath: str, outpath: str) -> None:
    try:
        with jsonlines.open(inpath, "r") as reader:
            json_data = list(reader)

        with open(outpath, "w") as writer:
            json.dump(json_data, writer, indent=2)

    except Exception as e:
        print(f"Error processing the files: {e}")
        raise


def summarize_files(_dir: str) -> None:
    sandbox_dir = pathlib.Path(_dir)

    inpath = sandbox_dir / "all.jsonl"

    assert inpath.exists(), f"{inpath} does not exist."

    outpath2 = sandbox_dir / "daily_import_usage.jsonl"
    outpath3 = sandbox_dir / "daily_import_usage.json"

    df = pandas.read_json(inpath, lines=True)
    df["start_time"] = pandas.to_datetime(df["start_time"])
    df.set_index("start_time", inplace=True)
    daily_import_usage = df["import_kwh"].resample("D").sum().asfreq("D")
    daily_import_usage = daily_import_usage.reset_index()

    daily_import_usage.to_json(outpath2, orient="records", lines=True)
    daily_import_usage.to_json(outpath3, orient="records", lines=False, indent=2)


def process_dir(data_dir: str, scratch_dir: str, no_cache: bool) -> None:
    matching_files = find_matching_files(data_dir)
    process_files(matching_files, scratch_dir, no_cache)
    summarize_files(scratch_dir)


def process_files(paths: list, scratch_dir: str, no_cache: bool) -> None:
    _dir = pathlib.Path(scratch_dir)
    _dir.mkdir(parents=True, exist_ok=True)

    outpaths = []
    for path in paths:
        inpath = pathlib.Path(path)

        outpath = _dir / f"{inpath.stem}.jsonl"

        logger.debug(f"processing {inpath}")
        logger.debug(f"outpath is {outpath.resolve()}")

        outpaths.append(outpath)

        if no_cache:
            delete_processed_file(outpath)
            process_file(inpath, outpath)
        else:
            if pathlib.Path(outpath).exists():
                logger.debug(f"skip creating {inpath} because it already exists.")
                continue
            process_file(inpath, outpath)

    lines = []
    for path in outpaths:
        with open(path, "r") as file:
            lines.extend(file.readlines())

    unique_sorted_lines = sorted(set(lines))

    aggregate_path = _dir / "all.jsonl"

    with open(aggregate_path, "w") as output_file:
        output_file.writelines(unique_sorted_lines)

    outpath = aggregate_path.with_suffix(".json")

    jsonl_to_json(str(aggregate_path), str(outpath))


def find_matching_files(_dir: str):
    pattern = "scl_electric_usage_interval_data*.csv"
    directory = pathlib.Path(_dir)
    matching_files = list(directory.rglob(pattern))
    return [str(file) for file in matching_files]


def main(args: argparse.Namespace = None):
    scratch_dir = pathlib.Path(args.scratch)
    data_dir = pathlib.Path(args.basedir)

    process_dir(data_dir, scratch_dir, args.no_cache)


def process_file(original_path: str, processed_path: str) -> io.StringIO:
    content = read_file(original_path)
    content = delete_lines_until_header(content, header1)
    content = assert_column_headers(content, header1)
    content = modify_header(content, header2)
    content = add_column_notes(content)
    content = add_columns_start_end_time(content)
    content = assert_column_headers(content, header2)
    content = add_column_rolling_average(content)
    content = delete_column_date(content)
    content = assert_column_headers(content, header4)
    content = convert_to_jsonl(content)
    content = write_file(content, processed_path)
    content = report_completion(content, original_path, processed_path)

    return content


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Transform utility data into a format "
        "that can be imported into grafana."
    )

    parser.add_argument(
        "--verbose", "-v", action="count", default=0, help="Increase verbosity level"
    )
    parser.add_argument("basedir", help="Paths to input files")
    parser.add_argument("--no-cache", action="store_true", default=False)
    parser.add_argument("--scratch-dir", default="scratch", dest="scratch")

    args = parser.parse_args()

    logger = setup_logger(args.verbose)

    main(args)
