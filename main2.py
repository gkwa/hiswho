import pandas

df = pandas.read_json("all.jsonl", lines=True)
df["start_time"] = pandas.to_datetime(df["start_time"])
df.set_index("start_time", inplace=True)
daily_import_usage = df["import_kwh"].resample("D").sum().asfreq("D")
daily_import_usage = daily_import_usage.reset_index()

daily_import_usage.to_json("daily_import_usage.jsonl", orient="records", lines=True)
daily_import_usage.to_json("daily_import_usage.json", orient="records", lines=False, indent=2)
