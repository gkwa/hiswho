import io

import pandas

with open("data.csv", "r") as file:
    for line in file:
        if line.startswith(
            "TYPE,DATE,START TIME,END TIME,IMPORT (KWh),EXPORT (KWh),NOTES"
        ):
            break

    data = file.read()

df = pandas.read_csv(
    io.StringIO(data),
    header=None,
    names=[
        "TYPE",
        "DATE",
        "START TIME",
        "END TIME",
        "IMPORT (KWh)",
        "EXPORT (KWh)",
        "NOTES",
    ],
)

df = pandas.read_csv(io.StringIO(data), sep=",")
df["datetime"] = pandas.to_datetime(df["DATE"] + " " + df["START TIME"])
df.set_index("datetime", inplace=True)
rolling_avg = df["IMPORT (KWh)"].rolling(window=4).mean()
df["rolling_avg"] = rolling_avg

print(df[["DATE", "START TIME", "IMPORT (KWh)", "rolling_avg"]])
