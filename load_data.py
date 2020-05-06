#!/usr/bin/python3
# load_data.py

import psycopg2
from pathlib import Path
import os
from mmap import mmap, ACCESS_READ
from collections.abc import Sequence
from typing import Callable
from math import floor
from datetime import date
from decimal import Decimal



# Display the current progress bar
def progress_bar() -> None:
    self = progress_bar

    # If we haven't initialized, don't print
    if not hasattr(self, "total") or self.loaded > self.total:
        return

    # Update variables
    self.loaded += 1
    portion = float(self.loaded)/self.total
    new_perc = floor(portion*100)
    new_fill = floor(portion*self.bar_length)
    # If the display won't have changed, don't even bother reprinting it
    if new_perc == self.perc and new_fill == self.fill and self.loaded != 1:
        return
    self.perc = new_perc
    self.fill = new_fill

    # Print the bar
    print(f"{self.perc}% [", end = "", flush = False)
    # Fill up the loading bar in proportion to the number of lines that have been loaded.
    for i in range(self.fill):
        print("=", end = "", flush = False)
    for i in range(self.bar_length - self.fill):
        print(" ", end = "", flush = False)
    # If the loading bar will need to be reprinted in the future, print a carriage return without
    # a new line.
    print("]", end = "\n" if self.loaded == self.total else "\r", flush = True)

# Function attributes act similarly to static variables. This resets them for progress_bar.
#
# We use functions instead of a class because there should never be more than a single progress bar
# at a time.
def init_progress_bar(total: int, length: int) -> None:
    func = progress_bar

    func.bar_length = length
    func.loaded = 0
    func.total = total
    # Display new_percentage next to loading bar:
    func.new_perc = 0
    # Number of "full" characters in the loading bar:
    func.new_fill = 0



def insert_str(table: str, *insertions) -> None:
    self = insert_str

    vals_list = ["%s"]
    vals_list.extend(", %s" for i in range(1, len(insertions)))
    vals = "".join(vals_list)

    self.cur.execute(f"INSERT INTO {table} VALUES ({vals})", insertions)

def insert_weather_line(line: str) -> None:
    # Split by commas
    row = line.split(",")
    assert len(row) == 24
    # Remove surrounding quotes & whitespace
    for col in row:
        col = col.strip("\"").strip()
    # Parse date first, since the resulting date object will be reused several times
    w_date = date(*(row[1].split("-")))

    # Actual insertions:
    insert_str("Weather", row[0], w_date)
    insert_str("Wind", w_date, Decimal(row[2]))
    insert_str("Precipitation", w_date, Decimal(row[4]), Decimal(row[5]), Decimal(row[6]))
    insert_str("Temperature", w_date, int(row[8]), int(row[9]))
    insert_str("Wtype", w_date, rows[11] == "1", rows[12] == "1", rows[13] == "1", rows[14] == "1",
        rows[15] == "1", rows[16] == "1", rows[17] == "1", rows[18] == "1", rows[19] == "1",
        rows[20] == "1", rows[21] == "1", rows[22] == "1", rows[23] == "1")

def insert_collision_line(line: str) -> None:
    # Split by commas
    row = line.split(",")
    assert len(row) == 29
    # Remove surrounding quotes & whitespace
    for col in row:
        col = col.strip("\"").strip()
    # Save ID since it'll be reused several times
    id_col = row[23]

    # Actual insertions:
    insert_str("Crash", id_col, date(*(row[0].split("-"))), row[1])
    insert_str("Location", id_col, row[2], row[3], Decimal(row[4]), Decimal(row[5]), row[7], row[8],
        row[9])
    insert_str("Injuries", id_col, int(row[10]), int(row[12]), int(row[14]), int(row[16]))
    insert_str("Deaths", id_col, int(row[11]), int(row[13]), int(row[15]), int(row[17]))
    insert_str("VehiclesFactors", id_col, row[24], row[25], row[26], row[27], row[28], row[18],
        row[19], row[20], row[21], row[22])

def linecount(mm: mmap) -> int:
    count = 1
    pos = 0
    # ":=" operator only available in Python 3.8
    while pos := mm.find(b"\n", pos):
        if pos == -1:
            return count
        count += 1
        pos += 1

def process_data(data: str, inserter: Callable[[str], None]) -> None:
    self = process_data

    print(f"Loading {data}:")
    fd = os.open(data, self.open_flags)
    # Use a memory map to reduce the number of I/O operations:
    with mmap(fd, self.max_map, access = ACCESS_READ) as mm:
        init_progress_bar(linecount(mm), 32)
        # Disregard the header row
        mm.seek(mm.find(b"\n") + 1)
        # Loop through the CSV, adding each tuple to the database
        while len(line := mm.readline()) != 0:
            # Remove trailing carriage return, in case it's there from a file with Windows-style
            # newlines, then convert the line to a proper text string.
            inserter(line.rstrip(b"\r").decode())
            progress_bar()
    os.close(fd)

# Load the given data into the appropriate database
def import_routine(name: str) -> None:
    self = import_routine

    print(f"/// Importing {name} data \\\\\\")

    this_info = self.info[name]
    data, inserter = this_info["data"], this_info["inserter"]
    if isinstance(data, Sequence):
        for d in data:
            process_data(str(d), inserter)
    else:
        process_data(str(data), inserter)

    self.conn.commit()
    print(f"\\\\\\ Successfully imported {name} data ///")



def main() -> None:
    #.Pass effectively static variables to some helper functions. I could # have passed them as
    # normal function arguments instead, but they never change between calls, so what's the point?

    # Used to safely insert data into tables
    import_routine.conn = psycopg2.connect("host='localhost' dbname='dbms_final_project' " \
        "user='dbms_project_user' password='dbms_password'")
    insert_str.cur = import_routine.conn.cursor()

    # Note that this is a Path object, not a path string
    directory = Path(__file__).parent.joinpath("datasets")
    # Comparing against dictionary keys is much quicker than chained if/elif statements
    import_routine.info = {
        "weather": {
            "data": directory.joinpath("weather.csv"),
            "inserter": insert_weather_line
        },
        "collision": {
            #"data": directory.glob("collisions*.csv"),
            "data": directory.joinpath("Motor_Vehicle_Collisions_-_Crashes.csv"),
            "inserter": insert_collision_line
        }
    }

    # Working with byte sequences rather than encoded strings reduces CPU effort during memory
    # mapping. However, Windows requires the O_BINARY flag for this, whereas POSIX doesn't have it
    # at all.
    process_data.open_flags = os.O_RDONLY | os.O_BINARY if hasattr(os, "O_BINARY") \
        else os.O_RDONLY
    # Maximum size of memory map for reading data, in bytes:
    process_data.max_map = 1024*1024*128 # 128 MB

    ### LOAD WEATHER DATA ###
    import_routine("weather")
    ### LOAD COLLISION DATA ###
    import_routine("collision")

if __name__ == "__main__":
    main()
