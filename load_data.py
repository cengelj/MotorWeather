#!/usr/bin/python3
# load_data.py

DEBUG = False

from typing import Sequence, Callable, Union, List, Tuple
from pathlib import Path
import sys
import os
from time import perf_counter
import psycopg2
from mmap import mmap, ACCESS_READ
from collections.abc import Sequence as Sequence_class
import re
from math import isfinite

# Type aliases
Connection = psycopg2.extensions.connection
Cursor = psycopg2.extensions.cursor
Executor = Callable[[List[str], Cursor], None]



##### Helpers for executors #####

def numeric(val: str) -> float:
    try:
        out = float(val)
        return out if isfinite(out) else None
    except Exception:
        return None

def integer(val: str) -> int:
    try:
        return int(val)
    except Exception:
        return None

def wtype(val: str) -> int:
    return 1 if len(val) != 0 else 0

# Insert values into a PostgreSQL table
def insert_str(cur: Cursor, table: str, *insertions) -> None:
    vals = ", ".join(("%s" for i in range(len(insertions))))
    cur.execute(f"INSERT INTO {table} VALUES ({vals})", insertions)

# Split the string by commas while disregarding commas surrounded by quotes.
def csv_split(line: str) -> List[str]:
    # Possibilities:
    # - Quoted string
    # - Non-empty unquoted string
    # - Empty unquoted string
    #   - Between two commas
    #   - Between beginning of line and comma
    #   - Between comma and end of line
    row = re.findall("(\"[^\"]*\"|[^,]+|(?<=,)(?=,)|^(?=,)|(?<=,)$)", line)
    # Remove surrounding quotes & unnecessary whitespace.
    for c in range(len(row)):
        row[c] = re.sub("  +", " ", row[c].strip("\"").strip())
    return row

# Ensure that the list obtained from csv_split is the correct length
def data_length_check(row: List[str], expected_length: int) -> None:
    # For some ungodly reason, whenever the last element isn't blank, the length of the list is
    # one greater than it should be, but whenever you try popping it, it actually pops two
    # items. Why it does this, I do not know, but it seems relatively harmless so.
    assert len(row) == expected_length or len(row) == expected_length + 1 \
        and len(row[expected_length]) == 0



##### Progress bar functions #####

# Display the current progress bar
def progress_bar() -> None:
    self = progress_bar

    # If we haven't initialized, don't print
    if not hasattr(self, "total") or self.loaded > self.total:
        return

    # Update variables
    self.loaded += 1
    portion = min(1., float(self.loaded)/self.total)
    new_perc = round(portion*100, self.precision)
    new_fill = round(portion*self.bar_length)
    # If the display won't have changed, don't even bother reprinting it
    if new_perc == self.perc and new_fill == self.fill and self.loaded != 1 and not DEBUG:
        return
    self.perc = new_perc
    self.fill = new_fill

    # Print the bar
    # If this is a reprint, write a carriage return to take us to the beginning of the line.
    # Since the loading bar printout only ever grows longer, there's no need to use \b.
    if self.loaded != 1:
        sys.stdout.write("\r")
    # TWO layers of formatting here:
    sys.stdout.write(f"{{:.{self.precision}f}}% [".format(self.perc))
    # Fill up the loading bar in proportion to the number of lines that have been loaded.
    for i in range(self.fill):
        sys.stdout.write("=")
    for i in range(self.bar_length - self.fill):
        sys.stdout.write(" ")
    sys.stdout.write("]")
    if DEBUG:
        sys.stdout.write(self.total_str.format(self.loaded))
    sys.stdout.flush()

# Function attributes act similarly to static variables. This resets them for progress_bar.
#
# We use functions instead of a class because there should never be more than a single progress bar
# at a time.
def init_progress_bar(total: int, length: int, precision: int = 0) -> None:
    progress_bar.bar_length = length
    progress_bar.loaded = 0
    progress_bar.total = total
    if DEBUG:
        progress_bar.total_str = f" (<DEBUG>{{}}/~{progress_bar.total})"
    # Display new_percentage next to loading bar:
    progress_bar.perc = 0
    progress_bar.precision = precision
    # Number of "full" characters in the loading bar:
    progress_bar.fill = 0



##### Executors #####

def insert_weather_line(row: List[str], cur: Cursor) -> None:
    data_length_check(row, 24)

    # Save date, since as the primary key it'll be passed to every table. This should already be in
    # ISO format.
    w_date = row[1]

    # Actual insertions:
    insert_str(cur, "Weather", row[0], w_date)
    insert_str(cur, "Wind", w_date, numeric(row[2]))
    insert_str(cur, "Precipitation", w_date, numeric(row[4]), numeric(row[5]), numeric(row[6]))
    insert_str(cur, "Temperature", w_date, integer(row[8]), integer(row[9]))
    insert_str(cur, "Wtypes", w_date, wtype(row[11]), wtype(row[12]), wtype(row[13]),
        wtype(row[14]), wtype(row[15]), wtype(row[16]), wtype(row[17]), wtype(row[18]),
        wtype(row[19]), wtype(row[20]), wtype(row[21]), wtype(row[22]), wtype(row[23]))

def insert_collision_line(row: List[str], cur: Cursor) -> None:
    data_length_check(row, 29)

    # Save ID since it'll be reused several times
    id_col = row[23]
    # Restructure date string to match ISO format
    date_match = re.fullmatch(r"(\d\d)/(\d\d)/(\d{4})", row[0])
    c_date = "-".join((date_match.group(3), date_match.group(1), date_match.group(2))) \
        if date_match else row[0]
    # Zero-pad the time if necessary
    c_time = row[1] if len(row[1]) == 5 else "".join(("0", row[1]))

    # Actual insertions:
    insert_str(cur, "Crash", id_col, c_date, c_time)
    insert_str(cur, "Location", id_col, row[2], row[3], numeric(row[4]), numeric(row[5]), row[7],
        row[8], row[9])
    insert_str(cur, "Injuries", id_col, integer(row[10]), integer(row[12]), integer(row[14]),
        integer(row[16]))
    insert_str(cur, "Deaths", id_col, integer(row[11]), integer(row[13]), integer(row[15]),
        integer(row[17]))
    insert_str(cur, "VehiclesFactors", id_col, row[24], row[25], row[26], row[27], row[28], row[18],
        row[19], row[20], row[21], row[22])



##### Helpers for read loop #####

def estimated_line_count(mm: mmap, start_position: int = 0) -> int:
    position = start_position
    count = 1
    # ":=" operator only available in Python 3.8
    while (new_position := mm.find(b"\n", position)) != -1:
        count += 1
        position = new_position + 1
    # Don't count an empty line at the end of the file
    if position == mm.size():
        count = max(0, count - 1)
    return count

# Converts seconds to formatted ?h?m?s string, removing h and m if they're 0
def duration(seconds: float) -> str:
    minutes = 0
    while seconds > 60:
        seconds -= 60
        minutes += 1
    hours = 0
    while minutes > 60:
        minutes -= 60
        hours += 1
    return f"{{}}{{}}{seconds:.3f}s".format(f"{hours}h" if hours != 0 else "",
        f"{minutes}m" if minutes != 0 else "")



##### Read loop functions #####

# Load the given file into memory & perform the given executor function upon each line of it.
def process_file(data_path: Path, open_flags: int, conn: Connection, cur: Cursor,
        executor: Executor = None, prog_config: Tuple[int, int] = None) -> int:
    # Use os.open instead of the built-in open() to avoid any unnecessary overhead in the creation
    # of a file object.
    fd = os.open(data_path, open_flags)
    line_count = 0
    data_size = data_path.stat().st_size
    # Use a memory map to reduce the number of I/O operations:
    with mmap(fd, 0, access = ACCESS_READ) as mm:
        # For schema, just read the whole file at once.
        if executor == None:
            cur.execute(mm.read())
        # Otherwise, loop through the given dataset
        else:
            # Disregard the given CSV file's header row
            mm.seek(mm.find(b"\n") + 1)
            init_progress_bar(estimated_line_count(mm, mm.tell()), *prog_config)
            # LOOP
            while len(line := mm.readline()) != 0:
                try:
                    # Strip any carriage returns due to Windows-style line endings, then convert to
                    # proper encoded text
                    row = csv_split(line.rstrip(b"\r").decode())
                    executor(row, cur)
                    line_count += 1
                    # Reprint progress bar over itself
                    progress_bar()
                except BaseException as e:
                    # We need to make a distinction between actual exceptions (class Exception) and
                    # any cause of unnatural of program termination, e.g. the user pressing CTRL+C
                    # (class BaseException).
                    if isinstance(e, Exception):
                        if isinstance(e, AssertionError):
                            # TO DO: If the row length is less than expected, save the row & try to
                            # splice it with the next one.
                            # For now, just don't insert it, and continue the loop without raising
                            # the exception.
                            pass
                        else:
                            if DEBUG:
                                print("\n<DEBUG>Row contents:", file = sys.stderr)
                                for c in range(len(row)):
                                    print
                                    print(f"  [{c}]: \"{row[c]}\"", file = sys.stderr)
                                print(f"  Length: {len(row)}", file = sys.stderr)
                            raise e
                    else:
                        sys.exit(1)
            print() # Newline to get us past the progress bar
    os.close(fd)
    conn.commit()
    return line_count

# Wrapper for processing data files
def import_routine(data: Union[Path, Sequence[Path]], open_flags: int, conn: Connection,
        cur: Cursor, executor: Executor, prog_config: Tuple[int, int]) -> int:
    args = (open_flags, conn, cur, executor, prog_config)
    if isinstance(data, Sequence_class):
        total_line_count = 0
        for d in data:
            data_name = str(d)
            print(f"+++ Parsing \"{data_name}\" +++")
            time_start = perf_counter()
            line_count = process_file(d, *args)
            time_elapsed = perf_counter() - time_start
            total_line_count += line_count
            print(f"+++ Finished parsing \"{data_name}\" +++")
            print(f"    (processed {line_count} lines in {duration(time_elapsed)})")
        return total_line_count
    else:
        data_name = str(data)
        print(f"+++ Parsing \"{data_name}\" +++")
        time_start = perf_counter()
        line_count = process_file(data, *args)
        time_elapsed = perf_counter() - time_start
        print(f"+++ Finished parsing \"{data_name}\" +++")
        return line_count



##### MAIN #####

def main() -> None:
    # Note that this is a Path object, not a string of a path
    this_dir = Path(__file__).parent

    # Working with byte sequences rather than encoded strings reduces CPU effort during memory
    # mapping. However, Windows requires the O_BINARY flag for this, whereas POSIX doesn't have it
    # at all.
    if hasattr(os, "O_BINARY"):
        open_flags = os.O_RDONLY | os.O_BINARY
        if DEBUG:
            print("<DEBUG>The O_BINARY flag IS PRESENT on this system (Windows)")
    else:
        open_flags = os.O_RDONLY
        if DEBUG:
            print("<DEBUG>The O_BINARY flag IS NOT PRESENT on this system (not Windows)")

    # Used to safely insert data into tables
    conn = psycopg2.connect("host='localhost' dbname='dbms_final_project' user='dbms_project_user' "
        "password='dbms_password'")
    cur = conn.cursor()

    ### SET UP TABLES ###

    schema_file = this_dir.joinpath("schema.sql")
    if not schema_file.exists():
        print(f"ERROR: Schema file \"{str(schema_file)}\" does not exist!", file = sys.stderr)
        sys.exit(1)
    print("### Creating schema ###")
    time_start = perf_counter()
    process_file(schema_file, open_flags, conn, cur)
    time_elapsed = perf_counter() - time_start
    print("### Finished creating schema ###")
    print(f"    (processed in {duration(time_elapsed)})")

    data_dir = this_dir.joinpath("datasets")

    ### LOAD WEATHER DATA ###

    print()
    weather_data = data_dir.joinpath("weather.csv")
    if not weather_data.exists():
        print(f"ERROR: Data file \"{str(weather_data)}\" does not exist!", file = sys.stderr)
        sys.exit(1)
    print("### Importing weather data ###")
    time_start = perf_counter()
    line_count = import_routine(weather_data, open_flags, conn, cur, insert_weather_line, (32, 0))
    time_elapsed = perf_counter() - time_start
    print("### Finished importing weather data ###")
    print(f"    (processed {line_count} lines in {duration(time_elapsed)})")

    ### LOAD COLLISION DATA ###

    print()
    collision_data = data_dir.joinpath("Motor_Vehicle_Collisions_-_Crashes.csv")
    if not collision_data.exists():
        print(f"ERROR: Data file \"{str(collision_data)}\" does not exist!", file = sys.stderr)
        sys.exit(1)
    print("### Importing collision data ###")
    time_start = perf_counter()
    line_count = import_routine(collision_data, open_flags, conn, cur, insert_collision_line,
        (48, 2))
    time_elapsed = perf_counter() - time_start
    print("### Finished importing collision data ###")
    print(f"    (processed {line_count} lines in {duration(time_elapsed)})")

if __name__ == "__main__":
    main()
