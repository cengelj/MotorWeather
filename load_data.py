#!/usr/bin/python3
# load_data.py

DEBUG = True



from typing import Sequence, Callable, Union, List, Tuple
from pathlib import Path
import sys
import os
import psycopg2
from mmap import mmap, ACCESS_READ
from collections.abc import Sequence as Sequence_class
import re
from datetime import date, datetime
from decimal import Decimal



# Type aliases
Connection = psycopg2.extensions.connection
Cursor = psycopg2.extensions.cursor
Executor = Callable[[str, Cursor], None]



##### Helpers for executors #####

# Helper function for input to the Decimal constructor
def numeric(val: str) -> Union[int, str]:
    return Decimal(0 if len(val) == 0 else val)

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
def data_assertion(row: List[str], expected_length: int) -> None:
    try:
        assert len(row) == expected_length# or len(row) == expected_length + 1 \
            #and row[expected_length] == ""
    except Exception as e:
        print() # Newline to get us past the progress bar
        if type(e).__name__ == "AssertionError":
            print(f"ERROR: Row length is {len(row)} instead of {expected_length}",
                file = sys.stderr)
        if DEBUG:
            print("<DEBUG>", file = sys.stderr)
            for c in range(len(row)):
                print(f"  {c + 1}: \"{row[c]}\"", file = sys.stderr)
        raise e



##### Progress bar functions #####

# Display the current progress bar
def progress_bar() -> None:
    self = progress_bar

    # If we haven't initialized, don't print
    if not hasattr(self, "total") or self.loaded > self.total:
        return

    # Update variables
    self.loaded += 1
    portion = float(self.loaded)/self.total
    new_perc = round(portion*100, self.precision)
    new_fill = round(portion*self.bar_length)
    # If the display won't have changed, don't even bother reprinting it
    if new_perc == self.perc and new_fill == self.fill and self.loaded != 1 and not DEBUG:
        return
    self.perc = new_perc
    self.fill = new_fill

    # Print the bar
    # If this is a reprint, write a carriage return to take us to the beginning of the line.
    line_start = "" if self.loaded == 1 else "\r"
    # TWO layers of formatting here:
    sys.stdout.write(f"{line_start}{{:.{self.precision}f}}% [".format(self.perc))
    # Fill up the loading bar in proportion to the number of lines that have been loaded.
    for i in range(self.fill):
        sys.stdout.write("=")
    for i in range(self.bar_length - self.fill):
        sys.stdout.write(" ")
    sys.stdout.write("]")
    if DEBUG:
        sys.stdout.write(f" (<DEBUG>{self.loaded}/{self.total})")
    sys.stdout.flush()

# Function attributes act similarly to static variables. This resets them for progress_bar.
#
# We use functions instead of a class because there should never be more than a single progress bar
# at a time.
def init_progress_bar(total: int, length: int, precision: int = 0) -> None:
    progress_bar.bar_length = length
    progress_bar.loaded = 0
    progress_bar.total = total
    # Display new_percentage next to loading bar:
    progress_bar.perc = 0
    progress_bar.precision = precision
    # Number of "full" characters in the loading bar:
    progress_bar.fill = 0



##### Executors #####

def execute_schema_statement(statement: str, cur: Cursor):
    # Condense (our copy of) the statement to remove comments & excessive whitespace
    statement = re.sub(r"(?:/\*.*?\*/|--.*?\n)", "", statement)
    statement = re.sub("(?<=;) *\n*", r"\n", statement)
    statement = re.sub("(?<!;)[\n ]*", " ", statement)
    cur.execute(statement)

def insert_weather_line(line: str, cur: Cursor) -> None:
    row = csv_split(line)
    data_assertion(row, 24)

    # Parse date first, since the resulting date object will be reused several times
    w_date = date.fromisoformat(row[1])

    # Actual insertions:
    insert_str(cur, "Weather", row[0], w_date)
    insert_str(cur, "Wind", w_date, numeric(row[2]))
    insert_str(cur, "Precipitation", w_date, numeric(row[4]), numeric(row[5]), numeric(row[6]))
    insert_str(cur, "Temperature", w_date, int(row[8]), int(row[9]))
    insert_str(cur, "Wtypes", w_date, row[11] == "1", row[12] == "1", row[13] == "1",
        row[14] == "1", row[15] == "1", row[16] == "1", row[17] == "1", row[18] == "1",
        row[19] == "1", row[20] == "1", row[21] == "1", row[22] == "1", row[23] == "1")

def insert_collision_line(line: str, cur: Cursor) -> None:
    row = csv_split(line)
    data_assertion(row, 29)

    # Save ID since it'll be reused several times
    id_col = row[23]

    # Actual insertions:
    # Zero-pad the time if necessary (otherwise the format string passed to strptime won't work)
    if len(row[1]) == 4:
        row[1] = "".join(("0", row[1]))
    insert_str(cur, "Crash", id_col, datetime.strptime(" ".join((row[0], row[1])),
        "%m/%d/%Y %H:%M"))
    insert_str(cur, "Location", id_col, row[2], row[3], numeric(row[4]), numeric(row[5]), row[7],
        row[8], row[9])
    insert_str(cur, "Injuries", id_col, int(row[10]), int(row[12]), int(row[14]), int(row[16]))
    insert_str(cur, "Deaths", id_col, int(row[11]), int(row[13]), int(row[15]), int(row[17]))
    insert_str(cur, "VehiclesFactors", id_col, row[24], row[25], row[26], row[27], row[28], row[18],
        row[19], row[20], row[21], row[22])



##### Helpers for read loop #####

def substring_count(mm: mmap, sub: bytes) -> int:
    position = 0
    count = 0
    # ":=" operator only available in Python 3.8
    while (position := mm.find(sub, position)) != -1:
        position += 1
        count += 1
    return count

# Count number of newlines in a memory mapped file
def line_count(mm: mmap) -> int:
    count = substring_count(mm, b"\n") + 1
    # Don't count an empty line at the end of the file
    if mm.rfind(b"\n") == mm.size() - 1:
        count -= 1
    return count



##### Read loop functions #####

# Load the given file into memory & perform the given executor function upon each line of it.
def process_data(data_path: Path, open_flags: int, max_map: int, executor: Executor,
        conn: Connection, cur: Cursor, prog_config: Tuple[int, int]) -> None:
    # Use os.open instead of the built-in open() to avoid any unnecessary overhead in the creation
    # of a file object.
    fd = os.open(data_path, open_flags)
    data_size = data_path.stat().st_size
    # Use a memory map to reduce the number of I/O operations:
    with mmap(fd, 0 if data_size < max_map else max_map, access = ACCESS_READ) as mm:
        if executor == execute_schema_statement:
            init_progress_bar(substring_count(mm, b";"), *prog_config)
            # LOOP (schema)
            old_position = 0
            # For each iteration of the loop, read all until the next semicolon
            while len(statement := mm.read(mm.find(b";", (position := mm.tell()))
                    - old_position)) != 0:
                executor(statement.decode(), cur)
                # Reprint progress bar over itself
                progress_bar()
                old_position = position + 1
        else:
            init_progress_bar(line_count(mm), *prog_config)
            # Disregard the header row of the given CSV files
            mm.seek(mm.find(b"\n") + 1)
            # LOOP (data)
            while len(line := mm.readline()) != 0:
                # Strip any carriage returns due to Windows-style line endings, then convert to
                # proper encoded text
                executor(line.rstrip(b"\r").decode(), cur)
                progress_bar()
        print() # Newline to get us past the progress bar
    os.close(fd)

    conn.commit()

# Wrapper for processing data files
def import_routine(name: str, data: Union[Path, Sequence[Path]], open_flags: int, max_map: int,
        executor: Executor, conn: Connection, cur: Cursor, prog_config: Tuple[int, int]) \
        -> None:
    print(f"+++ Importing {name} data +++")
    args = (open_flags, max_map, executor, conn, cur, prog_config)
    if isinstance(data, Sequence_class):
        for d in data:
            print(f"Parsing \"{str(d)}\"")
            process_data(d, *args)
    else:
        print(f"Parsing \"{str(data)}\"")
        process_data(data, *args)
    print(f"### Finished importing {name} data ###")



##### MAIN #####

def main() -> None:
    # Note that this is a Path object, not a string of a path
    this_dir = Path(__file__).parent

    # Working with byte sequences rather than encoded strings reduces CPU effort during memory
    # mapping. However, Windows requires the O_BINARY flag for this, whereas POSIX doesn't have it
    # at all.
    open_flags = os.O_RDONLY | os.O_BINARY if hasattr(os, "O_BINARY") else os.O_RDONLY
    # Maximum size of memory map for reading data, in bytes:
    max_map = 1024*1024*128 # 128 MB

    # Used to safely insert data into tables
    conn = psycopg2.connect("host='localhost' dbname='dbms_final_project' user='dbms_project_user' "
        "password='dbms_password'")
    cur = conn.cursor()

    ### SET UP TABLES ###

    schema_file = this_dir.joinpath("schema.sql")
    if not schema_file.exists():
        print(f"ERROR: Schema file \"{str(schema_file)}\" does not exist!", file = sys.stderr)
        sys.exit(1)
    print(f"+++ Creating schema +++")
    process_data(schema_file, open_flags, max_map, execute_schema_statement, conn, cur, (24, 0))
    print(f"### Finished creating schema ###")

    data_dir = this_dir.joinpath("datasets")

    ### LOAD WEATHER DATA ###

    weather_data = data_dir.joinpath("weather.csv")
    if not weather_data.exists():
        print(f"ERROR: Data file \"{str(weather_data)}\" does not exist!", file = sys.stderr)
        sys.exit(1)
    import_routine("weather", weather_data, open_flags, max_map, insert_weather_line, conn, cur,
        (32, 0))

    ### LOAD COLLISION DATA ###

    collision_data = data_dir.joinpath("Motor_Vehicle_Collisions_-_Crashes.csv")
    if not weather_data.exists():
        print(f"ERROR: Data file \"{str(collision_data)}\" does not exist!", file = sys.stderr)
        sys.exit(1)
    import_routine("collision", collision_data, open_flags, max_map, insert_collision_line, conn,
        cur, (48, 2))

if __name__ == "__main__":
    main()
