# load_data_async.py

DEBUG = False

from sys import platform, stdout, stderr, exit
WINDOWS = platform.startswith("win")
from typing import Any, Sequence, Callable, Optional, List, Tuple
from pathlib import Path
import os
from time import perf_counter
from mmap import mmap
if WINDOWS:
    from mmap import ACCESS_READ
    from msvcrt import get_osfhandle
else:
    from mmap import MAP_SHARED, PROT_READ
from multiprocessing import Process, Lock as LockFactory
from multiprocessing.sharedctypes import RawValue
from multiprocessing.synchronize import Lock
from multiprocessing.connection import wait
import psycopg2
from psycopg2.extensions import connection as Connection, cursor as Cursor
from ctypes import c_size_t, c_float, c_ubyte, c_bool
import re
from math import isfinite
from traceback import print_exc

# Type alias
Executor = Callable[[List[str], Cursor], None]



##### CLASS DEFINITIONS #####

# Due to the way progress bars print, there should never be more than one, so this is a singleton,
# although technically each child process will have its own copy.
class ProgressBar:
    __instance = None

    def __new__(cls, *args: Any, **kwargs: Any) -> "ProgressBar":
        if cls.__instance is None:
            cls.__instance = super().__new__(cls)
        return cls.__instance

    def __init__(self, total: int, length: int, precision: int = 0) -> None:
        self.bar_length = length
        # Using synchronized Values allows us to keep the same value for all child processes. No
        # need for locks, since access to __call__ should always be externally synchronized anyway.
        self.first = RawValue(c_bool, True)
        self.loaded = RawValue(c_size_t, 0)
        self.total = total
        if DEBUG:
            self.total_str = f"(<DEBUG>{{}}/{self.total}) "
        # Display new_percentage next to loading bar:
        self.perc = RawValue(c_float, 0.)
        self.precision = precision
        self.format = max(0, self.precision)
        # Number of "full" characters in the loading bar:
        self.fill = RawValue(c_ubyte, 0)

    def __call__(self) -> None:
        if self.first.value:
            self.first.value = False
        else:
            # Primary update
            self.loaded.value += 1
            #assert self.loaded.value <= self.total
            # Secondary & tertiary updates
            portion = self.loaded.value/self.total
            new_perc = round(portion*100, self.precision)
            new_fill = round(portion*self.bar_length)
            # If the display won't have changed, don't even bother reprinting it.
            if new_perc == self.perc.value and new_fill == self.fill.value and not DEBUG:
                return
            self.perc.value = new_perc
            self.fill.value = new_fill

        # Print the bar
        # TWO layers of formatting here:
        stdout.write(f"\r{{:.{self.format}f}}% [".format(self.perc.value))
        # Fill up the loading bar in proportion to the number of lines that have been loaded.
        for i in range(self.fill.value):
            stdout.write("=")
        if self.loaded.value == 0:
            stdout.write(" ")
        elif self.fill.value != self.bar_length:
            stdout.write(">")
        for i in range(self.fill.value + 1, self.bar_length):
            stdout.write(" ")
        stdout.write("] ")
        if DEBUG:
            stdout.write(self.total_str.format(self.loaded.value))
        stdout.flush()

# Why does everyone dislike singletons so much, anyway?



##### Helpers for executors #####

def numeric(val: str) -> Optional[float]:
    try:
        out = float(val)
        return out if isfinite(out) else None
    except Exception:
        return None

def integer(val: str) -> Optional[int]:
    try:
        return int(val)
    except Exception:
        return None

def wtype(val: str) -> int:
    return 1 if len(val) != 0 else 0

# Insert values into a PostgreSQL table
def insert_str(cur: Cursor, table: str, *insertions: Any) -> None:
    vals = ", ".join("%s" for i in insertions)
    cur.execute(f"INSERT INTO {table} VALUES ({vals})", insertions)

# Ensure that the list obtained from csv_split is the correct length
def data_length_check(row: List[str], expected_length: int) -> None:
    # For some ungodly reason, whenever the last element isn't blank, the length of the list is
    # one greater than it should be, but whenever you try popping it, it actually pops two
    # items. Why it does this, I do not know, but it seems relatively harmless.
    assert len(row) == expected_length or len(row) == expected_length + 1 \
        and len(row[expected_length]) == 0



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
    c_time = row[1] if len(row[1]) == 5 else "0" + row[1]

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



##### Helpers for main() subroutines #####

def get_connection() -> Tuple[Connection, Cursor]:
    conn = psycopg2.connect("host='localhost' dbname='dbms_final_project' user='dbms_project_user' "
        "password='dbms_password'")
    cur = conn.cursor()
    return conn, cur

def count_lines(mm: mmap, start: int, end: int) -> int:
    position = start
    count = 1
    # ":=" operator only available in Python 3.8
    while (new_position := mm.find(b"\n", position)) != -1 and new_position < end:
        count += 1
        position = new_position + 1
    # Don't count an empty line at the end of the file
    if position == end:
        count -= 1
    return count

# Create approximately equal sections of the file to give to child processes, "rounded" to the
# nearest newline.
def get_boundaries(mm: mmap, start: int, end: int, num_sections: int) -> List[int]:
    interval = (end - start)/num_sections
    # Create the list with a preset length
    boundaries = list(range(num_sections + 1))
    # Populate the list
    boundaries[0] = start
    boundaries[-1] = end
    n_code = ord("\n")
    for i in range(1, len(boundaries) - 1):
        # Find the nearest newline to the next interval & place the boundary after it
        high = low = round(i*interval)
        while True:
            if mm[(high := high + 1)] == n_code:
                boundaries[i] = high + 1
                break
            if mm[(low := low - 1)] == n_code:
                boundaries[i] = low + 1
                break
    return boundaries

# Determine the correct quantity & unit pairing (e.g. "1 line" or "? lines")
def plural_check(size: int, unit: str, units: str) -> str:
    return f"1 {unit}" if size == 1 else f"{size} {units}"

# Converts seconds to formatted ?h?m?s string, removing h and m if they're 0
def duration(seconds: float) -> str:
    hours = minutes = 0
    while seconds > 60:
        seconds -= 60
        minutes += 1
    while minutes > 60:
        minutes -= 60
        hours += 1
    return f"{{}}{{}}{seconds:.3f}s".format(f"{hours}h" if hours != 0 else "",
        f"{minutes}m" if minutes != 0 else "")



##### Subroutines for main() #####

# CHILD PROCESS: loop over a given section of the memory map
def proc_exec(name: str, fd_or_size: int, shm_tag: Optional[str], file_start: int, file_end: int,
        print_lock: Lock, executor: Executor, progress_bar: ProgressBar) -> None:
    # I want to avoid unnecessary, messy, interleaved stacktraces
    try:
        # Obtain the same memory map as in the parent process.
        if WINDOWS:
            mm = mmap(-1, fd_or_size, shm_tag, ACCESS_READ)
        else:
            mm = mmap(fd_or_size, 0, MAP_SHARED, PROT_READ)
        # Postgres connection objects
        conn, cur = get_connection()

        # LOOP
        line_start = file_start
        if DEBUG:
            line_num = 1
        while line_start < file_end:
            try:
                # Note that we can't use readline() because there's a chance it could change the
                # file position for the other processes as well. Not sure though.
                if (line_end := mm.find(b"\n", line_start, file_end)) == -1:
                    line_end = file_end
                # Strip any carriage returns due to Windows-style line endings, then convert to
                # proper text
                line = mm[line_start:line_end].rstrip(b"\r").decode()

                # CSV possibilities:
                # - Quoted string
                # - Non-empty unquoted string
                # - Empty unquoted string
                #   - Between two commas
                #   - Between beginning of line and comma
                #   - Between comma and end of line
                row = re.findall("(\"[^\"]*\"|[^,]+|(?<=,)(?=,)|^(?=,)|(?<=,)$)", line)
                # Remove surrounding quotes & unnecessary whitespace.
                for i, col in enumerate(row):
                    row[i] = re.sub("  +", " ", col.strip("\"").strip())
                
                executor(row, cur)
            except AssertionError:
                # TO DO: If the row length is less than expected, save the row & try to splice it
                # with the next one.
                # For now, just don't insert it, and skip it while still counting it toward the
                # overall progress.
                pass

            with print_lock:
                progress_bar()
            # Prepare for the next loop
            line_start = line_end + 1
            if DEBUG:
                line_num += 1
    except Exception as e:
        with print_lock:
            print()
            if DEBUG:
                # Print row, one line at a time
                print(f"<DEBUG>Offset: {file_start}", end = "", file = stderr)
                try:
                    line_num # Check for variable existence
                except NameError:
                    print(file = stderr)
                else:
                    print(f"; line: {line_num}", file = stderr)
                    try:
                        row
                    except NameError:
                        pass
                    else:
                        print(f"<DEBUG>Row contents:", file = stderr)
                        for i, col in enumerate(row):
                            print(f"  [{i}]: \"{col}\"", file = stderr)
                        print(f"  Length: {len(row)}", file = stderr)
            print(f"Process {name}:")
            print_exc()
        exit(1)
    except:
        exit(2)

    conn.commit() # Only commit on success

# Load the given file into memory & perform the given executor function upon each line of it.
def process_data(data_path: Path, open_flags: int, shm_tag: Optional[str], num_procs: int,
        executor: Executor, prog_config: Tuple[int, int]) -> int:
    # Use os.open instead of the built-in open() to avoid any unnecessary overhead in the creation
    # of a file object.
    fd = os.open(data_path, open_flags)
    # Use a memory map to reduce the number of I/O operations. API differs betwees OSes:
    if WINDOWS:
        #assert shm_tag
        # Obtain underlying file handle from file descriptor
        os.set_handle_inheritable(get_osfhandle(fd), True)
        mm = mmap(fd, 0, shm_tag, ACCESS_READ)
    else:
        os.set_inheritable(fd, True)
        mm = mmap(fd, 0, MAP_SHARED, PROT_READ)
    # Disregard the CSV file's header row by skipping past the first newline.
    file_start = mm.find(b"\n") + 1
    file_end = mm.size()

    # If n = the number of CPU cores, create n processes that each work on 1/n of the total file.
    num_procs = min(num_procs, line_count := count_lines(mm, file_start, file_end))
    boundaries = get_boundaries(mm, file_start, file_end, num_procs)
    if DEBUG:
        print("<DEBUG>Boundaries:", boundaries)
    # Create other variables for the child processes.
    print_lock = LockFactory()
    progress_bar = ProgressBar(line_count, *prog_config)
    # We avoid the actual Pool class so we can get some more control over what gets sent where.
    pool = tuple(Process(target = proc_exec, args = (str(i + 1), mm.size() if WINDOWS else fd,
        shm_tag, boundaries[i], boundaries[i + 1], print_lock, executor, progress_bar),
        daemon = True) for i in range(num_procs))

    # START PARSING
    progress_bar() # Initial print
    for p in pool:
        p.start()

    try: # Wait for completion of all or failure of one.
        sentinel_map = {p.sentinel: p for p in pool}
        sentinels = sentinel_map.keys() # Live view: updates with the dict
        while num_procs != 0:
            for sentinel in wait(sentinels): # wait() is a blocking call
                if sentinel_map.pop(sentinel).exitcode == 0:
                    num_procs -= 1
                else:
                    exit(1)
    except BaseException as e:
        # This section is reached when there's a keyboard interruption or something
        print()
        print(type(e).__name__, file = stderr)
        exit(2)

    print()
    mm.close()
    os.close(fd)
    return line_count

# Wrapper for processing data files
def import_dataset(category: str, dataset: Sequence[Path], open_flags: int, shm_tag: Optional[str],
        num_procs: int, executor: Executor, prog_config: Tuple[int, int]) -> None:
    for d in dataset:
        if not d.exists():
            print(f"ERROR: Data file \"{str(d)}\" does not exist!", file = stderr)
            exit(1)

    print(f"### Importing {category} data ###")

    total_time_start = perf_counter()
    total_line_count = 0
    for d in dataset:
        data_name = str(d)
        print(f"+++ Parsing \"{data_name}\" +++")

        time_start = perf_counter()
        line_count = process_data(d, open_flags, shm_tag, num_procs, executor, prog_config)
        time_elapsed = perf_counter() - time_start

        print(f"+++ Finished parsing \"{data_name}\" +++")
        print(f"    (processed {{}} in {duration(time_elapsed)})".format(
            plural_check(line_count, "line", "lines")))

        total_line_count += line_count
    total_time_elapsed = perf_counter() - total_time_start

    print(f"### Finished importing {category} data ###")
    print(f"    (processed {{}} from {{}} in {duration(total_time_elapsed)})".format(
        plural_check(total_line_count, "line", "lines"),
        plural_check(len(dataset), "file", "files")))

# Load the given SQL file into memory & run it
def create_schema(schema_path: Path) -> None:
    if not schema_path.exists():
        print(f"ERROR: Schema file \"{str(schema_path)}\" does not exist!", file = stderr)
        exit(1)

    print("### Creating schema ###")
    time_start = perf_counter()

    conn, cur = get_connection()
    # No need to memory map here, since we're only performing a single read.
    with open(schema_path, "r") as schema_file:
        cur.execute(schema_file.read())
    conn.commit()

    time_elapsed = perf_counter() - time_start
    print("### Finished creating schema ###")
    print(f"    (processed in {duration(time_elapsed)})")



##### MAIN #####

def main() -> None:
    # Note that this is a Path object, not a string of a path
    this_dir = Path(__file__).parent

    open_flags = os.O_RDONLY
    # A few things differ between operating systems
    if WINDOWS:
        if DEBUG:
            print("<DEBUG>Running on Windows", end = "\n\n")
        # Memory mapping and low-level (relatively) file opening have different implementations
        # between Windows and Unix-based systems.
        #assert hasattr(os, "O_BINARY")
        open_flags |= os.O_BINARY | os.O_RANDOM
        shm_tag = f"load_data_mmap_{os.getpid()}"
    else:
        if DEBUG:
            print("<DEBUG>Not running on Windows")
        shm_tag = None

    ### SET UP TABLES ###

    schema_file = this_dir.joinpath("schema.sql")
    create_schema(schema_file)
    print()

    ## LOAD DATA ##

    data_dir = this_dir.joinpath("datasets")
    if (num_cores := os.cpu_count()) is None:
        num_cores = 1
        if DEBUG:
            print("<DEBUG>Could not determine number of CPU cores", end = "\n\n")
    elif DEBUG:
        print(f"<DEBUG>CPU has {num_cores} cores", end = "\n\n")

    weather_data = (data_dir.joinpath("weather.csv"),)
    import_dataset("weather", weather_data, open_flags, shm_tag, num_cores, insert_weather_line,
        (32, 0 if DEBUG else -1))
    print()
    collision_data = (data_dir.joinpath("Motor_Vehicle_Collisions_-_Crashes.csv"),)
    import_dataset("collision", collision_data, open_flags, shm_tag, num_cores,
        insert_collision_line, (48, 2 if DEBUG else 1))

if __name__ == "__main__":
    main()
