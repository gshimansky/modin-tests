import os
import time
import pstats
import argparse
import pandas as pd

csv_columns = [
    "ncalls",
    "ncalls_percent",
    "primitive_calls",
    "primitive_calls_percent",
    "total_time",
    "total_time_percent",
    "cumulative_time",
]

parser = argparse.ArgumentParser(
    description="Read cProfile stats file and sort hotspots"
)
parser.add_argument("-f", dest="filename", required=True, help="Name of file to parse")
parser.add_argument(
    "-n", dest="number", default=10, type=int, help="Number of top functions to print"
)
parser.add_argument(
    "-k",
    dest="key",
    default="TIME",
    choices=list(pstats.SortKey.__members__.keys()) + csv_columns,
    help="Key to use for sorting function names. Capital values are keys for printing, lowcase are keys for CSV output.",
)
parser.add_argument("-o", dest="output", help="CSV file name to write")
parser.add_argument("-ah", action="store_true", dest="add_header", help="Add header to the right of table")
parser.add_argument("-no100", action="store_true", dest="no100", help="Do not multiply percentages by 100 (useful for Excel when formatting column as percentage)")
args = parser.parse_args()

ps = pstats.Stats(args.filename)

if args.output is None:
    ps.sort_stats(pstats.SortKey[args.key]).print_stats(args.number)
else:
    file_stats = os.stat(args.filename)
    header = pd.DataFrame.from_dict(
        data={
            args.filename: [
                time.ctime(file_stats.st_mtime),
                ps.total_calls,
                ps.prim_calls,
                ps.total_tt,
            ]
        },
        orient="index",
        columns=[
            "date",
            "total function calls",
            "primitive function calls",
            "total time",
        ],
    )
    header.index.name = "file name"
    print(header)

    if args.no100:
        percent_mult = 1
    else:
        percent_mult = 100

    df = pd.DataFrame.from_dict(
        data={
            func: [
                nc,
                float(nc) / float(ps.total_calls) * percent_mult,
                cc,
                float(cc) / float(ps.prim_calls) * percent_mult,
                tt,
                float(tt) / float(ps.total_tt) * percent_mult,
                ct,
            ]
            for (func, (cc, nc, tt, ct, callers)) in ps.stats.items()
        },
        orient="index",
        columns=csv_columns,
    )
    df.index.name = "function"
    df.sort_values(by=args.key, inplace=True, ascending=False)
    if args.add_header:
        df = pd.concat([df.reset_index(), header.reset_index()], axis=1)
    print(df)
    if args.output.endswith(".csv"):
        df.to_csv(args.output)
    elif args.output.endswith(".html"):
        df.to_html(args.output)
    else:
        raise Exception("Bad output file extension {}".format(args.output))

