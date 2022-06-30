import argparse
import pathlib

import cosima_cookbook as cc


def main(argv=None):
    parser = argparse.ArgumentParser(description="Update COSIMA cookbook database.")
    parser.add_argument(
        "dirs", type=pathlib.Path, nargs="+", help="Directories to index."
    )
    parser.add_argument(
        "-db",
        "--database",
        dest="db",
        action="store",
        default="cosima_master.db",
        help="Database to update.",
    )
    args = parser.parse_args(argv)

    print(cc)

    print("Establishing a DB connection to: {}".format(args.db))
    session = cc.database.create_session(args.db, timeout=30)

    for dir in args.dirs:
        print("Indexing: {}".format(dir))
        cc.database.build_index(
            dir, session, prune="delete", force=False, followsymlinks=True, nfiles=1000
        )
