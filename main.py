import argparse
import shutil

from bank import Bank
from commons import Constants
from inspector import Inspector

if __name__ == "__main__":

    # create and parse arguments
    ap = argparse.ArgumentParser()

    ap.add_argument(
        "-b",
        "--bank",
        required=False,
        action="store_true",
        help="Use this option to run an instance of Bank (a branch).",
    )
    ap.add_argument(
        "-i",
        "--inspector",
        required=False,
        action="store_true",
        help="Use this option to run the inspector",
    )
    ap.add_argument(
        "-c",
        "--clear",
        required=False,
        action="store_true",
        help="Clear the branches information file.",
    )

    args = ap.parse_args()

    if args.clear:
        try:
            shutil.rmtree(Constants.dir_logs)
            shutil.rmtree(Constants.dir_bank)
            print("bank/ and logs/ directories were removed.")
        except FileNotFoundError:
            print("directories do not exist. They were already removed.")

        exit(0)

    if args.bank and args.inspector:
        raise "You must only use one option."
    elif args.bank:
        branch = Bank()
        branch.run()
    elif args.inspector:
        inspector = Inspector()
        inspector.run()
    else:
        raise "Use one of the options (-b or -i)"
