import argparse

from commons import Bank
from inspector import Inspector

if __name__ == '__main__':

    # create and parse arguments
    ap = argparse.ArgumentParser()

    # ap.add_argument("-d", "--id", required=True, type=int,
    #                 help="Id of the branch")
    ap.add_argument("-b", "--bank", required=False, action='store_true',
                    help="Use this option to run an instance of Bank (a branch).")
    ap.add_argument("-i", "--inspector", required=False, action='store_true',
                    help="Use this option to run the inspector")

    args = ap.parse_args()

    if args.bank and args.inspector:
        raise "Use must use only one option."
    elif args.bank:
        branch = Bank()
        branch.run()
    elif args.inspector:
        inspector = Inspector()
        inspector.run()
    else:
        raise "Use one of the options (-b or -i)"