"""The `name_records` subcommand provides a list of all the file names
list in the darshan log hash table.
"""
import sys
import argparse
import darshan




def setup_parser(parser=None):
    # setup nested actions/subcommands?
    #actions = parser.add_subparsers(dest='api')
    parser.description = "List all name records in Darshan log"

    # setup arguments
    parser.add_argument('input', help='darshan log file', nargs='?', default='example.darshan')
    parser.add_argument('--verbose', help='', action='store_true')
    parser.add_argument('--debug', help='', action='store_true')



def main(args=None):

    if args is None:
        parser = argparse.ArgumentParser(description='')
        setup_parser(parser)
        args = parser.parse_args()


    if args.debug:
        print(args)

    report = darshan.DarshanReport(args.input, read_all=True)  # Default behavior
    
    for nrec, path in report.name_records.items():
        print("{:<20} => {}".format(nrec, path))


if __name__ == "__main__":
    main()
