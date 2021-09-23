"""The `info` subcommand provides a basic dump of information about
the darshan log equivalent to DarshanReport.info().
"""
import sys
import argparse
import darshan




def setup_parser(parser=None):
    # setup nested actions/subcommands?
    #actions = parser.add_subparsers(dest='api')
    parser.description = "Display basic information about the Darshan log"

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
    report.info()



if __name__ == "__main__":
    main()
