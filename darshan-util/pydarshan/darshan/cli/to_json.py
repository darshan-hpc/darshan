"""The `to_json` subcommand dumps the darshan log to json format.
"""
import sys
import argparse
import darshan




def setup_parser(parser=None):
    # setup nested actions/subcommands?
    #actions = parser.add_subparsers(dest='api')
    parser.description = "Convert Darshan log into report in JSON format"

    # setup arguments
    parser.add_argument('input', help='darshan log file', nargs='?', default='example.darshan')
    parser.add_argument('--verbose', help='', action='store_true')
    parser.add_argument('--debug', help='', action='store_true')

    parser.add_argument(
        "--exclude_names",
        "-e",
        action='append',
        help="regex patterns for file record names to exclude in JSON output"
    )
    parser.add_argument(
        "--include_names",
        "-i",
        action='append',
        help="regex patterns for file record names to include in JSON output"
    )

def main(args=None):

    if args is None:
        parser = argparse.ArgumentParser(description='')
        setup_parser(parser)
        args = parser.parse_args()

    if args.debug:
        print(args)

    filter_patterns = None
    filter_mode = "exclude"
    if args.exclude_names and args.include_names:
        raise ValueError('Only one of --exclude_names and --include_names may be used.')
    elif args.exclude_names:
        filter_patterns = args.exclude_names
        filter_mode = "exclude"
    elif args.include_names:
        filter_patterns = args.include_names
        filter_mode = "include"

    # apply the filter mode and patterns into the Report from which the json file is generated
    report = darshan.DarshanReport(
        args.input,
        read_all=True,
        filter_patterns=filter_patterns,
        filter_mode=filter_mode
    )

    print(report.to_json())


if __name__ == "__main__":
    main()
