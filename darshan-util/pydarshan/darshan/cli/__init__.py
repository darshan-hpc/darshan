"""The cli package provides a basis for building future python
based command line utilities. Currently, the existing commands
provide basic examples with limited functionality.
"""
import argparse
import logging
from argparse import ArgumentDefaultsHelpFormatter
import importlib
import sys


logger = logging.getLogger(__name__)



class CustomHelpFormatter(argparse.HelpFormatter):
    def _format_action(self, action):
        if type(action) == argparse._SubParsersAction:
            # inject new class variable for subcommand formatting
            subactions = action._get_subactions()
            invocations = [self._format_action_invocation(a) for a in subactions]

            help_text = ""

            for name, choice in action.choices.items():
                help_text += "  {:21} {}\n".format(name, choice.description)

            return help_text

        if type(action) == argparse._SubParsersAction._ChoicesPseudoAction:
            # format subcommand help line
            subcommand = self._format_action_invocation(action) # type: str
            width = self._subcommand_max_length
            help_text = ""
            if action.help:
                help_text = self._expand_help(action)
            return "  {:{width}} -  {}\n".format(subcommand, help_text, width=width)

        elif type(action) == argparse._SubParsersAction:
            # process subcommand help section
            msg = '\n'
            for subaction in action._get_subactions():
                msg += self._format_action(subaction)
            return msg
        else:
            return super(CustomHelpFormatter, self)._format_action(action)



def discover_subcommands():
    """
    Enable experimental features such as aggregation methods for reports.

    Args:
        verbose (bool): Display log of enabled features. (Default: True)

    """
    import os
    import glob
    import importlib
    import darshan    

    subcommands = []

    paths = glob.glob(darshan.__path__[0] + "/cli/*.py")
    for path in paths:
        base = os.path.basename(path)
        name = os.path.splitext(base)[0]
        
        if name in ['__init__', '__main__']:
            continue

        subcommands.append(name)

    return subcommands



def main():
    """
    Darshan CLI wrapper, to expose individual commands as subcommands.
    """

    # early parsing for selected arguments
    preparser = argparse.ArgumentParser(
            usage="darshan <command>", 
            description='PyDarshan CLI Utilities', 
            add_help=False,
            formatter_class=CustomHelpFormatter)

    preparser.add_argument('--debug', help='', action='store_true', default=False)
    preparser.add_argument('--version', help='', action='store_true', default=False)

    # parse selected args early
    args, unkown_args = preparser.parse_known_args()

    # be verbose for debugging
    if args.debug:
        logging.basicConfig(level=logging.DEBUG)
        # TODO: Unfortunetly, this does not propagate to the libdarshan discovery utility process which already happened at this point

        #for name in logging.root.manager.loggerDict:
        #    print(name)
        #    logging.getLogger(name).setLevel(logging.DEBUG)
        print(args)

    if args.version:
        import darshan
        print(darshan.__version__)
        sys.exit()



    #parser = argparse.ArgumentParser(usage="darshan <command>", description='Darshan CLI Utilities', formatter_class=ArgumentDefaultsHelpFormatter)
    parser = argparse.ArgumentParser(usage="darshan <command>", description='PyDarshan CLI Utilities', formatter_class=CustomHelpFormatter)

    # Shared Optional Arguments
    optionals = parser.add_argument_group()
    optionals.add_argument("--config-file", dest='config_file', action='store', type=str,
                            help='Specify Absolute/relative path to desired config file')
    optionals.add_argument('--output', type=str.lower, nargs=1,
                            choices=['json', 'json-pretty', 'yaml', 'yaml', 'dict', 'dict-pretty'],
                            help='Format responses as dict, json, or yaml')
    optionals.add_argument("--log-level", type=str.lower, nargs=1,
                            choices=['debug', 'info', 'warning', 'error', 'critical'],
                            help='Set logging level')
    optionals.add_argument("--no-timestamp", dest='no_timestamp', action='store_true',
                            help='Removes timestamp from log events')

    optionals.add_argument('--debug', help='', action='store_true', default=False)
    optionals.add_argument('--version', help='', action='store_true', default=False)



    # setup parser for sub-commands
    subparsers = parser.add_subparsers(dest='action')

    # custom help messge
    parser._positionals.title = "commands"

    subcmds = discover_subcommands()
    for subcmd in subcmds:
        subcmd_parser = subparsers.add_parser(subcmd)

        mod = importlib.import_module('darshan.cli.{0}'.format(subcmd))
        mod.setup_parser(subcmd_parser)


    args = parser.parse_args()

    # be verbose for debugging (again now with full known options)
    if args.debug:
        print(args)


    # default behavior when no arguments provided: show help
    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(0)

    # route subcommands
    if args.action in subcmds:
        mod = importlib.import_module('darshan.cli.{0}'.format(args.action))
        mod.main(args)



if __name__ == "__main__":
    main()
