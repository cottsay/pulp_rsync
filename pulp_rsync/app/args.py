import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--server', action='store_true')
parser.add_argument('--sender', action='store_true')
parser.add_argument('--debug')
parser.add_argument('--dirs', '-d', action='store_true')
parser.add_argument('--recursive', '-r')
parser.add_argument('--rsh', '-e')
parser.add_argument('--times', '-t', action='store_true')
parser.add_argument('src')
parser.add_argument('dst')


def no_message(message):
    dont_exit(1, message)


def dont_exit(status=0, message=None):
    if status:
        raise Exception(message)


parser.exit = dont_exit
parser.error = no_message


def parse_args(args):
    return parser.parse_args(args)
