import asyncio
import sys

from pulp_rsync.app.server import run


def main(args=None):
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run())


if __name__ == "__main__":
    sys.exit(main())
