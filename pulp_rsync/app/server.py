import asyncio
import os
import socket

import django
from django.conf import settings

from .client import RsyncClient

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "pulpcore.app.settings")
django.setup()

from pulpcore.app.models import ContentAppStatus


async def _heartbeat():
    name = "rsync-{pid}@{hostname}".format(pid=os.getpid(),
                                           hostname=socket.gethostname())
    heartbeat_interval = settings.CONTENT_APP_TTL
    while True:
        content_app_status, created = ContentAppStatus.objects.get_or_create(
            name=name)
        if not created:
            content_app_status.save_heartbeat()
        await asyncio.sleep(heartbeat_interval)


async def handle_client(reader, writer):
    try:
        client = RsyncClient(reader, writer)

        # Initial handshake
        distro = await client.handshake()
        if not distro:
            return

        try:
            # Get arguments
            args = await client.setup()

            # Send the file list
            rel_path = args.dst[len(distro.name) + 1:].lstrip('/')
            dirs_and_arts = await client.send_file_list(
                distro, rel_path, recurse=args.recursive)
            if not dirs_and_arts:
                return
        except Exception as e:
            await client.send_error('pulp_rsync: ' + str(e))
            raise
        finally:
            # Send the end to the file list
            await client._write_mux(b'\x00')

        # Sort the file list
        flist = sorted(dirs_and_arts.items())

        # Perform block transfer
        await client.transfer_blocks(flist)

        # TODO: Aggregate statistics
        await client._write_mux(
            b'\x00\x00\x00' +
            b'\x00\x00\x00' +
            b'\x00\x00\x00' +
            b'\x00\x00\x00' +
            b'\x00\x00\x00')

        # Farewell
        assert await client._reader.readexactly(1) == b'\x00'
        await client._write_mux(b'\x00')

    except Exception as e:
        print('Connection to client closed: %s: %s' % (type(e).__name__, e))
    finally:
        writer.close()


async def run():
    asyncio.ensure_future(_heartbeat())
    server = await asyncio.start_server(handle_client, port=1234)
    await server.wait_closed()
