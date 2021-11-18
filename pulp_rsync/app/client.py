import asyncio
import math
import os
import random
from enum import IntFlag
from pathlib import PurePath

from django.conf import settings
from django.core.exceptions import ObjectDoesNotExist

from pulpcore.app.models import (
    BaseDistribution,
    PublishedArtifact,
)

from .args import parse_args


class FileListFlag(IntFlag):
    TopDir = 1 << 0
    ExtendedFlags = 1 << 2
    SameUid = 1 << 3
    SameGid = 1 << 4
    ModTimeNsec = 1 << 13


class BlockFlag(IntFlag):
    ReportATime = 1 << 0
    ReportTime = 1 << 3
    ReportPerms = 1 << 4
    ReportOwner = 1 << 5
    ReportGroup = 1 << 6
    ReportAcl = 1 << 7
    ReportXattr = 1 << 8
    BasisTypeFollows = 1 << 11
    IsNew = 1 << 13
    LocalChange = 1 << 14
    Transfer = 1 << 15


def to_varint(val):
    cnt = 3
    b = val.to_bytes(4, byteorder='little')
    while cnt >= 1 and b[cnt] == 0:
        cnt -= 1
    bit = 1 << (6 - cnt + 1)
    if b[cnt] >= bit:
        cnt += 1
        bf = ~(bit - 1)
    elif cnt >= 1:
        bf = b[cnt] | ~(bit * 2 - 1)
    else:
        bf = b[cnt]
    return bf.to_bytes(1, byteorder='little', signed=True) + b[:cnt]


def to_varlong(val, min_bytes):
    cnt = 7
    b = val.to_bytes(8, byteorder='little')
    while cnt >= min_bytes and b[cnt] == 0:
        cnt -= 1
    bit = 1 << (6 - cnt + min_bytes)
    if b[cnt] >= bit:
        cnt += 1
        bf = ~(bit - 1)
    elif cnt >= min_bytes:
        bf = b[cnt] | ~(bit * 2 - 1)
    else:
        bf = b[cnt]
    return bf.to_bytes(1, byteorder='little', signed=True) + b[:cnt]


def should_exclude(filters, path):
    matchpath = PurePath(path)
    for mode, pattern in filters:
        if matchpath.match(pattern):
            if mode == '+':
                return False
            elif mode == '-':
                return True


class RsyncClient:

    def __init__(self, reader, writer):
        self._raw_reader = reader
        self._reader = self._raw_reader
        self._writer = writer
        self._seed = random.randrange(1, 2 ** 32 - 1)

    async def _close(self):
        self._writer.write_eof()
        self._writer.close()

    async def _read(self, num):
        ret = await self._reader.read(num)
        if not ret:
            raise EOFError()
        return ret

    async def _read_int(self, num):
        return int.from_bytes(await self._reader.readexactly(num),
                              byteorder='little')

    async def _read_line(self):
        ret = await self._reader.readline()
        if not ret:
            raise EOFError()
        return ret[:-1].decode()

    async def _read_mux(self):
        while True:
            size = await self._raw_reader.readexactly(3)
            size = int.from_bytes(size, byteorder='little')
            tag = ord(await self._raw_reader.readexactly(1))
            ret = await self._raw_reader.readexactly(size)
            if tag == 7:
                return ret
            elif tag == 93:
                await self._close()
                raise EOFError('Client closed connection with MSG_ERROR_EXIT')
            else:
                print('Got %d bytes for tag %d - Ignoring' % (size, tag))

    async def _read_mux_forward(self):
        try:
            while True:
                data = await self._read_mux()
                self._reader.feed_data(data)
        except Exception as e:
            self._reader.set_exception(e)
        finally:
            self._reader.feed_eof()

    async def _read_strings(self):
        ret = []
        while True:
            item = (await self._reader.readuntil(b'\x00'))[:-1]
            if not item:
                break
            ret.append(item.decode())
        return ret

    async def _start_muxing(self):
        self._reader = asyncio.StreamReader()
        asyncio.ensure_future(self._read_mux_forward())

    async def _write_line(self, data):
        self._writer.write(data.encode() + b'\n')
        await self._writer.drain()

    async def _write_lines(self, data):
        self._writer.writelines([item.encode() + b'\n' for item in data])
        await self._writer.drain()

    async def _write_modules(self):
        base_paths = list(BaseDistribution.objects.values_list("name",
                                                               flat=True))
        await self._write_lines(
            [path + '\t' for path in base_paths] + ['@RSYNCD: EXIT'])

    async def _write_mux(self, data, tag=7):
        self._writer.write(len(data).to_bytes(3, byteorder='little'))
        self._writer.write(tag.to_bytes(1, byteorder='little'))
        self._writer.write(data)
        await self._writer.drain()

    async def handshake(self):
        await self._write_line('@RSYNCD: 30.0')
        await self._read_line()  # ver
        cmd = await self._read_line()

        if cmd in ['', '#list']:
            await self._write_modules()
            await self._close()
            return

        if cmd[0].startswith('#'):
            await self._write_line("@ERROR: Unknown command '" + cmd + "'")
            await self._close()
            return

        try:
            distro = BaseDistribution.objects.get(name=cmd).cast()
        except BaseDistribution.DoesNotExist:
            await self._write_line("@ERROR: Unknown module '" + cmd + "'")
            await self._close()
            return

        if distro.content_guard:
            await self._write_line("@ERROR: Unknown module '" + cmd + "'")
            await self._close()
            return

        await self._write_line('@RSYNCD: OK')
        return distro

    async def send_error(self, msg):
        await self._write_mux(msg.encode() + b'\n', 8)

    async def setup(self):
        raw_args = await self._read_strings()

        try:
            args = parse_args(raw_args)
        finally:
            self._writer.write(
                b'\x00' + self._seed.to_bytes(4, byteorder='little'))
            await self._writer.drain()

            await self._start_muxing()

        filters = []
        filter_len = await self._read_int(4)
        while filter_len:
            rule = await self._reader.readexactly(filter_len)
            rule = rule.decode().lstrip()
            if rule.startswith('+ ') or rule.startswith('- '):
                filters.append((rule[0], rule[2:]))
                if rule[2:].endswith('/'):
                    # Matching a dir - also match contents
                    filters.append((rule[0], rule[2:] + '*'))
                elif not rule[2:].endswith('/*'):
                    # Might also be a dir (and therefore contents)
                    filters.append((rule[0], rule[2:] + '/'))
                    filters.append((rule[0], rule[2:] + '/*'))
            else:
                await self.send_error(
                    "pulp_rsync: Unsupported filter '%s'" % (rule,))
            filter_len = await self._read_int(4)

        return args, filters

    async def send_file_list(self, distro, rel_path,
                             recurse=False, filters=None):
        publication = getattr(distro, 'publication', None)
        if not publication:
            await self.send_error('Distribution has no publication')

        dirs = {}
        artifacts = {}

        # First, assume they gave us an artifact path
        try:
            artifacts[rel_path] = publication.published_artifact.get(
                relative_path=rel_path)
        except ObjectDoesNotExist:
            # From here, we know it's a directory or it doesn't exist.
            # Enumerate as if it is a directory.
            dir_path = rel_path
            if dir_path and not dir_path.endswith('/'):
                dir_path += '/'
            for art in publication.published_artifact.filter(
                    relative_path__startswith=dir_path):
                lastsep = art.relative_path.rfind('/', len(dir_path))
                if lastsep >= 0:
                    stamp = art.content_artifact.artifact.pulp_created.timestamp()
                    subdirs = art.relative_path[len(dir_path):lastsep].split('/')
                    subdir = subdirs[0]
                    if dirs.get(subdir, math.inf) > stamp:
                        dirs[subdir] = stamp
                    if recurse:
                        for dirpart in subdirs[1:]:
                            subdir += '/' + dirpart
                            if dirs.get(subdir, math.inf) > stamp:
                                dirs[subdir] = stamp
                        artifacts[art.relative_path[len(dir_path):]] = art
                else:
                    artifacts[art.relative_path[len(dir_path):]] = art

            # If we found anything we KNOW it's a directory
            if dirs or artifacts:
                # Add an entry for '.'
                dirs['.'] = min(dirs.values(), default=math.inf)
                artstamp = min(
                    (art.content_artifact.artifact.pulp_created.timestamp()
                     for art in artifacts.values()),
                    default=math.inf)
                if dirs['.'] > artstamp:
                    dirs['.'] = artstamp

                # If they didn't supply a trailing '/', just show the single
                # directory, not the contents
                if rel_path and not rel_path.endswith('/'):
                    artifacts = {}
                    dirs = {rel_path: dirs['.']}

        if not dirs and not artifacts:
            await self.send_error(
                'rsync: link_stat "/%s" (in %s) failed: '
                'No such file or directory (2)' % (rel_path, distro.name))
            return

        flags = (
            FileListFlag.TopDir | FileListFlag.ExtendedFlags |
            FileListFlag.SameUid | FileListFlag.SameGid |
            FileListFlag.ModTimeNsec)
        for dirname, stamp in dirs.items():
            if filters and should_exclude(filters, dirname + '/'):
                continue
            if len(dirname) > 255:
                await self.send_error(
                    'No long path support! Files are missing!')
                continue
            entry = (
                flags.to_bytes(2, byteorder='little') +
                len(dirname).to_bytes(1, byteorder='little') +
                dirname.encode() +
                to_varlong(4096, 3) +
                to_varlong(int(stamp), 4) +
                to_varint(int((stamp - int(stamp)) * 1000000000)) +
                int(0o40755).to_bytes(4, byteorder='little')
            )
            await self._write_mux(entry)

        flags = (
            FileListFlag.ExtendedFlags | FileListFlag.SameUid |
            FileListFlag.SameGid | FileListFlag.ModTimeNsec)
        for artname, art in artifacts.items():
            if filters and should_exclude(filters, artname):
                continue
            if len(artname) > 255:
                await self.send_error(
                    'No long path support! Files are missing!')
                continue
            stamp = art.content_artifact.artifact.pulp_created.timestamp()
            entry = (
                flags.to_bytes(2, byteorder='little') +
                len(artname).to_bytes(1, byteorder='little') +
                artname.encode() +
                to_varlong(art.content_artifact.artifact.file.size, 3) +
                to_varlong(int(stamp), 4) +
                to_varint(int((stamp - int(stamp)) * 1000000000)) +
                int(0o100644).to_bytes(4, byteorder='little')
            )
            await self._write_mux(entry)

        artifacts.update(dirs)
        return artifacts

    async def transfer_blocks(self, flist):
        # Phase 1
        findex = -1
        ndx = await self._reader.readexactly(1)
        while ndx[0]:
            if ndx[0] == 0xff:
                await self.send_error(
                    'pulp_rsync: Negative indexes are not implemented')
                raise Exception('Got a negative index')
            elif ndx[0] == 0xfe:
                ndx += await self._reader.readexactly(2)
                if ndx[1] & 0x80:
                    findex = int.from_bytes(
                        (ndx[2:] +
                         await self._reader.readexactly(2) +
                         (ndx[1] & ~0x80).to_bytes(1, byteorder='little')),
                        byteorder='little')
                else:
                    findex += int.from_bytes(ndx[1:], byteorder='big')
            else:
                findex += ndx[0]

            flags = BlockFlag(await self._read_int(2))

            try:
                flist_entry = flist[findex]
            except IndexError:
                raise Exception('Invalid ndx value')
            if isinstance(flist_entry[1], float):
                # This is essentially a 'chdir' from what I can tell
                await self._write_mux(
                    ndx + flags.to_bytes(2, byteorder='little'))
                ndx = await self._reader.readexactly(1)
                continue
            elif not isinstance(flist_entry[1], PublishedArtifact):
                raise Exception('Got an %s' % (type(flist_entry[1]),))
            art = flist_entry[1]

            # TODO: Verify flags

            sum_count = await self._read_int(4)
            sum_blength = await self._read_int(4)
            sum_s2length = await self._read_int(4)
            sum_remainder = await self._read_int(4)
            await self._reader.readexactly(sum_count * (4 + sum_s2length))

            # TODO: Just ignore what the client has - send the whole file

            buf = (ndx +
                   flags.to_bytes(2, byteorder='little') +
                   sum_count.to_bytes(4, byteorder='little') +
                   sum_blength.to_bytes(4, byteorder='little') +
                   sum_s2length.to_bytes(4, byteorder='little') +
                   sum_remainder.to_bytes(4, byteorder='little'))

            buf += art.content_artifact.artifact.file.size.to_bytes(
                4, byteorder='little')

            filename = os.path.join(settings.MEDIA_ROOT,
                                    art.content_artifact.artifact.file.name)
            with open(filename, 'rb') as f:
                buf += f.read(524265)
                while len(buf) >= 524288:
                    await self._write_mux(buf)
                    buf = f.read(524288)

            # Token
            buf += b'\x00\x00\x00\x00'

            buf += bytes.fromhex(art.content_artifact.artifact.md5)

            await self._write_mux(buf)
            ndx = await self._reader.readexactly(1)
        await self._write_mux(b'\x00')

        # Phase 2
        assert await self._reader.readexactly(1) == b'\x00', \
            "Expected nothing in phase 2"
        await self._write_mux(b'\x00')

        # End
        assert await self._reader.readexactly(1) == b'\x00', \
            "Expected end of block transfer"
        await self._write_mux(b'\x00')
