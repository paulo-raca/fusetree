from stat import S_IFDIR, S_IFLNK, S_IFREG
from typing import Dict, Iterable
import aiohttp
from io import BytesIO

import os

from .types import *
from .core import *
from . import types_conv

class BaseFile(Node):
    def __init__(self, mode: int = 0o444) -> None:
        self.mode = mode & 0o777

    async def getattr(self) -> Stat:
        return Stat(
            st_mode=S_IFREG | self.mode
        )


class BaseDir(Node):
    def __init__(self, mode: int = 0o444) -> None:
        self.mode = mode & 0o777

    async def getattr(self) -> Stat:
        return Stat(
            st_mode=S_IFDIR | self.mode
        )


class BaseSymlink(Node):
    def __init__(self, mode: int = 0o444) -> None:
        self.mode = mode & 0o777

    async def getattr(self) -> Stat:
        return Stat(
            st_mode=S_IFLNK | self.mode
        )


class Symlink(BaseSymlink):
    def __init__(self, link: str, mode: int = 0o444) -> None:
        super().__init__(mode)
        self.link = link

    async def readlink(self) -> str:
        return self.link



class BlobFile(BaseFile):
    def __init__(self, data: bytes = b'', mode: int = None, rw: bool = False) -> None:
        super().__init__(mode if mode is not None else 0o666 if rw else 0o444)
        self.rw = rw
        self.data = data
        self.shared_handle = None

    async def load(self) -> bytes:
        return self.data

    async def save(self, data: bytes) -> None:
        self.data = data

    async def open(self, mode: int) -> FileHandle:
        if self.shared_handle is None:
            self.shared_handle = BlobFile.Handle(self, await self.load())
        self.shared_handle.refs += 1
        return self.shared_handle

    async def getattr(self) -> Stat:
        if self.shared_handle is not None:
            size = len(self.shared_handle.buffer.getvalue())
        else:
            size=len(self.data)

        return Stat(
            st_mode=S_IFREG | self.mode,
            st_size=size
        )

    async def truncate(self, size: int) -> None:
        handle = await self.open(os.O_RDWR)
        try:
            await handle.truncate(size)
        finally:
            await handle.release()

    class Handle(FileHandle):
        def __init__(self, node: Node, data: bytes) -> None:
            super().__init__(node)
            self.buffer = BytesIO(data)
            self.dirty = False
            self.refs = 0

        async def read(self, size: int, offset: int) -> bytes:
            self.buffer.seek(offset)
            return self.buffer.read(size)

        async def write(self, buffer, offset):
            if not self.node.rw:
                raise fuse.FuseOSError(errno.EPERM)

            self.dirty = True
            self.buffer.seek(offset)
            self.buffer.write(buffer)
            return len(buffer)

        async def truncate(self, size: int) -> None:
            if not self.node.rw:
                raise fuse.FuseOSError(errno.EPERM)

            self.dirty = True
            self.buffer.truncate(size)

        async def flush(self) -> None:
            if self.dirty:
                await self.node.save(self.buffer.getvalue())
                self.dirty = None

        async def release(self) -> None:
            self.refs -= 1
            if self.refs == 0:
                await self.flush()
                self.node.shared_handle = None





class GeneratorFile(BaseFile):
    def __init__(self, generator: Iterable[Bytes_Like], mode: int = 0o444, min_read_len: int = -1) -> None:
        super().__init__(mode)
        self.generator = generator
        self.min_read_len = min_read_len

    async def open(self, mode: int) -> FileHandle:
        return GeneratorFile.Handle(self, self.generator, self.min_read_len)

    class Handle(FileHandle):
        def __init__(self, node: Node, generator: Iterable[Bytes_Like], min_read_len: int = -1) -> None:
            super().__init__(node, direct_io=True, nonseekable=True)

            self.generator = self.as_generator(generator)
            self.current_blob = b''
            self.current_blob_position = 0
            self.min_read_len = min_read_len

        async def read(self, size: int, offset: int) -> bytes:
            ret = b''
            while size > len(ret) and self.current_blob is not None:
                n = min(size - len(ret), len(self.current_blob) - self.current_blob_position)

                if n > 0:
                    ret += self.current_blob[self.current_blob_position : self.current_blob_position + n]
                    self.current_blob_position += n
                else:
                    try:
                        self.current_blob = types_conv.as_bytes(await self.generator.__anext__())
                    except StopAsyncIteration:
                        self.current_blob = None
                    self.current_blob_position = 0

                if self.min_read_len > 0 and len(ret) >= self.min_read_len:
                    break
            return ret

        def as_generator(self, generator):
            async def as_async_gen(data):
                for x in data:
                    yield x

            if hasattr(generator, '__anext__'):
                return generator
            elif hasattr(generator, '__aiter__'):
                return generator.__aiter__()
            elif hasattr(generator, '__next__'):
                return as_async_gen(generator)
            elif hasattr(generator, '__iter__'):
                return as_async_gen(iter(generator))
            elif callable(generator):
                return self.as_generator(generator())

            raise TypeError('Expected iterator, iterable, async iterator, async iterable or callable')

def generatorfile(func, async=None):
    def tmp(*args, **kwargs):
        return GeneratorFile(lambda: func(*args, **kwargs))
    return tmp


class HttpFile(BaseFile):
    def __init__(self, url: str, mode: int = 0o444) -> None:
        super().__init__(mode)
        self.url = url

    async def open(self, mode: int) -> FileHandle:
        session = await aiohttp.ClientSession().__aenter__()
        response = await (await session.get(self.url)).__aenter__()
        return HttpFile.Handle(self, session, response)

    class Handle(FileHandle):
        def __init__(self, node: Node, session, response) -> None:
            super().__init__(node, direct_io=True, nonseekable=True)
            self.session = session
            self.response = response

        async def read(self, size: int, offset: int) -> bytes:
            return await self.response.content.read(size)

        async def release(self) -> None:
            await self.response.__aexit__(None, None, None)
            await self.session.__aexit__(None, None, None)


class DictDir(BaseDir):
    def __init__(self, contents: Dict[str, Node_Like], mode: int = None, rw: bool = False) -> None:
        super().__init__(mode if mode is not None else 0o666 if rw else 0o444)
        self.rw = rw
        self.contents = contents

    # ====== RO operations ======

    async def lookup(self, name: str) -> Node_Like:
        return self.contents.get(name, None)

    async def opendir(self) -> DirHandle_Like:
        return DictDir.Handle(self, self.contents.keys())

    class Handle(DirHandle):
        def __init__(self, node: Node, items: Iterable[DirEntry]) -> None:
            super().__init__(node)
            self.items = items

        async def readdir(self) -> Iterable[DirEntry]:
            for item in self.items:
                yield item

    # ====== RW operations ======

    async def mknod(self, name: str, mode: int, dev: int) -> Node_Like:
        if not self.rw:
            raise fuse.FuseOSError(errno.EPERM)

        if dev != 0:
            raise fuse.FuseOSError(errno.ENOSYS)

        new_file = BlobFile(b'', mode, rw=True)
        self.contents[name] = new_file
        return new_file

    async def mkdir(self, name: str, mode: int) -> Node_Like:
        if not self.rw:
            raise fuse.FuseOSError(errno.EPERM)

        new_dir = DictDir({}, mode, rw=True)
        self.contents[name] = new_dir
        return new_dir

    async def unlink(self, name: str) -> None:
        if not self.rw:
            raise fuse.FuseOSError(errno.EPERM)

        del self.contents[name]

    async def rmdir(self, name: str) -> None:
        if not self.rw:
            raise fuse.FuseOSError(errno.EPERM)

        del self.contents[name]

    async def symlink(self, name: str, target: str) -> Node_Like:
        if not self.rw:
            raise fuse.FuseOSError(errno.EPERM)

        new_link = Symlink(target)
        self.contents[name] = new_link

    async def rename(self, old_name: str, new_parent: Node, new_name: str) -> None:
        if not isinstance(new_parent, DictDir):
            raise fuse.FuseOSError(errno.ENOSYS)

        if not self.rw or not new_parent.rw:
            raise fuse.FuseOSError(errno.EPERM)

        node = self.contents[name]
        del self.contents[name]
        new_parent.contents[name] = node

    async def link(self, name: str, node: Node) -> Node_Like:
        self.contents[name] = node
