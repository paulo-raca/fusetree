from stat import S_IFDIR, S_IFLNK, S_IFREG
from typing import Dict, Iterable
import urllib.request

from .types import *
from .core import *
from . import types_conv

class BlobFile(Node):
    def __init__(self, data: bytes, mode: int = 0o444) -> None:
        self.data = data
        self.mode = mode & 0o777

    def getattr(self, path: Path) -> Stat:
        return Stat(
            st_mode=S_IFREG | self.mode,
            st_size=len(self.data)
        )

    def open(self, path: Path, mode: int) -> FileHandle:
        return BlobFile.Handle(self, self.data)


    class Handle(FileHandle):
        def __init__(self, node: Node, data: bytes) -> None:
            super().__init__(node)
            self.data = data

        def getattr(self, path: Path) -> Stat:
            return Stat(
                st_size=len(self.data),
                **types_conv.as_stat(super().getattr(path))._asdict())

        def read(self, path: Path, size: int, offset: int) -> bytes:
            return self.data[offset : offset + size]


class GeneratorFile(Node):
    def __init__(self, generator: Iterable[Bytes_Like], mode: int = 0o444, min_read_len: int = -1) -> None:
        self.generator = generator
        self.mode = mode & 0o777
        self.min_read_len = min_read_len

    def getattr(self, path: Path) -> Stat:
        return Stat(
            st_mode=S_IFREG | self.mode,
            st_size=0
        )

    def open(self, path: Path, mode: int) -> FileHandle:
        return GeneratorFile.Handle(self, self.generator, self.min_read_len)

    class Handle(FileHandle):
        def __init__(self, node: Node, generator: Iterable[Bytes_Like], min_read_len: int = -1) -> None:
            super().__init__(node, direct_io = True)
            self.generator = iter(generator)
            self.current_blob = b''
            self.current_blob_position = 0
            self.min_read_len = min_read_len

        def read(self, path: Path, size: int, offset: int) -> bytes:
            ret = b''
            while size > len(ret) and self.current_blob is not None:
                n = min(size - len(ret), len(self.current_blob) - self.current_blob_position)

                if n > 0:
                    ret += self.current_blob[self.current_blob_position : self.current_blob_position + n]
                    self.current_blob_position += n
                else:
                    try:
                        self.current_blob = types_conv.as_bytes(next(self.generator))
                    except StopIteration:
                        self.current_blob = None
                    self.current_blob_position = 0

                if self.min_read_len > 0 and len(ret) >= self.min_read_len:
                    break
            return ret


def generatorfile(func):
    def tmp(*args, **kwargs):
        class Iterable:
            def __init__(self, func, *args, **kwargs):
                self.func = func
                self.args = args
                self.kwargs = kwargs

            def __iter__(self):
                return self.func(*self.args, **self.kwargs)

        iterable = Iterable(func, *args, **kwargs)
        return GeneratorFile(iterable)
    return tmp


class UrllibFile(Node):
    def __init__(self, url: str, mode: int = 0o444) -> None:
        self.url = url
        self.mode = mode & 0o777

    def getattr(self, path: Path) -> Stat:
        return Stat(
            st_mode=S_IFREG | self.mode,
            st_size=0
        )

    def open(self, path: Path, mode: int) -> FileHandle:
        return UrllibFile.Handle(self, self.url)

    class Handle(FileHandle):
        def __init__(self, node: Node, url: str) -> None:
            super().__init__(node, direct_io = True)
            self.url = url
            self.response = urllib.request.urlopen(self.url)

        def read(self, path: Path, size: int, offset: int) -> bytes:
            return self.response.read(size)

        def release(self, path: Path) -> None:
            self.response.close()


class DictDir(Node):
    def __init__(self, contents: Dict[str, Node_Like], mode: int = 0o444) -> None:
        self.contents = contents
        self.mode = mode & 0o777

    def getattr(self, path: Path) -> Stat:
        return Stat(
            st_mode=S_IFDIR | self.mode
        )

    def __getitem__(self, name: str) -> Node_Like:
        return self.contents.get(name, None)

    def opendir(self, path: Path) -> DirHandle_Like:
        return DictDir.Handle(self, self.contents.keys())

    class Handle(DirHandle):
        def __init__(self, node: Node, items: Iterable[DirEntry]) -> None:
            super().__init__(node)
            self.items = items

        def readdir(self, path: Path) -> Iterable[DirEntry]:
            yield from self.items
