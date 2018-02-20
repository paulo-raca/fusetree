from typing import Dict, Iterable, Sequence, Tuple, NamedTuple, Union, Any

class Stat(NamedTuple):
    st_dev: int = None
    st_ino: int = None
    st_nlink: int = None
    st_mode: int = None
    st_uid: int = None
    st_gid: int = None
    st_rdev: int = None
    st_size: int = None
    st_blksize: int = None
    st_blocks: int = None
    st_flags: int = None
    st_gen: int = None
    st_lspare: int = None
    st_qspare: int = None
    st_atime: float = None
    st_mtime: float = None
    st_ctime: float = None
    st_birthtime: float = None

    def with_values(self, **kwargs):
        values = self.as_dict()
        values.update(kwargs)
        return Stat(**values)

    def as_dict(self) -> dict:
        return {
            k: v
            for (k, v) in self._asdict().items()
            if v is not None
        }

class StatVFS(NamedTuple):
    f_bsize: int = None
    f_frsize: int = None
    f_blocks: int = None
    f_bfree: int = None
    f_bavail: int = None
    f_files: int = None
    f_ffree: int = None
    f_favail: int = None
    f_fsid: int = None
    f_flag: int = None
    f_namemax: int = None

    def as_dict(self) -> dict:
        return {
            k: v
            for (k, v) in self._asdict().items()
            if v is not None
        }

class Path:
    def __init__(self, elements: Sequence[Tuple[str, 'core.Node']]) -> None:
        self.elements = elements

    @property
    def target_node(self):
        return self.elements[-1][1]

    @property
    def parent_path(self):
        return Path(self.elements[:-1])

    @property
    def parent_node(self):
        return self.elements[:-2][1]

    def __str__(self):
        return '/'.join([name for name, node in self.elements])

    def __repr__(self):
        return 'Path([' + repr


Bytes_Like = Union[bytes, str]
Stat_Like = Union[Stat, dict, int]
StatVFS_Like = Union[StatVFS, dict]
Node_Like = Union['core.Node', Bytes_Like, Dict[str, Any]]
FileHandle_Like = Union['core.FileHandle', Bytes_Like, Iterable[Union[str, bytes]]]
DirEntry = Union[str, Tuple[str, Node_Like]]
DirHandle_Like = Union['core.DirHandle', Iterable[str], Iterable[Tuple[str, int]]]

from . import core
