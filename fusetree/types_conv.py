from .types import *
from . import core
from . import nodetypes
from . import util

import fuse
import errno

def as_bytes(data: Bytes_Like) -> bytes:
    if data is None:
        return b''
    elif isinstance(data, bytes):
        return data
    elif isinstance(data, str):
        return data.encode('utf-8')
    else:
        return str(data).encode('utf-8')

def as_node(node: Node_Like) -> 'core.Node':
    if node is None:
        raise fuse.FuseOSError(errno.ENOENT)
    elif isinstance(node, core.Node):
        return node
    elif isinstance(node, bytes) or isinstance(node, str):
        return nodetypes.BlobFile(as_bytes(node))
    elif isinstance(node, dict):
        return nodetypes.DictDir(node)
    elif util.is_iterable(node) or util.is_async_iterable(node):
        return nodetypes.GeneratorFile(node)
    raise fuse.FuseOSError(errno.EIO)

def as_filehandle(node: 'core.Node', filehandle: FileHandle_Like) -> 'core.FileHandle':
    if isinstance(filehandle, core.FileHandle):
        return filehandle
    elif isinstance(filehandle, bytes) or isinstance(filehandle, str):
        return nodetypes.BlobFile.Handle(node, as_bytes(filehandle))
    elif util.is_iterable(filehandle):
        return nodetypes.GeneratorFile.Handle(node, filehandle)
    raise fuse.FuseOSError(errno.EIO)

def as_dirhandle(node: 'core.Node', dirhandle: DirHandle_Like) -> 'core.DirHandle':
    if isinstance(dirhandle, core.DirHandle):
        return dirhandle
    elif util.is_iterable(dirhandle):
        return nodetypes.DictDir.Handle(node, dirhandle)
    raise fuse.FuseOSError(errno.EIO)

def as_stat(stat: Stat_Like) -> Stat:
    if isinstance(stat, int):
        return Stat(st_mode=stat)
    if isinstance(stat, Stat):
        return stat
    elif isinstance(stat, dict):
        return Stat(**stat)
    raise fuse.FuseOSError(errno.EIO)

def as_statvfs(statvfs: StatVFS_Like) -> StatVFS:
    if isinstance(statvfs, StatVFS):
        return statvfs
    elif isinstance(statvfs, dict):
        return StatVFS(**statvfs)
    raise fuse.FuseOSError(errno.EIO)
