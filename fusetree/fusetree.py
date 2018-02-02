import fuse
from fuse import fuse_file_info
from typing import Dict, Iterable, Tuple, Optional, Any

import logging
import errno
import time
import threading

from .util import LoggingFuseOperations
from .types import *
from .core import *
from .types_conv import *

class FuseTree(fuse.Operations):
    def __init__(self, rootNode: Node_Like, mountpoint, log=True, **kwargs) -> None:
        self.rootNode = as_node(rootNode)
        self._handle_lock = threading.Lock()
        self._next_handle = 1
        self._file_handles: Dict[int, FileHandle] = {}
        self._dir_handles: Dict[int, DirHandle] = {}

        kwargs.setdefault('fsname', self.rootNode.__class__.__name__)

        operations = self if not log else LoggingFuseOperations(self)
        fuse.FUSE(operations, mountpoint, raw_fi=True, **kwargs)


    def decode_path(self, path: str) -> Path:
        path_parts = [part for part in path.split('/') if part]
        node = self.rootNode
        route = [('', node)]
        for part in path_parts:
            node = as_node(node[part])
            route.append((part, node))
        return Path(route)

    def decode_folder(self, path: str) -> Tuple[Path, str]:
        path_parts = [part for part in path.split('/') if part]
        if len(path_parts) == 0:
            raise fuse.FuseOSError(errno.ENOSYS)

        last_part = path_parts[-1]
        path_parts = path_parts[:-1]

        node = self.rootNode
        route = [('', node)]
        for part in path_parts:
            node = as_node(node[part])
            route.append((part, node))
        return Path(route), last_part

    def _get_file_handle(self, fi: fuse_file_info) -> FileHandle:
        with self._handle_lock:
            return self._file_handles[fi.fh]

    def _add_file_handle(self, node: Node, filehandle: FileHandle, fi: fuse_file_info) -> None:
        with self._handle_lock:
            filehandle.node = node
            fh = self._next_handle
            self._next_handle += 1
            self._file_handles[fh] = filehandle
            fi.fh = fh
            fi.direct_io = 1 if filehandle.direct_io else 0

    def _release_file_handle(self, fi: fuse_file_info) -> None:
        with self._handle_lock:
            del self._file_handles[fi.fh]

    def _get_dir_handle(self, fh: int) -> DirHandle:
        with self._handle_lock:
            return self._dir_handles[fh]

    def _add_dir_handle(self, node: Node, dirhandle: DirHandle) -> int:
        with self._handle_lock:
            dirhandle.node = node
            fh = self._next_handle
            self._next_handle += 1
            self._dir_handles[fh] = dirhandle
            return fh

    def _release_dir_handle(self, fh: int) -> None:
        with self._handle_lock:
            del self._dir_handles[fh]

    def access(self, path: str, amode: int) -> int:
        decoded_path = self.decode_path(path)
        return decoded_path.target_node.access(decoded_path, amode)

    def bmap(self, path: str, blocksize: int, idx: int) -> int:
        decoded_path = self.decode_path(path)
        return decoded_path.target_node.bmap(decoded_path, blocksize, idx)

    def chmod(self, path: str, amode: int, fi: Optional[fuse_file_info] = None) -> None:
        decoded_path = self.decode_path(path)

        if fi is not None:
            filehandle = self._get_file_handle(fi)
            filehandle.chmod(decoded_path, amode)
        else:
            decoded_path.target_node.chmod(decoded_path, amode)

    def chown(self, path: str, uid: int, gid: int, fi: Optional[fuse_file_info] = None) -> None:
        decoded_path = self.decode_path(path)

        if fi is not None:
            filehandle = self._get_file_handle(fi)
            filehandle.chown(decoded_path, uid, gid)
        else:
            decoded_path.target_node.chown(decoded_path, uid, gid)

    def create(self, path: str, mode: int, fi: fuse_file_info) -> None:
        decoded_path, name = self.decode_folder(path)
        filehandle = decoded_path.target_node.create(decoded_path, name, mode)
        self._add_file_handle(decoded_path.target_node, filehandle, fi)

    def destroy(self, path: str) -> None:
        if isinstance(self.rootNode, RootNode):
            self.rootNode.destroy()

    def flush(self, path: str, fi: fuse_file_info) -> None:
        decoded_path = self.decode_path(path)
        filehandle = self._get_file_handle(fi)
        filehandle.flush(decoded_path)

    def fsync(self, path: str, datasync: int, fi: fuse_file_info) -> None:
        decoded_path = self.decode_path(path)
        filehandle = self._get_file_handle(fi)
        filehandle.fsync(decoded_path, datasync)

    def fsyncdir(self, path: str, datasync: int, fh) -> None:
        decoded_path = self.decode_path(path)
        dirhandle = self._get_dir_handle(fh)
        dirhandle.fsyncdir(decoded_path, datasync)

    def getattr(self, path: str, fi: Optional[fuse_file_info] = None) -> dict:
        decoded_path = self.decode_path(path)

        if fi is not None:
            filehandle = self._get_file_handle(fi)
            stat = filehandle.getattr(decoded_path)
        else:
            stat = decoded_path.target_node.getattr(decoded_path)

        return as_stat(stat).as_dict()

    def getxattr(self, path: str, name: str, position=0) -> bytes:
        decoded_path = self.decode_path(path)
        return decoded_path.target_node.getxattr(decoded_path, name)

    def init(self, path: str):
        if isinstance(self.rootNode, RootNode):
            self.rootNode.init()

    def link(self, path: str, target: str) -> None:
        decoded_path, name = self.decode_folder(path)
        decoded_target = self.decode_path(target)
        decoded_path.target_node.link(decoded_path, name, decoded_target)

    def listxattr(self, path: str) -> Iterable[str]:
        decoded_path = self.decode_path(path)
        return decoded_path.target_node.listxattr(decoded_path)

    def lock(self, path: str, fi: fuse_file_info, cmd: int, lock: Any) -> None:
        decoded_path = self.decode_path(path)
        filehandle = self._get_file_handle(fi)
        filehandle.lock(decoded_path, cmd, lock)

    def mkdir(self, path: str, mode: int) -> None:
        decoded_path, name = self.decode_folder(path)
        decoded_path.target_node.mkdir(decoded_path, name, mode)

    def mknod(self, path: str, mode: int, dev: int) -> None:
        decoded_path, name = self.decode_folder(path)
        decoded_path.target_node.mknod(decoded_path, name, mode, dev)

    def open(self, path: str, fi: fuse_file_info) -> None:
        decoded_path = self.decode_path(path)
        filehandle = decoded_path.target_node.open(decoded_path, fi.flags)
        self._add_file_handle(decoded_path.target_node, filehandle, fi)

    def opendir(self, path: str):
        decoded_path = self.decode_path(path)
        dirhandle = decoded_path.target_node.opendir(decoded_path)
        dirhandle = as_dirhandle(decoded_path.target_node, dirhandle)
        return self._add_dir_handle(decoded_path.target_node, dirhandle)

    def read(self, path: str, size: int, offset: int, fi: fuse_file_info) -> bytes:
        decoded_path = self.decode_path(path)
        filehandle = self._get_file_handle(fi)
        return filehandle.read(decoded_path, size, offset)

    def readdir(self, path: str, fh: int) -> Iterable[Tuple[str, dict, int]]:
        decoded_path = self.decode_path(path)
        dirhandle = self._get_dir_handle(fh)
        for entry in dirhandle.readdir(decoded_path):
            if isinstance(entry, str):
                yield entry, None, 0
            else:
                name, attrs = entry
                yield name, as_stat(attrs).as_dict(), 0

    def readlink(self, path: str) -> str:
        decoded_path = self.decode_path(path)
        return decoded_path.target_node.readlink(decoded_path)

    def release(self, path: str, fi: fuse_file_info) -> None:
        decoded_path = self.decode_path(path)
        filehandle = self._get_file_handle(fi)
        filehandle.release(decoded_path)
        self._release_file_handle(fi)

    def releasedir(self, path: str, fh: int):
        decoded_path = self.decode_path(path)
        dirhandle = self._get_dir_handle(fh)
        dirhandle.releasedir(decoded_path)
        self._release_dir_handle(fh)

    def removexattr(self, path: str, name: str) -> None:
        decoded_path = self.decode_path(path)
        decoded_path.target_node.removexattr(decoded_path, name)

    def rename(self, path: str, new_path: str) -> None:
        decoded_path = self.decode_path(path)
        decoded_new_path, new_name = self.decode_folder(new_path)
        decoded_path.target_node.rename(decoded_path, decoded_new_path, new_name)

    def rmdir(self, path: str) -> None:
        decoded_path = self.decode_path(path)
        decoded_path.target_node.rmdir(decoded_path)

    def setxattr(self, path: str, name: str, value: bytes, options: int, position: int = 0) -> None:
        decoded_path = self.decode_path(path)
        decoded_path.target_node.setxattr(decoded_path, name, value, options)

    def statfs(self, path: str) -> StatVFS:
        if isinstance(self.rootNode, RootNode):
            return self.rootNode.statfs()
        else:
            raise fuse.FuseOSError(errno.ENOSYS)

    def symlink(self, path: str, target: str) -> None:
        decoded_path, name = self.decode_folder(path)
        decoded_path.target_node.symlink(decoded_path, name, target)

    def truncate(self, path: str, length: int, fi: Optional[fuse_file_info] = None) -> None:
        decoded_path = self.decode_path(path)

        if fi is not None:
            filehandle = self._get_file_handle(fi)
            filehandle.truncate(decoded_path, length)
        else:
            decoded_path.target_node.truncate(decoded_path, length)

    def unlink(self, path: str) -> None:
        decoded_path = self.decode_path(path)
        decoded_path.target_node.unlink(decoded_path)

    def utimens(self, path: str, times: Tuple[float, float] = None, fi: Optional[fuse_file_info] = None) -> None:
        decoded_path = self.decode_path(path)

        if times is not None:
            atime, mtime = times
        else:
            atime = time.time()
            mtime = atime

        if fi is not None:
            filehandle = self._get_file_handle(fi)
            filehandle.utimens(decoded_path, atime, mtime)
        else:
            decoded_path.target_node.utimens(decoded_path, atime, mtime)

    def write(self, path: str, data: bytes, offset: int, fi: fuse_file_info) -> int:
        decoded_path = self.decode_path(path)
        filehandle = self._get_file_handle(fi)
        return filehandle.write(decoded_path, data, offset)
