import fuse
from fuse import fuse_file_info
from typing import Dict, Iterator, Iterable, Sequence, Tuple, Optional, Any, NamedTuple, Union
from stat import S_IFDIR, S_IFLNK, S_IFREG

import logging
import errno
import time
import threading
import traceback
import urllib.request

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

Bytes_Like = Union[bytes, str]
Stat_Like = Union[Stat, dict]
StatVFS_Like = StatVFS
Node_Like = Union['Node', Bytes_Like, Dict[str, 'Node_Like']]
FileHandle_Like = Union['FileHandle', Bytes_Like, Iterable[Union[str, bytes]]]
DirEntry = Union[str, Tuple[str, Stat_Like]]
DirHandle_Like = Union['DirHandle', Iterable[str]]

class Path:
    def __init__(self, path: Sequence[Tuple[str, 'Node']]) -> None:
        self.path = path
        self.target_node = path[-1][1]

    def __str__(self):
        return '/'.join([name for name, node in self.path])

    def __repr__(self):
        return 'Path([' + repr

class Node:
    def __getitem__(self, key: str) -> Node_Like:
        return None


    def getattr(self, path: Path) -> Stat_Like:
        """
        Get file attributes.

        Similar to stat().  The 'st_dev' and 'st_blksize' fields are
        ignored. The 'st_ino' field is ignored except if the 'use_ino'
        mount option is given. In that case it is passed to userspace,
        but libfuse and the kernel will still assign a different
        inode for internal use (called the "nodeid").
        """
        raise fuse.FuseOSError(errno.ENOSYS)

    def readlink(self, path: Path) -> str:
        """
        Read the target of a symbolic link
        """
        raise fuse.FuseOSError(errno.ENOSYS)

    def mknod(self, path: Path, name: str, mode: int, dev: int) -> None:
        """
        Create a file node

        This is called for creation of all non-directory, non-symlink
        nodes.  If the filesystem defines a create() method, then for
        regular files that will be called instead.
        """
        raise fuse.FuseOSError(errno.ENOSYS)

    def mkdir(self, path: Path, name: str, mode: int) -> None:
        """
        Create a directory

        Note that the mode argument may not have the type specification
        bits set, i.e. S_ISDIR(mode) can be false.  To obtain the
        correct directory type bits use  mode|S_IFDIR
        """
        raise fuse.FuseOSError(errno.ENOSYS)

    def unlink(self, path: Path) -> None:
        """
        Remove a file
        """
        raise fuse.FuseOSError(errno.ENOSYS)

    def rmdir(self, path: Path) -> None:
        """
        Remove a directory
        """
        raise fuse.FuseOSError(errno.ENOSYS)

    def symlink(self, path: Path, name: str, target: str) -> None:
        """
        Create a symbolic link

        TODO: `target` should probably be a `Path`?
        """
        raise fuse.FuseOSError(errno.ENOSYS)

    def rename(self, path: Path, new_path: Path, new_name: str) -> None:
        """
        Rename a file

        FIXME: fuse.h defines an extra `flags` argument
        """
        raise fuse.FuseOSError(errno.ENOSYS)

    def link(self, path: Path, name: str, target: Path) -> None:
        """
        Create a hard link to a file
        """
        raise fuse.FuseOSError(errno.ENOSYS)

    def chmod(self, path: Path, amode: int) -> None:
        """
        Change the permission bits of a file
        """
        raise fuse.FuseOSError(errno.ENOSYS)

    def chown(self, path: Path, uid: int, gid: int) -> None:
        """
        Change the owner and group of a file.
        """
        raise fuse.FuseOSError(errno.ENOSYS)

    def truncate(self, path: Path, length: int) -> None:
        """
        Change the size of a file
        """
        raise fuse.FuseOSError(errno.ENOSYS)

    def open(self, path: Path, mode: int) -> 'FileHandle':
        """
        File open operation

        No creation (O_CREAT, O_EXCL) and by default also no
        truncation (O_TRUNC) flags will be passed to open(). If an
        application specifies O_TRUNC, fuse first calls truncate()
        and then open(). Only if 'atomic_o_trunc' has been
        specified and kernel version is 2.6.24 or later, O_TRUNC is
        passed on to open.

        Unless the 'default_permissions' mount option is given,
        open should check if the operation is permitted for the
        given flags. Optionally open may also return an arbitrary
        filehandle in the fuse_file_info structure, which will be
        passed to all file operations.
        """
        raise fuse.FuseOSError(errno.ENOSYS)

    def setxattr(self, path: Path, name: str, value: bytes, flags: int) -> None:
        """
        Set extended attributes
        """
        raise fuse.FuseOSError(errno.ENOSYS)

    def getxattr(self, path: Path, name: str) -> bytes:
        """
        Get extended attributes
        """
        raise fuse.FuseOSError(errno.ENOSYS)

    def listxattr(self, path: Path) -> Iterable[str]:
        """
        List extended attributes
        """
        raise fuse.FuseOSError(errno.ENOSYS)

    def removexattr(self, path: Path, name: str) -> None:
        """
        Remove extended attributes
        """
        raise fuse.FuseOSError(errno.ENOSYS)

    def opendir(self, path: Path) -> DirHandle_Like:

        """
        Open directory

        Unless the 'default_permissions' mount option is given,
        this method should check if opendir is permitted for this
        directory. Optionally opendir may also return an arbitrary
        filehandle in the fuse_file_info structure, which will be
        passed to readdir, closedir and fsyncdir.
        """
        raise fuse.FuseOSError(errno.ENOSYS)

    def access(self, path: Path, amode: int) -> int:
        """
        Check file access permissions

        This will be called for the access() system call.  If the
        'default_permissions' mount option is given, this method is not
        called.
        """
        raise fuse.FuseOSError(errno.ENOSYS)

    def create(self, path: Path, name: str, mode: int) -> 'FileHandle':
        """
        Check file access permissions

        This will be called for the access() system call.  If the
        'default_permissions' mount option is given, this method is not
        called.
        """
        raise fuse.FuseOSError(errno.ENOSYS)

    def utimens(self, path: Path, atime: float, mtime: float) -> None:
        """
        Change the access and modification times of a file with
        nanosecond resolution
        """
        raise fuse.FuseOSError(errno.ENOSYS)

    def bmap(self, path: Path, blocksize: int, idx: int) -> int:
        """
        Map block index within file to block index within device

        Note: This makes sense only for block device backed filesystems
        mounted with the 'blkdev' option
        """
        raise fuse.FuseOSError(errno.ENOSYS)


class RootNode(Node):
    def init(self) -> None:
        """
        Initialize filesystem
        """
        pass

    def destroy(self) -> None:
        """
        Clean up filesystem

        Called on filesystem exit.
        """
        pass

    def statfs(self) -> StatVFS:
        """
        Get file system statistics

        The 'f_favail', 'f_fsid' and 'f_flag' fields are ignored
        """
        raise fuse.FuseOSError(errno.ENOSYS)


class DirHandle:
    def __init__(self, node: Node = None) -> None:
        self.node = node

    def readdir(self, path: Path) -> Iterable[DirEntry]:
        """
        Read directory
        """
        raise fuse.FuseOSError(errno.ENOSYS)

    def fsyncdir(self, path: Path, datasync: int) -> None:
        """
        Synchronize directory contents

        If the datasync parameter is non-zero, then only the user data
        should be flushed, not the meta data
        """
        raise fuse.FuseOSError(errno.ENOSYS)

    def releasedir(self, path: Path) -> None:
        """
        Release directory
        """
        pass



class FileHandle:
    def __init__(self, node: Node = None, direct_io: bool = False) -> None:
        self.node = node
        self.direct_io = direct_io

    def getattr(self, path: Path) -> Stat_Like:
        """
        Get file attributes of an open file.

        Similar to stat().  The 'st_dev' and 'st_blksize' fields are
        ignored. The 'st_ino' field is ignored except if the 'use_ino'
        mount option is given. In that case it is passed to userspace,
        but libfuse and the kernel will still assign a different
        inode for internal use (called the "nodeid").
        """
        return self.node.getattr(path)

    def chmod(self, path: Path, amode: int) -> None:
        """
        Change the permission bits of an open file.

        FIXME: This doesn't seem to be supported by fusepy
        """
        return self.node.chmod(path, amode)

    def chown(self, path: Path, uid: int, gid: int) -> None:
        """
        Change the owner and group of an open file.

        FIXME: This doesn't seem to be supported by fusepy
        """
        return self.node.chown(path, uid, gid)

    def truncate(self, path: Path, length: int) -> None:
        """
        Change the size of a file
        """
        return self.node.truncate(path, length)

    def read(self, path: Path, size: int, offset: int) -> bytes:
        """
        Read data from an open file

        Read should return exactly the number of bytes requested except
        on EOF or error, otherwise the rest of the data will be
        substituted with zeroes.	 An exception to this is when the
        'direct_io' mount option is specified, in which case the return
        value of the read system call will reflect the return value of
        this operation.
        """
        raise fuse.FuseOSError(errno.ENOSYS)

    def write(self, path: Path, data: bytes, offset: int) -> int:
        """
        Write data to an open file

        Write should return exactly the number of bytes requested
        except on error. An exception to this is when the 'direct_io'
        mount option is specified (see read operation).
        """
        raise fuse.FuseOSError(errno.ENOSYS)

    def flush(self, path: Path) -> None:
        """
        Possibly flush cached data

        BIG NOTE: This is not equivalent to fsync().  It's not a
        request to sync dirty data.

        Flush is called on each close() of a file descriptor.  So if a
        filesystem wants to return write errors in close() and the file
        has cached dirty data, this is a good place to write back data
        and return any errors.  Since many applications ignore close()
        errors this is not always useful.

        NOTE: The flush() method may be called more than once for each
        open().	This happens if more than one file descriptor refers
        to an opened file due to dup(), dup2() or fork() calls.	It is
        not possible to determine if a flush is final, so each flush
        should be treated equally.  Multiple write-flush sequences are
        relatively rare, so this shouldn't be a problem.

        Filesystems shouldn't assume that flush will always be called
        after some writes, or that if will be called at all.
        """
        raise fuse.FuseOSError(errno.ENOSYS)

    def release(self, path: Path) -> None:
        """
        Release an open file

        Release is called when there are no more references to an open
        file: all file descriptors are closed and all memory mappings
        are unmapped.

        For every open() call there will be exactly one release() call
        with the same flags and file descriptor.	 It is possible to
        have a file opened more than once, in which case only the last
        release will mean, that no more reads/writes will happen on the
        file.  The return value of release is ignored.
        """
        pass

    def fsync(self, path: Path, datasync: int) -> None:
        """
        Release an open file

        Release is called when there are no more references to an open
        file: all file descriptors are closed and all memory mappings
        are unmapped.

        For every open() call there will be exactly one release() call
        with the same flags and file descriptor.	 It is possible to
        have a file opened more than once, in which case only the last
        release will mean, that no more reads/writes will happen on the
        file.  The return value of release is ignored.
        """
        raise fuse.FuseOSError(errno.ENOSYS)

    def lock(self, path: Path, cmd: int, lock: Any) -> None:
        """
        Perform POSIX file locking operation

        The cmd argument will be either F_GETLK, F_SETLK or F_SETLKW.

        For the meaning of fields in 'struct flock' see the man page
        for fcntl(2).  The l_whence field will always be set to
        SEEK_SET.

        For checking lock ownership, the 'fuse_file_info->owner'
        argument must be used.

        For F_GETLK operation, the library will first check currently
        held locks, and if a conflicting lock is found it will return
        information without calling this method.	 This ensures, that
        for local locks the l_pid field is correctly filled in.	The
        results may not be accurate in case of race conditions and in
        the presence of hard links, but it's unlikely that an
        application would rely on accurate GETLK results in these
        cases.  If a conflicting lock is not found, this method will be
        called, and the filesystem may fill out l_pid by a meaningful
        value, or it may leave this field zero.

        For F_SETLK and F_SETLKW the l_pid field will be set to the pid
        of the process performing the locking operation.

        Note: if this method is not implemented, the kernel will still
        allow file locking to work locally.  Hence it is only
        interesting for network filesystems and similar.

        FIXME: fusepy doesn't seem to support it properly
        """
        raise fuse.FuseOSError(errno.ENOSYS)

    def utimens(self, path: Path, atime: float, mtime: float) -> None:
        """
        Change the access and modification times of a file with
        nanosecond resolution
        """
        return self.node.utimens(path, atime, mtime)


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
            return Stat(st_size=len(self.data), **FuseTree.to_stat(super().getattr(path))._asdict())

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
            #print(f'-->A {len(ret)} {size}')
            while size > len(ret) and self.current_blob is not None:
                n = min(size - len(ret), len(self.current_blob) - self.current_blob_position)
                #print(f'-->B {len(ret)} {size} {n}')

                if n > 0:
                    ret += self.current_blob[self.current_blob_position : self.current_blob_position + n]
                    self.current_blob_position += n
                else:
                    try:
                        self.current_blob = FuseTree.to_bytes(next(self.generator))
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
            print('close')


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



class LoggingFs(fuse.Operations):
    log = logging.getLogger('fusetree')

    def __init__(self, operations):
        self.operations = operations

    def __call__(self, op, path, *args):
        self.log.debug('-> %s %s %s', op, path, repr(args))
        ret = '[Unhandled Exception]'
        try:
            ret = self.operations.__call__(op, path, *args)
            return ret
        except OSError as e:
            traceback.print_exc()
            ret = str(e)
            raise
        finally:
            self.log.debug('<- %s %s', op, '%d bytes' % len(ret) if isinstance(ret, bytes) else repr(ret))


# create, mknod, mkdir, link, symlink, etc, are actually called on their parent folders

class FuseTree(fuse.Operations):
    def __init__(self, rootNode: Node_Like, mountpoint, **kwargs) -> None:
        self.rootNode = FuseTree.to_node(rootNode)
        self._handle_lock = threading.Lock()
        self._next_handle = 1
        self._file_handles: Dict[int, FileHandle] = {}
        self._dir_handles: Dict[int, DirHandle] = {}

        kwargs.setdefault('fsname', self.rootNode.__class__.__name__)
        fuse.FUSE(LoggingFs(self), mountpoint, raw_fi=True, **kwargs)


    def decode_path(self, path: str) -> Path:
        path_parts = [part for part in path.split('/') if part]
        node = self.rootNode
        route = [('', node)]
        for part in path_parts:
            node = FuseTree.to_node(node[part])
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
            node = FuseTree.to_node(node[part])
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

    @staticmethod
    def _is_iterable(x):
        try:
            iter(x)
            return True
        except:
            return False

    @staticmethod
    def to_bytes(data: Bytes_Like) -> bytes:
        if isinstance(data, bytes):
            return data
        elif isinstance(data, str):
            return data.encode('utf-8')
        else:
            return str(data).encode('utf-8')


    @staticmethod
    def to_node(node: Node_Like) -> Node:
        if node is None:
            raise fuse.FuseOSError(errno.ENOENT)
        elif isinstance(node, Node):
            return node
        elif isinstance(node, bytes) or isinstance(node, str):
            return BlobFile(FuseTree.to_bytes(node))
        elif isinstance(node, dict):
            return DictDir(node)
        elif FuseTree._is_iterable(node):
            return GeneratorFile(node)
        raise fuse.FuseOSError(errno.EIO)

    @staticmethod
    def to_filehandle(node: Node, filehandle: FileHandle_Like) -> FileHandle:
        if isinstance(filehandle, FileHandle):
            return filehandle
        elif isinstance(filehandle, bytes) or isinstance(filehandle, str):
            return BlobFile.Handle(node, FuseTree.to_bytes(filehandle))
        elif FuseTree._is_iterable(filehandle):
            return GeneratorFile.Handle(node, filehandle)
        raise fuse.FuseOSError(errno.EIO)

    @staticmethod
    def to_dirhandle(node: Node, dirhandle: DirHandle_Like) -> DirHandle:
        if isinstance(dirhandle, DirHandle):
            return dirhandle
        elif FuseTree._is_iterable(dirhandle):
            return DictDir.Handle(node, dirhandle)
        raise fuse.FuseOSError(errno.EIO)

    @staticmethod
    def to_stat(stat: Stat_Like) -> Stat:
        if isinstance(stat, Stat):
            return stat
        elif isinstance(stat, dict):
            return Stat(**stat)
        raise fuse.FuseOSError(errno.EIO)

    @staticmethod
    def stat_as_dict(stat: Stat) -> dict:
        return {
            k: v
            for (k, v) in stat._asdict().items()
            if v is not None
        }




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

        return FuseTree.stat_as_dict(FuseTree.to_stat(stat))

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
        dirhandle = FuseTree.to_dirhandle(decoded_path.target_node, dirhandle)
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
                yield name, FuseTree.stat_as_dict(FuseTree.to_stat(attrs)), 0

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
        decoded_new_path, new_name = self.decode_folder(path)
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
