import fuse
from fuse import fuse_file_info
from typing import Dict, Iterator, Iterable, Sequence, Tuple, Optional, Any, NamedTuple, Union, List

import logging
import errno
import time
import threading
import traceback

from . import util
from .types import *

class Node:
    """
    A node is the superclass of every entry in your filesystem.

    When working with the FUSE api, you have only one callback for each function,
    and must decide what to do based on the path.

    In constrast, FuseTree will find the node represented by the path and
    call the matching function on that object.

    e.g., `getattr('/foo/bar')` becomes `root['foo']['bar'].getattr()`

    Since node-creating operations, like `create`, `mkdir`, etc, receive paths that don't yet exist,
    they are instead called on their parent directories instead.

    e.g., `mkdir('/bar/foo/newdir')` becomes `root['bar']['foo'].mkdir('newdir')`

    Common implementations to several common node types are provided on `nodetypes`.
    """

    @property
    def attr_timeout(self):
        return 1

    @property
    def entry_timeout(self):
        return 1

    async def remember(self) -> None:
        """
        Hint that this node has been added to the kernel cache.
        On the root node it will be called once, when the FS is mounted -- Therefore it acts as fuse_init()
        """
        pass

    async def forget(self) -> None:
        """
        Hint that this node has been removed from the kernel cache

        On the root node it will be called once, when the FS is unmounted -- Therefore it acts as fuse_destroy()
        """
        pass

    async def lookup(self, name: str) -> Node_Like:
        """
        get one of this directory's child nodes by name
        (Or None if it doesn't exist)
        """
        return None

    async def getattr(self) -> Stat_Like:
        """
        Get file attributes.

        Similar to stat().  The 'st_dev' and 'st_blksize' fields are
        ignored. The 'st_ino' field is ignored except if the 'use_ino'
        mount option is given. In that case it is passed to userspace,
        but libfuse and the kernel will still assign a different
        inode for internal use (called the "nodeid").
        """
        raise fuse.FuseOSError(errno.ENOSYS)

    async def setattr(self, new_attr: Stat, to_set: List[str]) -> Stat_Like:
        cur_attr = await self.getattr()

        if 'st_mode' in to_set:
            await self.chmod(new_attr.st_mode)
        if 'st_uid' in to_set or 'st_gid' in to_set:
            await self.chown(
                new_attr.st_uid if 'st_uid' in to_set else cur_attr.st_uid,
                new_attr.st_gid if 'st_gid' in to_set else cur_attr.st_gid)
        if 'st_size' in to_set:
            await self.truncate(new_attr.st_size)
        if 'st_atime' in to_set or 'st_mtime' in to_set or 'st_ctime' in to_set:
            await self.utimens(
                new_attr.st_atime if 'st_atime' in to_set else cur_attr.st_atime,
                new_attr.st_mtime if 'st_mtime' in to_set else cur_attr.st_mtime)

        return await self.getattr()

    async def chmod(self, amode: int) -> None:
        """
        Change the permission bits of a file
        """
        raise fuse.FuseOSError(errno.ENOSYS)

    async def chown(self, uid: int, gid: int) -> None:
        """
        Change the owner and group of a file.
        """
        raise fuse.FuseOSError(errno.ENOSYS)

    async def truncate(self, length: int) -> None:
        """
        Change the size of a file
        """
        raise fuse.FuseOSError(errno.ENOSYS)

    async def utimens(self, atime: float, mtime: float) -> None:
        """
        Change the access and modification times of a file with
        nanosecond resolution
        """
        raise fuse.FuseOSError(errno.ENOSYS)

    async def readlink(self) -> str:
        """
        Read the target of a symbolic link
        """
        raise fuse.FuseOSError(errno.ENOSYS)

    async def mknod(self, name: str, mode: int, dev: int) -> Node_Like:
        """
        Create a file node

        This is called for creation of all non-directory, non-symlink
        nodes.  If the filesystem defines a create() method, then for
        regular files that will be called instead.
        """
        raise fuse.FuseOSError(errno.ENOSYS)

    async def mkdir(self, name: str, mode: int) -> Node_Like:
        """
        Create a directory

        Note that the mode argument may not have the type specification
        bits set, i.e. S_ISDIR(mode) can be false.  To obtain the
        correct directory type bits use  mode|S_IFDIR
        """
        raise fuse.FuseOSError(errno.ENOSYS)

    async def unlink(self, name: str) -> None:
        """
        Remove a file
        """
        raise fuse.FuseOSError(errno.ENOSYS)

    async def rmdir(self, name: str) -> None:
        """
        Remove a directory
        """
        raise fuse.FuseOSError(errno.ENOSYS)

    async def symlink(self, name: str, target: str) -> Node_Like:
        """
        Create a symbolic link
        """
        raise fuse.FuseOSError(errno.ENOSYS)

    async def rename(self, old_name: str, new_parent: 'Node', new_name: str) -> None:
        """
        Rename a file
        """
        raise fuse.FuseOSError(errno.ENOSYS)

    async def link(self, name: str, node: 'Node') -> Node_Like:
        """
        Create a hard link to a file
        """
        raise fuse.FuseOSError(errno.ENOSYS)

    async def open(self, mode: int) -> 'FileHandle':
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

    async def setxattr(self, name: str, value: bytes, flags: int) -> None:
        """
        Set extended attributes
        """
        raise fuse.FuseOSError(errno.ENOSYS)

    async def getxattr(self, name: str) -> bytes:
        """
        Get extended attributes
        """
        raise fuse.FuseOSError(errno.ENOSYS)

    async def listxattr(self) -> Iterable[str]:
        """
        List extended attributes
        """
        raise fuse.FuseOSError(errno.ENOSYS)

    async def removexattr(self, name: str) -> None:
        """
        Remove extended attributes
        """
        raise fuse.FuseOSError(errno.ENOSYS)

    async def opendir(self) -> DirHandle_Like:
        """
        Open directory

        Unless the 'default_permissions' mount option is given,
        this method should check if opendir is permitted for this
        directory. Optionally opendir may also return an arbitrary
        filehandle in the fuse_file_info structure, which will be
        passed to readdir, closedir and fsyncdir.
        """
        raise fuse.FuseOSError(errno.ENOSYS)

    async def statfs(self) -> StatVFS:
        """
        Get file system statistics

        The 'f_favail', 'f_fsid' and 'f_flag' fields are ignored
        """
        raise fuse.FuseOSError(errno.ENOSYS)


    async def access(self, amode: int) -> None:
        """
        Check file access permissions

        This will be called for the access() system call.  If the
        'default_permissions' mount option is given, this method is not
        called.
        """
        pass

    async def create(self, name: str, mode: int) -> 'FileHandle':
        """
        Check file access permissions

        This will be called for the access() system call.  If the
        'default_permissions' mount option is given, this method is not
        called.
        """
        raise fuse.FuseOSError(errno.ENOSYS)



class DirHandle:
    """
    A DirHandle is what you get with a call to `opendir()`.

    It can be used to list the directory contents (very important) and to fsync (optional)

    You probably don't need to deal with this class directly: Just return a collection
    of the file names on `opendir` and a new DirHandle will be created automatically
    """

    def __init__(self, node: Node = None) -> None:
        self.node = node

    async def readdir(self) -> Iterable[DirEntry]:
        """
        Read directory
        """
        raise fuse.FuseOSError(errno.ENOSYS)

    async def fsyncdir(self, datasync: int) -> None:
        """
        Synchronize directory contents

        If the datasync parameter is non-zero, then only the user data
        should be flushed, not the meta data
        """
        raise fuse.FuseOSError(errno.ENOSYS)

    async def releasedir(self) -> None:
        """
        Release directory
        """
        pass



class FileHandle:
    """
    A FileHandle is what you get with a call to `open()` or `create()`.

    Most importantly, you should implement `read` and/or `write`.

    You probably don't need to deal with this class directly: Just return a blob from `read()`
    and a FileHandle will be created automatically -- Otherwise, check the many common implementation in `nodetypes`
    """

    def __init__(self, node: Node = None, direct_io: bool = False, nonseekable: bool = False) -> None:
        self.node = node
        self.direct_io = direct_io
        self.nonseekable = nonseekable

    async def getattr(self) -> Stat_Like:
        """
        Get file attributes of an open file.

        Similar to stat().  The 'st_dev' and 'st_blksize' fields are
        ignored. The 'st_ino' field is ignored except if the 'use_ino'
        mount option is given. In that case it is passed to userspace,
        but libfuse and the kernel will still assign a different
        inode for internal use (called the "nodeid").
        """
        raise fuse.FuseOSError(errno.ENOSYS)

    async def setattr(self, new_attr: Stat, to_set: List[str]) -> Stat_Like:
        cur_attr = await self.getattr()

        if 'st_mode' in to_set:
            await self.chmod(new_attr.st_mode)
        if 'st_uid' in to_set or 'st_gid' in to_set:
            await self.chown(
                new_attr.st_uid if 'st_uid' in to_set else cur_attr.st_uid,
                new_attr.st_gid if 'st_gid' in to_set else cur_attr.st_gid)
        if 'st_size' in to_set:
            await self.truncate(new_attr.st_size)
        if 'st_atime' in to_set or 'st_mtime' in to_set or 'st_ctime' in to_set:
            await self.utimens(
                new_attr.st_atime if 'st_atime' in to_set else cur_attr.st_atime,
                new_attr.st_mtime if 'st_mtime' in to_set else cur_attr.st_mtime)

        return await self.getattr()

    async def chmod(self, amode: int) -> None:
        """
        Change the permission bits of an open file.
        """
        raise fuse.FuseOSError(errno.ENOSYS)

    async def chown(self, uid: int, gid: int) -> None:
        """
        Change the owner and group of an open file.
        """
        raise fuse.FuseOSError(errno.ENOSYS)

    async def truncate(self, length: int) -> None:
        """
        Change the size of an open file.
        """
        raise fuse.FuseOSError(errno.ENOSYS)

    async def utimens(self, atime: float, mtime: float) -> None:
        """
        Change the access and modification times of a file with
        nanosecond resolution
        """
        raise fuse.FuseOSError(errno.ENOSYS)

    async def read(self, size: int, offset: int) -> bytes:
        """
        Read data from an open file

        Read should return exactly the number of bytes requested except
        on EOF or error, otherwise the rest of the data will be
        substituted with zeroes. An exception to this is when the
        'direct_io' mount option is specified, in which case the return
        value of the read system call will reflect the return value of
        this operation.
        """
        raise fuse.FuseOSError(errno.ENOSYS)

    async def write(self, data: bytes, offset: int) -> int:
        """
        Write data to an open file

        Write should return exactly the number of bytes requested
        except on error. An exception to this is when the 'direct_io'
        mount option is specified (see read operation).
        """
        raise fuse.FuseOSError(errno.ENOSYS)

    async def flush(self) -> None:
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
        pass

    async def release(self) -> None:
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

    async def fsync(self, datasync: int) -> None:
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

    async def lock(self, cmd: int, lock: Any) -> None:
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

