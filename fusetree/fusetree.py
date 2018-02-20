import fusell
from fusell import fuse_file_info
from fuse import FuseOSError
from typing import Dict, Iterable, Tuple, Optional, Any

import logging
import errno
import time
import asyncio
import os
import ctypes
import threading
import time

from .util import LoggingFuseOperations
from .types import *
from .core import *
from .types_conv import *

_task_locals = {}
async def get_locals():
    loop = asyncio.get_event_loop()



class FuseTree(fusell.FUSELL):
    def __init__(self, rootNode: Node_Like, mountpoint, log=True, encoding='utf-8', loop=None, **kwargs) -> None:
        self.rootNode = as_node(rootNode)

        self._req_seq_lock = asyncio.Lock()
        self._next_req_seq = 1

        self._loop = loop
        self._loop_thread = None
        self._handle_lock = asyncio.Lock()
        self._inodes: Dict[int, Node] = {}
        self._inodes_refs: Dict[int, int] = {}
        self._next_fd = 1
        self._file_fds: Dict[int, FileHandle] = {}
        self._dir_fds: Dict[int, DirHandle] = {}

        super().__init__(mountpoint, encoding=encoding)


    async def _ino_to_node(self, inode: int) -> Node:
        async with self._handle_lock:
            try:
                return self._inodes[inode]
            except KeyError:
                raise FuseOSError(errno.ENOENT)

    async def _node_to_ino(self, node: Node, remember: bool = False) -> int:
        return 1 if node is self.rootNode else id(node)

    async def _update_inodef_refs(self, node: Node, update: int) -> int:
        ino = await self._node_to_ino(node)

        async with self._handle_lock:
            if update > 0:
                if ino not in self._inodes:
                    await node.remember()
                    self._inodes[ino] = node
                    self._inodes_refs[ino] = update
                else:
                    self._inodes_refs[ino] += update

            elif update < 0:
                if ino not in self._inodes:
                    raise FuseOSError(errno.ENOENT)
                else:
                    self._inodes_refs[ino] += update  # Remember, it's a negative number
                    if self._inodes_refs[ino] == 0:
                        await node.forget()
                        del self._inodes_refs[ino]
                        del self._inodes[ino]

            else:
                pass  # update == 0 -- Shouldn't happen, but is a no-op No-op

            new_refcount = self._inodes_refs.get(ino, 0)
            return new_refcount



    async def _reply_err(self, req, err: int) -> (int, str):
        self.reply_err(req, err)
        return (err, os.strerror(err))

    async def _reply_entry(self, req, node: Node) -> (int, Node):
        ino = await self._node_to_ino(node)
        await self._update_inodef_refs(node, +1)
        stat = as_stat(await node.getattr())
        stat = stat.with_values(st_ino=ino)

        entry = dict(
            ino=ino,
            attr=stat.as_dict(),
            attr_timeout=node.attr_timeout,
            entry_timeout=node.entry_timeout)

        self.reply_entry(req, entry)
        return ino, node

    async def _reply_attr(self, req, attr: Stat, ino: int, attr_timeout: float) -> Stat:
        attr = attr.with_values(st_ino=ino)

        self.reply_attr(req, attr.as_dict(), attr_timeout)
        return attr

    async def _reply_readlink(self, req, link: str) -> str:
        self.reply_readlink(req, link.encode(self.encoding))
        return link

    async def _reply_none(self, req, ret=None):
        self.reply_none(req)
        return ret

    async def _reply_write(self, req, n: int) -> str:
        self.reply_write(req, n)
        return f'{n} bytes'

    def _wrapper(method):
        def safe_arg(arg):
            """
            Copy arguments that are passed as pointers, as fuse_low_level destroys
            them as soon as the callback is returned
            """
            if isinstance(arg, ctypes._Pointer):
                if not arg:
                    return None

                contents = arg.contents
                if isinstance(contents, ctypes.Structure):
                    copy = type(contents)()
                    ctypes.pointer(copy)[0] = contents
                    return copy
                else:
                    raise Exception(f'Unsupported pointer type: {type(arg).__name__}')
            else:
                return arg


        def asstr(x):
            if isinstance(x, ctypes.Structure):
                return f'{x.__class__.__name__}(%s)' % (', '.join([
                  f'{entry[0]}={asstr(getattr(x, entry[0]))}'
                  for entry in x._fields_
                  if getattr(x, entry[0])
                ]))
            else:
                return str(x)

        async def wrapped(self, req, *args):
            async with self._req_seq_lock:
                req_seq = self._next_req_seq
                self._next_req_seq += 1

            print(f'{req_seq} > {method.__name__[5:]}:', ', '.join([asstr(arg) for arg in args]))

            try:
                result = await method(self, req, *args)
            except OSError as e:
                #traceback.print_exc()
                result = await self._reply_err(req, e.errno)
            except Exception as e:
                traceback.print_exc()
                await self._reply_err(req, errno.EFAULT)
                result = repr(e)

            print(f'{req_seq} < {method.__name__[5:]}: {result}')

        def wrapper(self, *args):
            # Pointer arguments are destroyed when the callback exits,
            # therefore they must be copied
            if method.__name__ == 'fuse_write':
                args = list(args)
                args[2] = ctypes.string_at(args[2], args[3])

            args = map(safe_arg, args)

            future = wrapped(self, *args)
            self._loop.call_soon_threadsafe(asyncio.async, future)

        return wrapper


    def fuse_init(self, userdata, conn):
        # Start event loop
        if self._loop is None:
            self._loop = asyncio.new_event_loop()
            def f(loop):
                asyncio.set_event_loop(loop)
                #print("AsyncIO loop started...")
                loop.run_forever()
                #print("AsyncIO loop completed")
            self._loop_thread = threading.Thread(target=f, args=(self._loop,))
            self._loop_thread.start()

        # Remember root node
        async def remember_root():
            await self._update_inodef_refs(self.rootNode, 1)
        asyncio.run_coroutine_threadsafe(remember_root(), self._loop).result()


    def fuse_destroy(self, userdata):
        # Forget all nodes
        async def forget_all():
            async with self._handle_lock:
                wait_for = []
                for node in self._inodes.values():
                    if node is not self.rootNode:
                        wait_for.append(node.forget())
                if len(wait_for):
                    await asyncio.wait(wait_for)

                # The root node is the last one to be forgotten, and acts as a destroy()
                await self.rootNode.forget()
                self._inodes.clear()
                self._inodes_refs.clear()

        asyncio.run_coroutine_threadsafe(forget_all(), self._loop).result()


        # Stop event loop
        if self._loop_thread is not None:
            self._loop.call_soon_threadsafe(self._loop.stop)
            self._loop_thread.join()

            self._loop = None
            self._loop_thread = None


    @_wrapper
    async def fuse_lookup(self, req, parent_ino, name):
        parent = await self._ino_to_node(parent_ino)
        _child = await parent.lookup(name.decode(self.encoding))
        child = as_node(_child)
        if child is not _child:
            print(f'Converted child {name} for {type(_child)} to {type(child)}')

        return await self._reply_entry(req, child)

    @_wrapper
    async def fuse_forget(self, req, ino, nlookup):
        node = await self._ino_to_node(ino)

        remaining_refs = await self._update_inodef_refs(node, -1)

        return await self._reply_none(req, 'Forgot' if remaining_refs == 0 else f'{remaining_refs} references remaining')

    @_wrapper
    async def fuse_getattr(self, req, ino, fi):
        node = await self._ino_to_node(ino)
        stat = as_stat(await node.getattr())

        return await self._reply_attr(req, stat, ino, node.attr_timeout)

    @_wrapper
    async def fuse_setattr(self, req, ino, attr, to_set, fi):
        node = await self._ino_to_node(ino)

        now = time.time()
        to_set = fusell.setattr_mask_to_list(to_set)
        attr = as_stat({
            k[:-4] if k.endswith('_now') else k: now if k.endswith('_now') else v
            for k, v in fusell.stat_to_dict(ctypes.pointer(attr)).items()
            if k in to_set
        })

        ## Try to send setattr to the FileHandle
        if fi is not None:
            try:
                async with self._handle_lock:
                    handle = self._file_fds[fi.fh]
                new_attr = as_stat(await handle.setattr(attr, to_set))
            except OSError as e:
                # Ignore not-implemented -- We will fallback to the same operation on the node
                if e.errno != errno.ENOSYS:
                    raise

        # send setattr to the Node
        new_attr = as_stat(await node.setattr(attr, to_set))
        return await self._reply_attr(req, new_attr, ino, node.attr_timeout)


    @_wrapper
    async def fuse_readlink(self, req, ino):
        node = await self._ino_to_node(ino)
        link = await node.readlink()

        return await self._reply_readlink(req, link)

    @_wrapper
    async def fuse_mknod(self, req, parent_ino, name, mode, rdev):
        parent = await self._ino_to_node(parent_ino)
        new_node = as_node(await parent.mknod(name.decode(self.encoding), mode, rdev))

        return await self._reply_entry(req, new_node)

    @_wrapper
    async def fuse_mkdir(self, req, parent_ino, name, mode):
        parent = await self._ino_to_node(parent_ino)
        new_dir = as_node(await parent.mkdir(name.decode(self.encoding), mode))

        return await self._reply_entry(req, new_dir)

    @_wrapper
    async def fuse_unlink(self, req, parent_ino, name):
        parent = await self._ino_to_node(parent_ino)
        await parent.unlink(name.decode(self.encoding))

        return await self._reply_err(req, 0)

    @_wrapper
    async def fuse_rmdir(self, req, parent_ino, name):
        parent = await self._ino_to_node(parent_ino)
        await parent.rmdir(name.decode(self.encoding))

        return await self._reply_err(req, 0)

    @_wrapper
    async def fuse_symlink(self, req, link, parent_ino, name):
        parent = await self._ino_to_node(parent_ino)
        new_symlink = await parent.symlink(name.decode(self.encoding), link.decode(self.encoding))

        return await self._reply_entry(req, new_symlink)

    @_wrapper
    async def fuse_rename(self, req, old_parent_ino, name, new_parent_ino, new_name):
        old_parent = await self._ino_to_node(old_parent_ino)
        new_parent = await self._ino_to_node(new_parent_ino)
        await old_parent.rename(name, new_parent, new_name)

        return await self._reply_err(req, 0)

    @_wrapper
    async def fuse_link(self, req, ino, new_parent_ino, new_name):
        node = await self._ino_to_node(ino)
        new_parent = await self._ino_to_node(new_parent_ino)
        new_link = as_node(await new_parent.link(ino, new_name))

        return await self._reply_entry(req, new_link)



    # FileHandle

    @_wrapper
    async def fuse_open(self, req, ino, fi):
        node = await self._ino_to_node(ino)
        handle = as_filehandle(node, await node.open(fi.flags))
        fi.direct_io = handle.direct_io
        fi.nonseekable = handle.nonseekable

        async with self._handle_lock:
            fd = self._next_fd
            fi.fh = fd
            self._file_fds[fd] = handle
            self._next_fd += 1

        self.libfuse.fuse_reply_open(req, ctypes.byref(fi))
        return fd, handle

    @_wrapper
    async def fuse_read(self, req, ino, size, off, fi):
        async with self._handle_lock:
            handle = self._file_fds[fi.fh]

        buf = await handle.read(size, off)

        self.libfuse.fuse_reply_buf(req, buf, len(buf))
        return f'{len(buf)} bytes'

    @_wrapper
    async def fuse_write(self, req, ino, buf, size, off, fi):
        async with self._handle_lock:
            handle = self._file_fds[fi.fh]

        n = await handle.write(buf, off)

        return await self._reply_write(req, n)

    @_wrapper
    async def fuse_flush(self, req, ino, fi):
        async with self._handle_lock:
            handle = self._file_fds[fi.fh]

        await handle.flush()

        return await self._reply_err(req, 0)

    @_wrapper
    async def fuse_release(self, req, ino, fi):
        async with self._handle_lock:
            handle = self._file_fds[fi.fh]
            del self._file_fds[fi.fh]

        await handle.release()

        return await self._reply_err(req, 0)


    @_wrapper
    async def fuse_fsync(self, req, ino, datasync, fi):
        async with self._handle_lock:
            handle = self._file_fds[fi.fh]

        await handle.fsync(datasync)

        return await self._reply_err(req, 0)



    # DirHandle

    @_wrapper
    async def fuse_opendir(self, req, ino, fi):
        node = await self._ino_to_node(ino)
        dirhandle = as_dirhandle(node, await node.opendir())

        async with self._handle_lock:
            dirfd = self._next_fd
            fi.fh = dirfd
            self._dir_fds[dirfd] = dirhandle
            self._next_fd += 1

        self.libfuse.fuse_reply_open(req, ctypes.byref(fi))
        return dirfd, dirhandle

    @_wrapper
    async def fuse_readdir(self, req, ino, size, off, fi):
        async with self._handle_lock:
            dirhandle = self._dir_fds[fi.fh]

        node = await self._ino_to_node(ino)
        node_stat = as_stat(await node.getattr())
        node_stat = node_stat.with_values(st_ino=ino)
        entries = [(b'.', node_stat.as_dict()), (b'..', node_stat.as_dict())]
        entry_names = []

        #TODO: Increase concurrency
        async for entry in dirhandle.readdir():
            if isinstance(entry, str):
                child_name = entry
                child = as_node(await node.lookup(child_name))
            else:
                child_name, child = entry

            child_ino = await self._node_to_ino(child)
            child_stat = as_stat(await child.getattr())
            child_stat = child_stat.with_values(st_ino=child_ino)

            entries.append((child_name.encode(self.encoding), child_stat.as_dict()))
            entry_names.append(child_name)

        self.reply_readdir(req, size, off, entries)
        return entry_names

    @_wrapper
    async def fuse_releasedir(self, req, ino, fi):
        async with self._handle_lock:
            handle = self._dir_fds[fi.fh]
            del self._dir_fds[fi.fh]

        await handle.releasedir()

        return await self._reply_err(req, 0)

    @_wrapper
    async def fuse_fsyncdir(self, req, ino, datasync, fi):
        async with self._handle_lock:
            dirhandle = self._dir_fds[fi.fh]

        await dirhandle.fsyncdir(datasync)

        return await self._reply_err(req, 0)









    #def decode_path(self, path: str) -> Path:
        #path_parts = [part for part in path.split('/') if part]
        #node = self.rootNode
        #route = [('', node)]
        #for part in path_parts:
            #node = as_node(node[part])
            #route.append((part, node))
        #return Path(route)

    #def decode_folder(self, path: str) -> Tuple[Path, str]:
        #path_parts = [part for part in path.split('/') if part]
        #if len(path_parts) == 0:
            #raise fuse.FuseOSError(errno.ENOSYS)

        #last_part = path_parts[-1]
        #path_parts = path_parts[:-1]

        #node = self.rootNode
        #route = [('', node)]
        #for part in path_parts:
            #node = as_node(node[part])
            #route.append((part, node))
        #return Path(route), last_part

    #def _get_file_handle(self, fi: fuse_file_info) -> FileHandle:
        #with self._handle_lock:
            #return self._file_handles[fi.fh]

    #def _add_file_handle(self, node: Node, filehandle: FileHandle, fi: fuse_file_info) -> None:
        #with self._handle_lock:
            #filehandle.node = node
            #fh = self._next_handle
            #self._next_handle += 1
            #self._file_handles[fh] = filehandle
            #fi.fh = fh
            #fi.direct_io = 1 if filehandle.direct_io else 0

    #def _release_file_handle(self, fi: fuse_file_info) -> None:
        #with self._handle_lock:
            #del self._file_handles[fi.fh]

    #def _get_dir_handle(self, fh: int) -> DirHandle:
        #with self._handle_lock:
            #return self._dir_handles[fh]

    #def _add_dir_handle(self, node: Node, dirhandle: DirHandle) -> int:
        #with self._handle_lock:
            #dirhandle.node = node
            #fh = self._next_handle
            #self._next_handle += 1
            #self._dir_handles[fh] = dirhandle
            #return fh

    #def _release_dir_handle(self, fh: int) -> None:
        #with self._handle_lock:
            #del self._dir_handles[fh]

    #def access(self, path: str, amode: int) -> int:
        #decoded_path = self.decode_path(path)
        #return decoded_path.tar_ino_to_node.access(decoded_path, amode)

    #def bmap(self, path: str, blocksize: int, idx: int) -> int:
        #decoded_path = self.decode_path(path)
        #return decoded_path.tar_ino_to_node.bmap(decoded_path, blocksize, idx)

    #def chmod(self, path: str, amode: int, fi: Optional[fuse_file_info] = None) -> None:
        #decoded_path = self.decode_path(path)

        #if fi is not None:
            #filehandle = self._get_file_handle(fi)
            #filehandle.chmod(decoded_path, amode)
        #else:
            #decoded_path.tar_ino_to_node.chmod(decoded_path, amode)

    #def chown(self, path: str, uid: int, gid: int, fi: Optional[fuse_file_info] = None) -> None:
        #decoded_path = self.decode_path(path)

        #if fi is not None:
            #filehandle = self._get_file_handle(fi)
            #filehandle.chown(decoded_path, uid, gid)
        #else:
            #decoded_path.tar_ino_to_node.chown(decoded_path, uid, gid)

    #def create(self, path: str, mode: int, fi: fuse_file_info) -> None:
        #decoded_path, name = self.decode_folder(path)
        #filehandle = decoded_path.tar_ino_to_node.create(decoded_path, name, mode)
        #self._add_file_handle(decoded_path.tar_ino_to_node, filehandle, fi)

    #def destroy(self, path: str) -> None:
        #if isinstance(self.rootNode, RootNode):
            #self.rootNode.destroy()

    #def flush(self, path: str, fi: fuse_file_info) -> None:
        #decoded_path = self.decode_path(path)
        #filehandle = self._get_file_handle(fi)
        #filehandle.flush(decoded_path)

    #def fsync(self, path: str, datasync: int, fi: fuse_file_info) -> None:
        #decoded_path = self.decode_path(path)
        #filehandle = self._get_file_handle(fi)
        #filehandle.fsync(decoded_path, datasync)

    #def fsyncdir(self, path: str, datasync: int, fh) -> None:
        #decoded_path = self.decode_path(path)
        #dirhandle = self._get_dir_handle(fh)
        #dirhandle.fsyncdir(decoded_path, datasync)

    #def getattr(self, path: str, fi: Optional[fuse_file_info] = None) -> dict:
        #decoded_path = self.decode_path(path)

        #if fi is not None:
            #filehandle = self._get_file_handle(fi)
            #stat = filehandle.getattr(decoded_path)
        #else:
            #stat = decoded_path.tar_ino_to_node.getattr(decoded_path)

        #return as_stat(stat).as_dict()

    #def getxattr(self, path: str, name: str, position=0) -> bytes:
        #decoded_path = self.decode_path(path)
        #return decoded_path.tar_ino_to_node.getxattr(decoded_path, name)

    #def init(self, path: str):
        #if isinstance(self.rootNode, RootNode):
            #self.rootNode.init()

    #def link(self, path: str, target: str) -> None:
        #decoded_path, name = self.decode_folder(path)
        #decoded_target = self.decode_path(target)
        #decoded_path.tar_ino_to_node.link(decoded_path, name, decoded_target)

    #def listxattr(self, path: str) -> Iterable[str]:
        #decoded_path = self.decode_path(path)
        #return decoded_path.tar_ino_to_node.listxattr(decoded_path)

    #def lock(self, path: str, fi: fuse_file_info, cmd: int, lock: Any) -> None:
        #decoded_path = self.decode_path(path)
        #filehandle = self._get_file_handle(fi)
        #filehandle.lock(decoded_path, cmd, lock)

    #def mkdir(self, path: str, mode: int) -> None:
        #decoded_path, name = self.decode_folder(path)
        #decoded_path.tar_ino_to_node.mkdir(decoded_path, name, mode)

    #def mknod(self, path: str, mode: int, dev: int) -> None:
        #decoded_path, name = self.decode_folder(path)
        #decoded_path.tar_ino_to_node.mknod(decoded_path, name, mode, dev)

    #def open(self, path: str, fi: fuse_file_info) -> None:
        #decoded_path = self.decode_path(path)
        #filehandle = decoded_path.tar_ino_to_node.open(decoded_path, fi.flags)
        #self._add_file_handle(decoded_path.tar_ino_to_node, filehandle, fi)

    #def opendir(self, path: str):
        #decoded_path = self.decode_path(path)
        #dirhandle = decoded_path.tar_ino_to_node.opendir(decoded_path)
        #dirhandle = as_dirhandle(decoded_path.tar_ino_to_node, dirhandle)
        #return self._add_dir_handle(decoded_path.tar_ino_to_node, dirhandle)

    #def read(self, path: str, size: int, offset: int, fi: fuse_file_info) -> bytes:
        #decoded_path = self.decode_path(path)
        #filehandle = self._get_file_handle(fi)
        #return filehandle.read(decoded_path, size, offset)

    #def readdir(self, path: str, fh: int) -> Iterable[Tuple[str, dict, int]]:
        #decoded_path = self.decode_path(path)
        #dirhandle = self._get_dir_handle(fh)
        #for entry in dirhandle.readdir(decoded_path):
            #if isinstance(entry, str):
                #yield entry, None, 0
            #else:
                #name, attrs = entry
                #yield name, as_stat(attrs).as_dict(), 0

    #def readlink(self, path: str) -> str:
        #decoded_path = self.decode_path(path)
        #return decoded_path.tar_ino_to_node.readlink(decoded_path)

    #def release(self, path: str, fi: fuse_file_info) -> None:
        #decoded_path = self.decode_path(path)
        #filehandle = self._get_file_handle(fi)
        #filehandle.release(decoded_path)
        #self._release_file_handle(fi)

    #def releasedir(self, path: str, fh: int):
        #decoded_path = self.decode_path(path)
        #dirhandle = self._get_dir_handle(fh)
        #dirhandle.releasedir(decoded_path)
        #self._release_dir_handle(fh)

    #def removexattr(self, path: str, name: str) -> None:
        #decoded_path = self.decode_path(path)
        #decoded_path.tar_ino_to_node.removexattr(decoded_path, name)

    #def rename(self, path: str, new_path: str) -> None:
        #decoded_path = self.decode_path(path)
        #decoded_new_path, new_name = self.decode_folder(new_path)
        #decoded_path.tar_ino_to_node.rename(decoded_path, decoded_new_path, new_name)

    #def rmdir(self, path: str) -> None:
        #decoded_path = self.decode_path(path)
        #decoded_path.tar_ino_to_node.rmdir(decoded_path)

    #def setxattr(self, path: str, name: str, value: bytes, options: int, position: int = 0) -> None:
        #decoded_path = self.decode_path(path)
        #decoded_path.tar_ino_to_node.setxattr(decoded_path, name, value, options)

    #def statfs(self, path: str) -> StatVFS:
        #if isinstance(self.rootNode, RootNode):
            #return self.rootNode.statfs()
        #else:
            #raise fuse.FuseOSError(errno.ENOSYS)

    #def symlink(self, path: str, target: str) -> None:
        #decoded_path, name = self.decode_folder(path)
        #decoded_path.tar_ino_to_node.symlink(decoded_path, name, target)

    #def truncate(self, path: str, length: int, fi: Optional[fuse_file_info] = None) -> None:
        #decoded_path = self.decode_path(path)

        #if fi is not None:
            #filehandle = self._get_file_handle(fi)
            #filehandle.truncate(decoded_path, length)
        #else:
            #decoded_path.tar_ino_to_node.truncate(decoded_path, length)

    #def unlink(self, path: str) -> None:
        #decoded_path = self.decode_path(path)
        #decoded_path.tar_ino_to_node.unlink(decoded_path)

    #def utimens(self, path: str, times: Tuple[float, float] = None, fi: Optional[fuse_file_info] = None) -> None:
        #decoded_path = self.decode_path(path)

        #if times is not None:
            #atime, mtime = times
        #else:
            #atime = time.time()
            #mtime = atime

        #if fi is not None:
            #filehandle = self._get_file_handle(fi)
            #filehandle.utimens(decoded_path, atime, mtime)
        #else:
            #decoded_path.tar_ino_to_node.utimens(decoded_path, atime, mtime)

    #def write(self, path: str, data: bytes, offset: int, fi: fuse_file_info) -> int:
        #decoded_path = self.decode_path(path)
        #filehandle = self._get_file_handle(fi)
        #return filehandle.write(decoded_path, data, offset)
