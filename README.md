# fusetree
A better Fuse API for Python 3

## Motivation:
I don't like Fuse's "high-level" API:
1. Having to parse a file's path at every call doesn't make sense except in the context of a "Hello World".
2. Related to above, faving to dispatch calls to different files from the same callbacks requires custom "routing" logic, which is meh.
3. Threads are cool, but AsyncIO is would be way better ;)

Fuse's The "low-level" API is much more fun, but:
- It is, after all, too low-level for pratical purposes.
- It also requires custom "routing" logic to make the same callbacks reach the correct file handlers.
- You have to manually keep track of inodes and etc

## Fusetree

Fusetree tries to solve these by providing an object-oriented implementation based on asyncio.

- Each inode is an instance of `Node`. Inode numbers are tracked automatically and don't require any attention from the programmer.
  - There is basically 1:1 mapping from fuse methods to `Node` methods, you can override whatever makes sense.
  - There are reasonable base classes for files, directories and symlinks.
  - There are various implementations for several common node types: Constant blob, dict-backed directory, files that perform an HTTP download, etc.
  
- Automatic convertions that do the right thing: 
  - `str`/`bytes` automatically becomes a read-only file
  - `dict` become a folder
  - `iterable` becomes a non-seekable read-only file

# Examples

Please take a look at [examples](examples)

