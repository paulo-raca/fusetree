import sys
sys.path.append("/media/Paulo/Workspace/spotify/fusetree")


import fusetree
import logging
from stat import S_IFDIR, S_IFLNK, S_IFREG
import time
import fuse


#def IterableGenerator(func: callable) -> callable:
    #def tmp(*args, **kwargs):
        #class Iterable:
            #def __init__(self, func, *args, **kwargs):
                #self.func = func
                #self.args = args
                #self.kwargs = kwargs

            #def __iter__(self):
                #return self.func(*self.args, **self.kwargs)

        #return Iterable(func, *args, **kwargs)
    #return tmp



@fusetree.generatorfile
def count(n=None):
    i = 1
    while n is None or i <= n:
        yield str(i) + '\n'
        i += 1

@fusetree.generatorfile
def bottles(n):
   for i in range(n, 0, -1):
      yield \
"""\
{0} {2} of beer on the wall
{0} {2} of beer
Take one down, pass it around
{1} {3} of beer on the wall

""".format(
                i, i - 1,
                "bottle" if i == 1 else "bottles",
                "bottle" if i - 1 == 1 else "bottles"
            )

rootNode = {
    'editable': fusetree.BlobFile(b"xyz", rw=True),
    'editabledir': fusetree.DictDir({}, rw=True),
    'Foo': b'meh',
    'Bar':'sldkjfn',
    'w': [1,2,3,4],
    'x':{},
    'count-100': count(100),
    'count-forever': count(),
    '99-bottles': bottles(999),
    'xkcd': fusetree.UrllibFile('https://xkcd.com'),
    'link': fusetree.Symlink('99-bottles'),
}
rootNode['z'] = rootNode

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    fusetree.FuseTree(rootNode, '/tmp/fusetree', foreground=True)
