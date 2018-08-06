import fuse
import logging
import traceback

class LoggingFuseOperations(fuse.Operations):
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
            #traceback.print_exc()
            ret = str(e)
            raise
        finally:
            self.log.debug('<- %s %s', op, '%d bytes' % len(ret) if isinstance(ret, bytes) else repr(ret))

def is_iterable(x):
    try:
        iter(x)
        return True
    except:
        return False


def is_async_iterable(x):
      return hasattr(x, '__anext__') or hasattr(x, '__aiter__')
