# https://stackoverflow.com/questions/38433837/subprocess-child-traceback

from multiprocessing import Process, Pipe
import traceback
import functools

class MyProcess(Process):
    def __init__(self, *args, **kwargs):
        Process.__init__(self, *args, **kwargs)
        self._pconn, self._cconn = Pipe()
        self._exception = None

    def run(self):
        try:
            Process.run(self)
            self._cconn.send(None)
        except Exception as e:
            tb = traceback.format_exc()
            self._cconn.send((e, tb))
            # raise e  # You can still rise this exception if you need to

    @property
    def exception(self):
        if self._pconn.poll():
            self._exception = self._pconn.recv()
        return self._exception

get_pack_path=r"D:\get_isbn_ssid_pack\get_isbn_ssid_pack.py"

p = MyProcess(target=functools.partial(execfile, get_pack_path))
p.start()
p.join() #wait for sub-process to end

if p.exception:
    error, traceback = p.exception
    print 'you got', traceback