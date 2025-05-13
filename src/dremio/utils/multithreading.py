from threading import Thread


class ThreadWithReturnValue(Thread):
    def __init__(
        self, group=None, target=None, name=None, args=(), kwargs={}, Verbose=None
    ):
        Thread.__init__(self, group, target, name, args, kwargs)
        self._return = None

    def run(self):
        if self._target is not None:  # type: ignore
            self._return = self._target(*self._args, **self._kwargs)  # type: ignore

    def join(self, *args):
        Thread.join(self, *args)
        return self._return
