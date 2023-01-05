class ExceptionProxy:
    """
    Proxy class to serialise exceptions

    In general exceptions are not serialisable. This proxy class saves the
    serialisable content of an exception. On the receiving end, it can be used
    to reconstruct a TaskExecutionError.
    """

    def __init__(self, exc_type, tb, *args, **kwargs):
        self.exc_type_name = exc_type.__name__
        self.tb = tb
        self.args = args
        self.kwargs = kwargs
