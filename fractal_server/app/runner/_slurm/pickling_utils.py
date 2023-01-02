# Examples taken from
# https://docs.python.org/3/library/pickle.html#restricting-globals
import io
import pickle

from ..common import TaskParameters
from .remote import ExceptionProxy


class RestrictedUnpickler(pickle.Unpickler):
    def find_class(self, module, name):
        # Only allow some custom classes
        if (module == "common" and name == "TaskParameters") or (
            module == "remote" and name == "ExceptionProxy"
        ):
            return getattr(module, name)
        # Forbid everything else.
        raise pickle.UnpicklingError("'%s.%s' is forbidden" % (module, name))


def restricted_pickle_loads(s):
    """Helper function analogous to pickle.loads()."""
    return RestrictedUnpickler(io.BytesIO(s)).load()
