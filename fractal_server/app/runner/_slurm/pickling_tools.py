import io
import pickle


class RestrictedUnpickler(pickle.Unpickler):
    def __init__(self, accepted_module_name_pairs):
        self.accepted_module_name_pairs = accepted_module_name_pairs

    def find_class(self, module, name):

        # Only allow some custom classes
        if (module, name) in self.accepted_module_name_pairs:
            return getattr(module, name)

        # Forbid everything else.
        raise pickle.UnpicklingError("'%s.%s' is forbidden" % (module, name))


def restricted_pickle_loads(s, module_name_pairs):
    """Helper function analogous to pickle.loads()."""
    return RestrictedUnpickler(
        io.BytesIO(s), accepted_module_name_pairs=module_name_pairs
    ).load()
