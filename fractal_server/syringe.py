# Copyright 2022 (C) eXact lab S.r.l.
#
# Original author:
# Jacopo Nespolo <jacopo.nespolo@exact-lab.it>
#
# This file is part of `Syringe`, a simple Python module for dependency
# injection. Redistribution and use must comply with the 3-clause BSD License
# (see `LICENSE` file).
"""
This module provides an extremely simple utility for dependency injection.

It's made up of a single singleton class that provides a directory for the
dependencies. The dependencies are stored in a dictionary and can be overridden
or popped from the directory.

## Usage:

    >>> from syringe import Inject
    >>> def foo():
    >>>     return 42
    >>>
    >>> def oof():
    >>>     return 24
    >>>
    >>> def bar():
    >>>     return Inject(foo)
    >>>
    >>> bar()
    42
    >>> Inject.override(foo, oof)
    >>> bar()
    24
    >>> Inject.pop(foo)
    >>> bar()
    42
"""
from typing import Any
from typing import Callable
from typing import Dict
from typing import TypeVar


T = TypeVar("T")
_instance_count = 0


class _Inject:
    """
    Injection class

    This is a private class that is never directly instantiated.

    Attributes:
        _dependencies:
            The dependency directory
    """

    _dependencies: Dict[Any, Any] = {}

    def __init__(self):
        global _instance_count
        if _instance_count == 1:
            raise RuntimeError("You must only instance this class once")
        _instance_count += 1

    @classmethod
    def __call__(cls, _callable: Callable[..., T]) -> T:
        """
        Call the dependency

        Args:
            _callable:
                Callable dependency object

        Returns:
            The output of calling `_callalbe` or its dependency override.
        """
        try:
            return cls._dependencies[_callable]()
        except KeyError:
            return _callable()

    @classmethod
    def pop(cls, _callable: Callable[..., T]) -> T:
        """
        Remove the dependency from the directory

        Args:
            _callable:
                Callable dependency object
        """
        try:
            return cls._dependencies.pop(_callable)
        except KeyError:
            raise RuntimeError(f"No dependency override for {_callable}")

    @classmethod
    def override(
        cls, _callable: Callable[..., T], value: Callable[..., T]
    ) -> None:
        """
        Override dependency

        Substitute a dependency with a different arbitrary callable.

        Args:
            _callable:
                Callable dependency object
            value:
                Callable override
        """
        cls._dependencies[_callable] = value


# NOTE: This is a singleton instance
Inject = _Inject()
"""
The singleton instance of `_Inject`, the only public member of this module.
"""
