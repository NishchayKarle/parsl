from abc import abstractmethod
from functools import singledispatch
from typing import Any, List, Sequence

from parsl.utils import RepresentationMixin

# Traverse the configuration hierarchy, returning a JSON component
# for each one. Configuration components which implement
# RepresentationMixin will be in the right form for inspecting
# object attributes. Configuration components which are lists or tuples
# are traversed in sequence. Other types default to reporting no
# usage information.


@singledispatch
def get_parsl_usage(obj) -> List[Any]:
    return []


@get_parsl_usage.register
def get_parsl_usage_representation_mixin(obj: RepresentationMixin):
    t = type(obj)
    qualified_name = f"{t.__module__}.{t.__name__}"

    # me can contain anything that can be rendered as JSON
    me: List[Any] = []

    if isinstance(obj, UsageInformation):
        # report rich usage information for this component
        attrs = {'c': qualified_name}
        attrs.update(obj.get_usage_information())
        me = [attrs]
    else:
        # report the class name of this component
        me = [qualified_name]

    # unwrap typeguard-style unwrapping
    init: Any = type(obj).__init__
    if hasattr(init, "__wrapped__"):
        init = init.__wrapped__

    import inspect

    argspec = inspect.getfullargspec(init)
    for arg in argspec.args[1:]: # skip first arg, self
        me.extend(get_parsl_usage(getattr(obj, arg)))

    return me


@get_parsl_usage.register(list)
@get_parsl_usage.register(tuple)
def get_parsl_usage_sequence(obj: Sequence) -> List[Any]:
    return [get_parsl_usage(v) for v in obj]


class UsageInformation:
    @abstractmethod
    def get_usage_information(self) -> dict:
        pass
