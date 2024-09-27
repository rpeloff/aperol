import reprlib
from typing import Any, TypeVar, Generic, Union


T = TypeVar("T")


PickleState = tuple[type[T], dict[str, Any] | None, dict[str, Any] | None]


# simple extension of functools.partial to classes
class partial_cls(Generic[T]):
    """Create new class with partial application of the given keyword arguments."""

    __slots__ = "obj_cls", "kwargs", "__dict__", "__weakref__"

    def __new__(cls, obj_cls: Union[type[T], "partial_cls"], /, **kwargs: Any) -> "partial_cls":
        if not isinstance(obj_cls, type) and not isinstance(obj_cls, partial_cls):
            raise TypeError("the first argument must be class")

        if isinstance(obj_cls, partial_cls):
            kwargs = {**obj_cls.kwargs, **kwargs}
            obj_cls = obj_cls.obj_cls

        self = super().__new__(cls)

        self.obj_cls = obj_cls
        self.kwargs = kwargs
        return self

    def __call__(self, *args, **kwargs: Any) -> T:
        kwargs = {**self.kwargs, **kwargs}
        return self.obj_cls(*args, **kwargs)

    @reprlib.recursive_repr()
    def __repr__(self) -> str:
        qualname = type(self).__qualname__
        args = [repr(self.obj_cls)]
        args.extend(f"{k}={v!r}" for (k, v) in self.kwargs.items())
        if type(self).__module__.endswith("clstools"):
            return f"{type(self).__module__}.{qualname}({', '.join(args)})"
        return f"{qualname}({', '.join(args)})"

    def __reduce__(self) -> tuple[type["partial_cls"], tuple[type[T]], PickleState]:
        return (
            type(self),
            (self.obj_cls,),
            (self.obj_cls, self.kwargs or None, self.__dict__ or None),
        )

    def __setstate__(self, state: PickleState) -> None:
        if not isinstance(state, tuple):
            raise TypeError("argument to __setstate__ must be a tuple")
        if len(state) != 3:
            raise TypeError(f"expected 3 items in state, got {len(state)}")

        obj_cls, kwargs, namespace = state
        if (
            (not isinstance(obj_cls, type) and not isinstance(obj_cls, partial_cls))
            or (kwargs is not None and not isinstance(kwargs, dict))
            or (namespace is not None and not isinstance(namespace, dict))
        ):
            raise TypeError("invalid partial_cls state")

        if kwargs is None:
            kwargs = {}
        elif type(kwargs) is not dict:
            kwargs = dict(kwargs)
        if namespace is None:
            namespace = {}

        self.__dict__ = namespace
        self.obj_cls = obj_cls
        self.kwargs = kwargs
