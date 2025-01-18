from abc import ABCMeta
from dataclasses import dataclass, fields
from typing import Iterable, TypeVar, Generic, cast


T = TypeVar('T', int, float)
"""Type variable for the generic class :class:`.MinMaxSum`.
"""

@dataclass
class MinMaxSum(Generic[T]):
    """A helper class for tracking mininum, maximum, and sum of a collection of values.
    Type variable ``T`` specifies the type of the values.
    """
    min: T | None = None
    max: T | None = None
    sum: T = cast(T, 0)

    def add(self, other: T) -> None:
        """Add a value to the collection.
        """
        self.min = other if self.min is None else min(self.min, other)
        self.max = other if self.max is None else max(self.max, other)
        self.sum += other
        return

class CustomInitMeta(ABCMeta):
    """A helper metaclass intended for abstract base classes that will automatically
    call a method ``__post_init__()`` (if it exists) after ``__init__()``.
    """
    def __call__(cls, *args, **kwargs):
        instance = super().__call__(*args, **kwargs)
        # after __new__ and __init__:
        if (post_init := getattr(cls, '__post_init__', None)) is not None:
            post_init(instance)
        return instance

@dataclass
class OptionsBase:
    """A helper base class for defining options that can be parsed from strings.
    When defining each option as a ``dataclass`` field,
    use ``field(default=,metadata=)`` to specify a default value for this option
    and a mapping from strings to option values.
    """

    def provides(self, key: str) -> set[str] | None:
        """Check if the options object provides an option named ``key``.
        If yes, return the set of valid string values for this option;
        otherwise, return ``None``.
        """
        for field in fields(self):
            if key == field.name:
                return set(field.metadata.keys())
        return None

    def set_from_str(self, key: str, val: str) -> None:
        """Set the option named ``key`` to value represented by string ``val``.
        """
        for field in fields(self):
            if key == field.name:
                if val not in field.metadata.keys():
                    raise ValueError
                setattr(self, field.name, field.metadata[val])
                return
        raise ValueError

    def to_pstr(self) -> Iterable[str]:
        """Produce a sequence of lines for pretty-printing the object.
        """
        for field in fields(self):
            val = getattr(self, field.name)
            yield f'{field.name} {val}'
        return
