from typing import Final, Any, TypeAlias
from enum import Enum, auto
from functools import cached_property
from sys import getsizeof
from datetime import datetime

# the following imports are needed for providing Python functions used in compiled expressions:
import re
from math import sqrt
from dateutil.parser import parse as str_to_datetime

class ValType(Enum):
    """Types supported by our database system.
    Conveniently, the names are also valid SQL types (except for ``ANY``).
    Note that the ordering reflects type precedence:
    when an operator combines expressions of different data types,
    the expression whose data type has lower precedence is first converted to one with the higher precedence
    (assuming an implicit cast is possible).
    """
    DATETIME = auto()
    FLOAT = auto()
    INTEGER = auto()
    BOOLEAN = auto()
    VARCHAR = auto()
    ANY = auto()

    def implicitly_casts_to(self, other: 'ValType') -> bool:
        """Check if a value of this type can be implicitly cast to a value of the ``other`` type.
        """
        if self == other:
            return True
        elif self == ValType.BOOLEAN and other in (ValType.INTEGER, ValType.FLOAT):
            return True
        elif self == ValType.INTEGER and other == ValType.FLOAT:
            return True
        elif self == ValType.VARCHAR and other == ValType.DATETIME:
            return True
        elif self == ValType.DATETIME and other == ValType.VARCHAR:
            return True
        elif other == ValType.ANY:
            return True
        else:
            return False

    def can_cast_to(self, other: 'ValType') -> bool:
        """Check if a value of this type can be explicitly cast to a value of the ``other`` type.
        """
        if self.implicitly_casts_to(other):
            return True
        elif self == ValType.ANY:
            return True
        elif other == ValType.VARCHAR:
            return True
        elif self == ValType.FLOAT and other == ValType.INTEGER:
            return True
        else:
            return False

    def cast_from(self, v: Any) -> Any:
        """Cast the given Python value into another Python value corresponding to this type.
        """
        match self:
            case ValType.DATETIME:
                return str_to_datetime(v)
            case ValType.FLOAT:
                return float(v)
            case ValType.INTEGER:
                return int(v)
            case ValType.BOOLEAN:
                return bool(v)
            case ValType.VARCHAR:
                return str(v)
            case ValType.ANY:
                return v

    @cached_property
    def size(self) -> int:
        """Size of an object of this type in bytes, in memory.
        In the case of variable-length types ``VARCHAR`` and ``ANY``, we can only return a random guess.
        NOTE: In DDB, the disk representation of the object may take a different number of bytes
        (often fewer because Python isn't very efficient with basic types).
        """
        return getsizeof(self.dummy_value)

    @cached_property
    def dummy_value(self) -> Any:
        """A dummy Python value for this type.
        """
        match self:
            case ValType.DATETIME:
                return datetime.now()
            case ValType.FLOAT:
                return float(0.142857)
            case ValType.INTEGER:
                return int(142857)
            case ValType.BOOLEAN:
                return False
            case ValType.VARCHAR:
                return '{:_^128}'.format('''DDB is Devil's DataBase, an instructional database system developed at Duke.''')
            case ValType.ANY:
                return '{:*^128}'.format('''DDB is Devil's DataBase, an instructional database system developed at Duke.''')

RowType: TypeAlias = list[ValType]
"""Type for a row, which is simply a list of ``ValType``s.
"""

def column_sizes(row_type: RowType) -> list[int]:
    """Return the sizes of columns according to their types (:meth:`.ValType.size`).
    """
    return [t.size for t in row_type]

def row_size(row_type: RowType) -> int:
    """Return the size the row according to the column types (:meth:`.ValType.size`).
    """
    return sum(column_sizes(row_type))

# the following function provides a Python function that can be used in compiled expressions:
def regexp_match(s: str, pattern: str):
    return re.match(pattern, s) is not None

class CompiledValExpr:
    """A compiled version of an expression tree that is more efficient for execution.
    During query execution, such expressions are often evaluated in the inner-most loops,
    so interpreted execution of parsed expressions is very expensive.
    Currently, this class is implemented as compiled Python expressions,
    which can additionally accept values for named parameters for each evaluation.
    """

    def __init__(self, code: str) -> None:
        """Construct a compiled expression from ``code``, which is a Python expression.
        ``code`` can refer to unbound variables, whose values must be set as named parameters when calling :meth:`.eval`.
        ``code`` can also make use of any functions imported or defined earlier in this module.
        """
        self._code: Final[str] = code
        self._exec: Final[Any] = compile(self._code, '<string>', 'eval')
        return

    def __str__(self) -> str:
        return self._code

    @classmethod
    def compare(cls, arg1: 'CompiledValExpr', op: str, arg2: 'CompiledValExpr') -> 'CompiledValExpr':
        """Construct a compiled expression comparing ``arg1`` and ``arg2`` using ``op``,
        which is one of the standard Python comparion operators represented as strings,
        e.g., ``'>='`` and ``'=='``.
        """
        return CompiledValExpr(f'{arg1._code} {op} {arg2._code}')

    @classmethod
    def logical(cls, op: str, *args: 'CompiledValExpr') -> 'CompiledValExpr':
        """Construct a compiled logical expression from ``args`` using ``op``,
        which is one of the standard Python logical operators represented as strings,
        i.e., ``'and'``, ``'or'``, and ``'not'``.
        """
        if len(args) > 1 and op in ('and', 'or'):
            return CompiledValExpr(f' {op} '.join(f'({arg._code})' for arg in args))
        elif len(args) == 1:
            if op == 'not':
                return CompiledValExpr(f'not({args[0]._code})')
            else:
                return args[0]
        else:
            raise TypeError

    @classmethod
    def conditional(cls, cond: 'CompiledValExpr', true_val: 'CompiledValExpr', false_val: 'CompiledValExpr') -> 'CompiledValExpr':
        """Construct a compiled conditional expression that returns ``true_value`` if ``cond`` evaluates to true,
        or ``false_value`` otherwise.
        """
        return CompiledValExpr(f'({true_val._code}) if ({cond._code}) else ({false_val._code})')

    @classmethod
    def tuple(cls, *args: 'CompiledValExpr', avoid_singleton: bool = False) -> 'CompiledValExpr':
        """Construct a compiled expression that returns a Python tuple whose components are values of the ``args`` when evaluated.
        If ``avoid_singleton`` and only one argument is supplied, simply return the argument as is, instead of a single-component tuple.
        """
        if len(args) == 1:
            if avoid_singleton:
                return args[0]
            else:
                return CompiledValExpr(f'({args[0]._code}, )')
        else:
            return CompiledValExpr('(' + ', '.join(f'({arg._code})' for arg in args) + ')')

    def eval(self, **kwarg) -> Any:
        """Evaluate this compiled expression, with variables therein bound to the named parameter values provided by ``kwarg``.
        """
        return eval(self._exec, None, kwarg)
