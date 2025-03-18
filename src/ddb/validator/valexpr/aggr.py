from abc import abstractmethod
from ...primitives import ValType
from .interface import ValExpr, ValidatorException, ArithOpValExpr
from .func import FunCallValExpr

class AggrValExpr(FunCallValExpr):
    """An abstract class for a built-in aggregation function."""
    
    def __init__(self, args: tuple[ValExpr, ...], is_distinct: bool = False):
        """Constructor.
        ``is_distinct`` indicates the presence of DISTINCT within the aggregate function.
        """
        super().__init__(args)
        self.is_distinct = is_distinct
        return

    def is_incremental(self) -> bool:
        """Determine whether this aggregation function can be incrementally computed.
        By default, we assume DISTINCT implies that the aggregate is not incremental;
        otherwise, the aggregate is incremental.
        """
        return not self.is_distinct

    def to_str(self) -> str:
        return '{}({}{})'.format(
            type(self).name,
            'DISTINCT ' if self.is_distinct else '',
            ', '.join(c.to_str() for c in self.children()))

    def _code_str(self, children_code_str: tuple[str, ...]) -> str:
        """Disabled method because aggregates are special."""
        raise TypeError

    @abstractmethod
    def code_str_init(self) -> str:
        """Generate Python code to compute the initial aggregate state."""
        pass

    @abstractmethod
    def code_str_add(self, state: str, child_code_str: str) -> str:
        """Generate Python code for updating the aggregate state."""
        pass

    @abstractmethod
    def code_str_merge(self, state1: str, state2: str) -> str:
        """Generate Python code for merging two aggregate states."""
        pass

    @abstractmethod
    def code_str_finalize(self, state: str) -> str:
        """Generate Python code to compute the final aggregate value."""
        pass


class SUM(AggrValExpr, ArithOpValExpr):  # Fixed inheritance order
    name = 'SUM'
    arity_min = 1
    arity_max = 1

    def _validate_valtype(self) -> tuple[ValType, ...]:
        """SUM works on numeric types."""
        return (ValType.FLOAT,)

    def code_str_init(self) -> str:
        return "0"

    def code_str_add(self, state: str, child_code_str: str) -> str:
        return f"{state} + {child_code_str}"

    def code_str_merge(self, state1: str, state2: str) -> str:
        return f"{state1} + {state2}"

    def code_str_finalize(self, state: str) -> str:
        return state


class COUNT(AggrValExpr):
    name = 'COUNT'
    arity_min = 1
    arity_max = 1

    def _validate_valtype(self) -> tuple[ValType, ...]:
        """COUNT works on any type."""
        return (ValType.INTEGER,)

    def code_str_init(self) -> str:
        return "0"

    def code_str_add(self, state: str, child_code_str: str) -> str:
        return f"{state} + 1"

    def code_str_merge(self, state1: str, state2: str) -> str:
        return f"{state1} + {state2}"

    def code_str_finalize(self, state: str) -> str:
        return state


class AVG(AggrValExpr):
    name = 'AVG'
    arity_min = 1
    arity_max = 1

    def _validate_valtype(self) -> tuple[ValType, ...]:
        child_type = self.children()[0].valtype()
        if child_type not in (ValType.INTEGER, ValType.FLOAT):
            raise ValidatorException(f'operand of {type(self).__name__} is not numeric')
        return ValType.FLOAT, child_type

    def code_str_init(self) -> str:
        return '(0.0, 0)'

    def code_str_add(self, state: str, child_code_str: str) -> str:
        return f'({state}[0] + {child_code_str}, {state}[1] + 1)'

    def code_str_merge(self, state1: str, state2: str) -> str:
        return f'({state1}[0] + {state2}[0], {state1}[1] + {state2}[1])'

    def code_str_finalize(self, state: str) -> str:
        return f'None if {state}[1] == 0 else {state}[0] / float({state}[1])'


class STDDEV_POP(AggrValExpr):
    name = 'STDDEV_POP'
    arity_min = 1
    arity_max = 1

    def _validate_valtype(self) -> tuple[ValType, ...]:
        child_type = self.children()[0].valtype()
        if child_type not in (ValType.INTEGER, ValType.FLOAT):
            raise ValidatorException(f'operand of {type(self).__name__} is not numeric')
        return ValType.FLOAT, child_type

    def code_str_init(self) -> str:
        return "(0, 0, 0)"

    def code_str_add(self, state: str, child_code_str: str) -> str:
        return f"({state}[0] + {child_code_str}, {state}[1] + 1, {state}[2] + ({child_code_str})**2)"

    def code_str_merge(self, state1: str, state2: str) -> str:
        return f"({state1}[0] + {state2}[0], {state1}[1] + {state2}[1], {state1}[2] + {state2}[2])"

    def code_str_finalize(self, state: str) -> str:
        return f"None if {state}[1] == 0 else (({state}[2] - ({state}[0]**2 / {state}[1])) / {state}[1])**0.5"


class MIN(AggrValExpr):
    name = 'MIN'
    arity_min = 1
    arity_max = 1

    def _validate_valtype(self) -> tuple[ValType, ...]:
        return self.children()[0].valtype(),

    def code_str_init(self) -> str:
        return "float('inf')"

    def code_str_add(self, state: str, child_code_str: str) -> str:
        return f"min({state}, {child_code_str})"

    def code_str_merge(self, state1: str, state2: str) -> str:
        return f"min({state1}, {state2})"

    def code_str_finalize(self, state: str) -> str:
        return f"None if {state} == float('inf') else {state}"


class MAX(AggrValExpr):
    name = 'MAX'
    arity_min = 1
    arity_max = 1

    def _validate_valtype(self) -> tuple[ValType, ...]:
        return self.children()[0].valtype(),

    def code_str_init(self) -> str:
        return "float('-inf')"

    def code_str_add(self, state: str, child_code_str: str) -> str:
        return f"max({state}, {child_code_str})"

    def code_str_merge(self, state1: str, state2: str) -> str:
        return f"max({state1}, {state2})"

    def code_str_finalize(self, state: str) -> str:
        return f"None if {state} == float('-inf') else {state}"
