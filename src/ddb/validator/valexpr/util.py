"""This module is the go-to place for useful functions for manipulating expressions
that return atomic values (as opposed to collections).
"""
from typing import cast, Final, TypeAlias, Type, Sequence, Iterable, Iterator, Any

from ...primitives import ValType, CompiledValExpr
from .interface import ValExpr, ValidatorException
from . import leaf, func, binary, aggr

def cast_if_needed(e: ValExpr, desired_type: ValType) -> ValExpr:
    if e.valtype() == desired_type:
        return e
    elif e.valtype().can_cast_to(desired_type):
        return func.CAST((e, ), dict(AS=desired_type))
    else:
        raise ValidatorException(f'cannot CAST {e.to_str()} ({e.valtype().name}) to {desired_type.name}')

def conjunctive_parts(cond: ValExpr) -> Iterable[ValExpr]:
    """Decompose ``cond`` into a conjunction of parts.
    If ``cond`` isn't Boolean in the first place, ``cond`` itself will be returned.

    TODO: The current implementation is pretty simple.
    It stops at any node that is not an AND,
    and it doesn't do anything fancy like conversion to a conjunctive normal form.
    """
    if isinstance(cond, binary.AND):
        for c in cond.children():
            yield from conjunctive_parts(c)
    else:
        yield cond
    return

def make_conjunction(conds: list[ValExpr]) -> ValExpr | None:
    """Construct a conjunction of the given conditions.
    If the list is empty, return ``None``.
    """
    if len(conds) == 0:
        return None
    elif len(conds) == 1:
        return conds[0]
    else:
        return binary.AND(conds[0], cast(ValExpr, make_conjunction(conds[1:])))

def in_scope(e: ValExpr, table_aliases: list[str]) -> bool:
    """Check if all column references under this expression are for the given collection of table aliases.
    Any relative column references will be treated as NOT in scope.
    """
    if isinstance(e, leaf.NamedColumnRef):
        return e.table_alias in table_aliases
    elif isinstance(e, leaf.RelativeColumnRef):
        return False
    else:
        return all(in_scope(child, table_aliases) for child in e.children())

def find_column_refs(e: ValExpr) -> Iterator['leaf.NamedColumnRef|leaf.RelativeColumnRef']: # NOTE: quotes to avoid circular import
    """Enumerate all column references (named or relative) inside the given expression.
    """
    if isinstance(e, leaf.NamedColumnRef):
        yield e
    elif isinstance(e, leaf.RelativeColumnRef):
        yield e
    else:
        for child in e.children():
            yield from find_column_refs(child)
    return

def contains_aggrs(e: ValExpr) -> bool:
    """Check whether the given expression contains any aggregation.
    """
    if isinstance(e, aggr.AggrValExpr):
        return True
    else:
        for child in e.children():
            if contains_aggrs(child):
                return True
    return False
    
def find_aggrs(e: ValExpr) -> Iterator[aggr.AggrValExpr]:
    """Enumerate all aggregate expressions inside the given expression.
    """
    if isinstance(e, aggr.AggrValExpr):
        yield e
    else:
        for child in e.children():
            yield from find_aggrs(child)
    return

def find_non_aggrs(e: ValExpr) -> Iterator[ValExpr]:
    """Enumerate all maximal aggregate-free expressions inside the given expression.
    By maximal, we mean that there is no other aggregate-free expressions inside the given expression containing it.
    """
    if isinstance(e, aggr.AggrValExpr):
        yield from ()
    elif all(not contains_aggrs(c) for c in e.children()):
        yield e
    else:
        for c in e.children():
            yield from find_non_aggrs(c)
    return

reverse_comparison: Final[dict[Type[binary.CompareOpValExpr], Type[binary.CompareOpValExpr]]] = {
    binary.EQ: binary.EQ,
    binary.NE: binary.NE,
    binary.LT: binary.GT,
    binary.LE: binary.GE,
    binary.GE: binary.LE,
    binary.GT: binary.LT
}
"""A map that reverses that comparison operation.
"""

def is_column_comparing_to_literal(e: ValExpr)\
    -> tuple[
        'leaf.NamedColumnRef|leaf.RelativeColumnRef', # NOTE: quotes to avoid circular import
        Type[binary.CompareOpValExpr], ValExpr
    ] | None:
    """Check if the given expression is of the form "column compares with column-free expression":
    if no, return ``None``; otherwise, return the triple of column reference, comparison type, and the column-free expression,
    where the comparison type is flipped as needed according to this order.
    Both name and relative column references are considered.
    """
    if not isinstance(e, binary.CompareOpValExpr):
        return None
    for this, comp, that in ((e.left(), type(e), e.right()), (e.right(), reverse_comparison[type(e)], e.left())):
        if (isinstance(this, leaf.NamedColumnRef) or isinstance(this, leaf.RelativeColumnRef))\
        and next(find_column_refs(that), None) is None:
            return (this, comp, that)
    return None

def are_columns_joining(e: ValExpr)\
    -> tuple[
        'leaf.RelativeColumnRef', # NOTE: quotes to avoid circular import
        Type[binary.CompareOpValExpr],
        'leaf.RelativeColumnRef' # NOTE: quotes to avoid circular import
    ] | None:
    """Check if the given expression is of the form "column from one input compares with column from the other":
    if no, return ``None``; otherwise, return the triple of the column from the left input, comparison type,
    and the column from the right input, where the comparison type is flipped as needed according to this order.
    We only check relative column references here.
    """
    if not isinstance(e, binary.CompareOpValExpr):
        return None
    for this, comp, that in ((e.left(), type(e), e.right()), (e.right(), reverse_comparison[type(e)], e.left())):
        if isinstance(this, leaf.RelativeColumnRef) and this.input_index == 0\
        and isinstance(that, leaf.RelativeColumnRef) and that.input_index == 1:
            return (this, comp, that)
    return None

def push_down_conds(cond: ValExpr, table_aliases: list[str]) -> tuple[ValExpr | None, ValExpr | None]:
    """Try to push parts of the given condition down to a collection of tables given by ``table_aliases``.
    Return a condition that can be pushed down (or ``None`` if it's impossible),
    as well as a "remainder" condition such that the AND of the two conditions is equivalent to the given condition.

    TODO: The current implementation is pretty simple.
    It first decomposes ``cond`` into a conjunction of parts,
    and then it pushes down all parts that can be fully evaluated within the scope of ``table_aliases``.
    """
    pushed_down_parts = list()
    remaining_parts = list()
    for part in conjunctive_parts(cond):
        if in_scope(part, table_aliases):
            pushed_down_parts.append(part)
        else:
            remaining_parts.append(part)
    return \
        make_conjunction(pushed_down_parts), \
        cond if len(pushed_down_parts) == 0 else make_conjunction(remaining_parts)

def find_column_in_exprs(table_alias: str, column_name: str, exprs: list[ValExpr], exact: bool = True):
    """Find the named column in a list of expressions.
    Return the index of the first occurrence, or ``None`` if no match is found.
    If ``exact``, the expression has to be the given qualified column reference exactly;
    otherwise, the expression only needs to reference the given qualified column in its subtree.
    """
    for i, expr in enumerate(exprs):
        if isinstance(expr, leaf.NamedColumnRef) and \
            expr.table_alias == table_alias and expr.column_name == column_name:
            return i
        if not exact:
            if find_column_in_exprs(table_alias, column_name, list(expr.children()), exact=False) is not None:
                return i
    return None

def must_be_equivalent(e1: ValExpr, e2: ValExpr) -> bool:
    """Check if ``e1`` and ``e2`` must be equivalent.
    If it returns ``False``, it doesn't mean ``e1`` and ``e2`` are not equivalent;
    it just means that we are unable to infer their equivalence.
    TODO: Currently we ignore various properties of operations (e.g., addition is commutative)
    and additional knowledge about equivalence classes of columns and constants
    (e.g., we know ``R.A = S.B`` by an earlier filter predicate).
    """
    if not e1.is_op_equivalent(e2):
        return False
    if len(e1.children()) != len(e2.children()):
        return False
    if isinstance(e1, aggr.AggrValExpr) and isinstance(e2, aggr.AggrValExpr) and e1.is_distinct != e2.is_distinct:
        return False
    for c1, c2 in zip(e1.children(), e2.children()):
        if not must_be_equivalent(c1, c2):
            return False
    return True

OutputLineage: TypeAlias = list[set[tuple[str, str]]]
"""For an output table, the output lineage maps each of its column index to a set of
(table alias, column name) pairs, any of which would be a valid :class:`.NamedColumnRef` to this column.
"""

def find_column_in_lineage(table_alias: str, column_name: str, output_lineage: OutputLineage) -> int | None:
    """Return the index of the output column referenced by the given (table alias, column name) pair,
    or ``None`` if there is none.
    """
    for i, valid_references in enumerate(output_lineage):
        if (table_alias, column_name) in valid_references:
            return i
    return None

def relativize(e: ValExpr,
               output_lineages: list[OutputLineage],
               expr_lists: list[Sequence[ValExpr|None] | None] | None = None,
               unsafe_outside_aggr: list[list[bool]] | None = None) -> ValExpr | None:
    """Express ``e`` in terms of :class:`.RelativeColumnRef` against a list of input rows.
    Return the new expression, or ``None`` if ``e`` cannot be fully relativized.
    For each input, there is an output lineage object in ``output_lineages``
    specifying what named column references can be made for each column;
    there is also a list in ``expr_lists`` (if given) specifying the expression (possilby none) computing each column.
    ``unsafe_outside_aggr``, if not ``None``, will be consulted to determine whether
    it is unsafe to express an expression outside of aggregation function using the corresponding columns.
    TODO: This function currently detects if a subexpression of ``e`` is equivalent to one in ``expr_lists``,
    but it does not recognize computability through rewrite, e.g., ``C+2`` can be computed from ``C-1`` as ``(C-1)+3``.
    """
    if isinstance(e, leaf.NamedColumnRef):
        for input_index, output_lineage in enumerate(output_lineages):
            if (column_index := find_column_in_lineage(e.table_alias, e.column_name, output_lineage)) is not None:
                if unsafe_outside_aggr is None or not unsafe_outside_aggr[input_index][column_index]:
                    return leaf.RelativeColumnRef(input_index, column_index, e.valtype())
    if expr_lists is not None:
        for input_index, expr_list in enumerate(expr_lists):
            if expr_list is None: continue
            for column_index, expr in enumerate(expr_list):
                if expr is None: continue
                if must_be_equivalent(e, expr):
                    if unsafe_outside_aggr is None or not unsafe_outside_aggr[input_index][column_index]:
                        return leaf.RelativeColumnRef(input_index, column_index, e.valtype())
    if isinstance(e, leaf.NamedColumnRef):
        # if there is still no match at this point for a named column reference, there is no relativization:
        return None
    else:
        relativized_children = list()
        for child in e.children():
            if (relativized_child := relativize(
                    child, output_lineages, expr_lists,
                    None if isinstance(e, aggr.AggrValExpr) else unsafe_outside_aggr)) is None:
                return None
            relativized_children.append(relativized_child)
        return e.copy_with_new_children(tuple(relativized_children))

def is_computable_from(e: ValExpr, exprs: list[ValExpr]) -> bool:
    """Check if ``e`` can be computed from ``exprs``.
    This function assumes (and does not verify) that these expressions are aggregree-free.
    """
    return relativize(e, [[set()]*len(exprs)], [exprs], None) is not None

def to_code_str(expr: ValExpr, output_lineages: list[OutputLineage], row_vars: list[str]) -> str:
    """Convert ``expr`` to a Python expression for evaluation inside a :class:`.QPop`.
    The list of ``OutputLineage`` objects, one for each children,
    is useful in converting :class:`.NamedColumnRef` to an index into an input row.
    The ``row_vars`` list specifies how the code should refer to a row when converting a column reference
    (:class:`.NamedColumnRef` or :class:`.RelativeColumnRef`):
    a row from the ``QPop``'s input at index ``i`` will be referred to using variable named ``row_vars[i]``.
    """
    if isinstance(expr, leaf.NamedColumnRef):
        return expr._code_str((), output_lineages=output_lineages, row_vars=row_vars)
    elif isinstance(expr, leaf.RelativeColumnRef):
        return expr._code_str((), row_vars=row_vars)
    else:
        return expr._code_str(tuple(to_code_str(c, output_lineages, row_vars) for c in expr.children()))

def eval_literal(expr: ValExpr) -> Any:
    """Assuming that the expression doesn't contain any column reference,
    evaluate it at compile-time (independent of the database instance) and return its Python value.
    """
    return CompiledValExpr(to_code_str(expr, list(), list())).eval()
