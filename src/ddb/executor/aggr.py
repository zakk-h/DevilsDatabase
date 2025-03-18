from typing import cast, Final, Iterable, Generator, Sequence
from dataclasses import dataclass
from functools import cached_property, partial

from ..profile import profile_generator
from ..storage import HeapFile
from ..validator import valexpr, ValExpr, OutputLineage
from ..primitives import CompiledValExpr
from ..metadata import TableMetadata, INTERNAL_ANON_COLUMN_NAME_FORMAT, INTERNAL_ANON_TABLE_NAME_FORMAT

from .interface import ExecutorException, QPop
from .util import ExtSortBuffer

from .mergesort import MergeSortPop

class AggrPop(QPop['AggrPop.CompiledProps']):
    """A physical operator for computing aggregate expression values over grouped input rows.
    This operator will output one row for each group, containing only the group-by values
    followed by the aggregate values.
    The input rows must have already been grouped such that all rows in the same group appear consecutively.
    For any aggregate that is not incrementally computable,
    this operator uses extra memory and temporary files as needed to sort all input values in the group.
    """

    @dataclass
    class CompiledProps(QPop.CompiledProps):
        groupby_execs: list[CompiledValExpr]
        """Executable for each GROUP BY expression.
        """
        aggr_input_execs: list[CompiledValExpr]
        """Executable for computing an input for each aggregate expression from an input row.
        """
        aggr_init_execs: list[CompiledValExpr]
        """Executable for computing the initial state for each aggregate expression.
        """
        aggr_add_execs: list[CompiledValExpr]
        """Executable for computing the updated state (upon receving an input) for each aggregate expression.
        """
        aggr_finalize_execs: list[CompiledValExpr]
        """Executable for computing the final result for each aggregate expression.
        """

        def pstr(self) -> Iterable[str]:
            yield from super().pstr()
            yield f'group by {len(self.groupby_execs)} expressions:'
            for column_name, exec in zip(self.output_metadata.column_names, self.groupby_execs):
                yield f'  {column_name}: {exec}'
            yield f'{len(self.aggr_add_execs)} aggregate expressions:'
            for column_name, exec in zip(self.output_metadata.column_names[len(self.groupby_execs):], self.aggr_add_execs):
                yield f'  {column_name}: {exec}'
            return

    def __init__(self, input: QPop[QPop.CompiledProps],
                 groupby_exprs: list[ValExpr],
                 aggr_exprs: list[valexpr.AggrValExpr],
                 column_names: Sequence[str | None] | None,
                 num_memory_blocks: int) -> None:
        """Construct a aggregation operator on top of the given ``input``.
        """
        super().__init__(input.context)
        self.input: Final = input
        self.output_table_name: Final[str] = INTERNAL_ANON_TABLE_NAME_FORMAT.format(pop=type(self).__name__, hex=hex(id(self)))
        self.groupby_exprs = groupby_exprs
        self.aggr_exprs = aggr_exprs
        self.output_column_names: Final[list[str]] = list()
        for i, (expr, column_name) in enumerate(zip(
            self.groupby_exprs + cast(list[ValExpr], self.aggr_exprs),
            [None] * (len(self.groupby_exprs) + len(self.aggr_exprs)) if column_names is None else column_names)):
            if column_name is not None:
                self.output_column_names.append(column_name)
            elif isinstance(expr, valexpr.leaf.NamedColumnRef):
                self.output_column_names.append(expr.column_name)
            else:
                self.output_column_names.append(INTERNAL_ANON_COLUMN_NAME_FORMAT.format(index = i)) # default
        self.num_memory_blocks: Final = num_memory_blocks
        self.num_non_incremental: Final = sum(not aggr.is_incremental() for aggr in self.aggr_exprs)
        if self.num_memory_blocks < 3 * self.num_non_incremental:
            raise ExecutorException('aggregation needs at least 3 memory blocks for merge sort')
        return

    def memory_blocks_required(self) -> int:
        return self.num_memory_blocks

    def children(self) -> tuple[QPop[QPop.CompiledProps], ...]:
        return (self.input, )

    def pstr_more(self) -> Iterable[str]:
        yield f'AS {self.output_table_name}:'
        for expr, name in zip(self.groupby_exprs + cast(list[ValExpr], self.aggr_exprs), self.output_column_names):
            yield f'  {name}: {expr.to_str()}'
        return

    @cached_property
    def compiled(self) -> 'AggrPop.CompiledProps':
        input_props = self.input.compiled
        output_column_types = [e.valtype() for e in self.groupby_exprs + self.aggr_exprs]
        output_lineage: OutputLineage = list()
        preserved_input_columns: dict[int, int] = dict()
        i: int | None
        for i, (expr, output_column_name) in enumerate(zip(
            self.groupby_exprs + cast(list[ValExpr], self.aggr_exprs),
            self.output_column_names)):
            output_column_lineage = set(((self.output_table_name, output_column_name), ))
            if (input_column_index := self.column_in_child(expr, 0)) is not None:
                output_column_lineage = output_column_lineage | input_props.output_lineage[input_column_index]
                preserved_input_columns[input_column_index] = i
            output_lineage.append(output_column_lineage)
        ordered_columns: list[int] = list()
        ordered_asc: list[bool] = list()
        for input_column_index, asc in zip(input_props.ordered_columns, input_props.ordered_asc):
            if (i := preserved_input_columns.get(input_column_index)) is not None:
                ordered_columns.append(i)
                ordered_asc.append(asc)
            else: # any "gap" means remaining columns won't be ordered
                break
        unique_columns: set[int] = set()
        for input_column_index in input_props.unique_columns:
            if (i := preserved_input_columns.get(input_column_index)) is not None:
                unique_columns = unique_columns | {i}
        # grouping will enforce uniqueness for the group-by columns as a whole,
        # but unfortunately we only capture single-column uniqueness:
        if len(self.groupby_exprs) == 1:
            unique_columns = unique_columns | {0}
        # compile!
        # GROUP BY expressions and inputs to aggregates are just compiled in the generic way:
        groupby_execs: list[CompiledValExpr] = [self.compile_valexpr(expr) for expr in self.groupby_exprs]
        aggr_input_execs: list[CompiledValExpr] = [self.compile_valexpr(aggr_expr.children()[0]) for aggr_expr in self.aggr_exprs]
        # aggregates themselves are compiled differently:
        aggr_init_execs: list[CompiledValExpr] = list()
        aggr_add_execs: list[CompiledValExpr] = list()
        aggr_finalize_execs: list[CompiledValExpr] = list()
        for e in self.aggr_exprs:
            aggr_init_execs.append(CompiledValExpr(e.code_str_init()))
            aggr_add_execs.append(CompiledValExpr(e.code_str_add('state', 'new_val')))
            aggr_finalize_execs.append(CompiledValExpr(e.code_str_finalize('state')))
        return AggrPop.CompiledProps(
            output_metadata = TableMetadata(self.output_column_names, output_column_types),
            output_lineage = output_lineage,
            ordered_columns = ordered_columns,
            ordered_asc = ordered_asc,
            unique_columns = unique_columns,
            groupby_execs = groupby_execs,
            aggr_input_execs = aggr_input_execs,
            aggr_init_execs = aggr_init_execs,
            aggr_add_execs = aggr_add_execs,
            aggr_finalize_execs = aggr_finalize_execs)

    @cached_property
    def estimated(self) -> QPop.EstimatedProps:
        stats = self.context.zm.grouping_stats(
            self.input.estimated.stats,
            [cast(ValExpr, valexpr.relativize(e, [self.input.compiled.output_lineage]))
             for e in self.groupby_exprs],
            [cast(valexpr.AggrValExpr, valexpr.relativize(a, [self.input.compiled.output_lineage]))
             for a in self.aggr_exprs])
        return QPop.EstimatedProps(
            stats = stats,
            blocks = QPop.StatsInBlocks(
                self_reads = 0,
                self_writes = 0,
                overall = self.input.estimated.blocks.overall))

    @profile_generator()
    def execute(self) -> Generator[tuple, None, None]:
        # THIS IS WHERE YOUR IMPLEMENTATION SHOULD GO
        # but feel free to define other helper methods in this class as you see fit
        orders_asc = [True] * len(self.aggr_exprs)
        sorter = MergeSortPop(input, self.aggr_exprs, orders_asc, self.num_memory_blocks)

        currgroup = None
        calculated = []
        for row in sorter.execute():
            grp = self.compiled.groupby_execs.eval(this=row, that=row)
            if currgroup is None:
                currgroup = grp

            if grp != currgroup:
                yield calculated
                calculated = self.compiled.aggr_init_execs

            calculated = self.compiled.aggr_add_execs.
            self.compiled.aggr_input_execs
            

            

        yield from ()
        return
