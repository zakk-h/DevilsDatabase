from typing import Final, Iterable, Generator
from dataclasses import dataclass
from functools import cached_property
from math import ceil
import logging

from ..profile import profile_generator
from ..storage import HeapFile
from ..validator import ValExpr
from ..primitives import CompiledValExpr

from .interface import QPop, ExecutorException
from .util import ExtSortBuffer

class MergeSortPop(QPop['MergeSortPop.CompiledProps']):
    """External merge sort physical operator.
    It will use as many memory blocks as it is given,
    with an option to use a different number of blocks for the final pass (as it may be used to optimize subsequent join).
    The intermediate runs will be stored as heap files in the tmp space.
    """

    @dataclass
    class CompiledProps(QPop.CompiledProps):
        cmp_exec: CompiledValExpr
        """Executable for comparing rows.
        """

        def pstr(self) -> Iterable[str]:
            yield from super().pstr()
            if self.cmp_exec is not None:
                yield f'row comparison code: {self.cmp_exec}'
            return

    def __init__(self, input: QPop[QPop.CompiledProps],
                 exprs: list[ValExpr],
                 orders_asc: list[bool],
                 num_memory_blocks: int, num_memory_blocks_final: int | None) -> None:
        """Construct a sort on top of the given ``input``, using the specified expressions and orders.
        The number of memory blocks for the final pass does NOT include any block used for buffering output.
        """
        super().__init__(input.context)
        self.input: Final = input
        self.exprs: Final = exprs
        self.orders_asc: Final = orders_asc
        self.num_memory_blocks: Final = num_memory_blocks
        if self.num_memory_blocks <= 2:
            raise ExecutorException('merge sort needs at least 3 memory blocks to perform a merge')
        self.num_memory_blocks_final: Final = num_memory_blocks_final or self.num_memory_blocks
        return

    def memory_blocks_required(self) -> int:
        return max(self.num_memory_blocks, self.num_memory_blocks_final)

    def children(self) -> tuple[QPop[QPop.CompiledProps], ...]:
        return (self.input, )

    def pstr_more(self) -> Iterable[str]:
        yield ', '.join(expr.to_str() + ' ' + ('ASC' if asc else 'DESC')
                        for expr, asc in zip(self.exprs, self.orders_asc))
        yield f'# memory blocks: {self.num_memory_blocks} ({self.num_memory_blocks_final} last pass)'
        return

    def _infer_ordering_props(self) -> tuple[list[int], list[bool]]:
        ordered_columns: list[int] = list()
        ordered_asc: list[bool] = list()
        for expr, asc in zip(self.exprs, self.orders_asc):
            if (input_column_index := self.column_in_child(expr, 0)) is not None:
                ordered_columns.append(input_column_index)
                ordered_asc.append(asc)
            else: # sorting by something that's not an output column
                break # this gap would destroy the rest of the ordering
        if len(ordered_columns) == len(self.exprs): # no gap
            # our sort is stable, so previous ordering is still there, just pushed later:
            for input_column_index, asc in zip(self.input.compiled.ordered_columns, self.input.compiled.ordered_asc):
                if input_column_index not in ordered_columns:
                    ordered_columns.append(input_column_index)
                    ordered_asc.append(asc)
        return ordered_columns, ordered_asc

    def compare(self, this: tuple, that: tuple) -> int:
        """Compare two rows ``this`` and ``that``,
        and return ``-1``, ``0``, or ``1`` if ``this`` is less than (i.e., goes before in ascending order),
        equal to, or greater than ``that``, respectively.
        """
        return self.compiled.cmp_exec.eval(this=this, that=that)

    def _compile_comparators(self) -> CompiledValExpr:
        # construct the comparator for sorting:
        this_before_that_codes: list[CompiledValExpr] = list()
        eq_codes: list[CompiledValExpr] = list()
        for expr, asc in zip(self.exprs, self.orders_asc):
            this_code = self.compile_valexpr(expr, ['this'])
            that_code = self.compile_valexpr(expr, ['that'])
            op = '<' if asc else '>'
            this_before_that_code = CompiledValExpr.compare(this_code, op, that_code)
            if len(eq_codes) > 0:
                this_before_that_code = CompiledValExpr.logical('and', *eq_codes, this_before_that_code)
            this_before_that_codes.append(this_before_that_code)
            eq_codes.append(CompiledValExpr.compare(this_code, '==', that_code))
        this_before_that_code = CompiledValExpr.logical('or', *this_before_that_codes)
        eq_code = CompiledValExpr.logical('and', *eq_codes)
        cmp_exec = CompiledValExpr.conditional(
            this_before_that_code,
            CompiledValExpr('-1'),
            CompiledValExpr.conditional(eq_code, CompiledValExpr('0'), CompiledValExpr('1')))
        return cmp_exec

    @cached_property
    def compiled(self) -> 'MergeSortPop.CompiledProps':
        input_props = self.input.compiled
        ordered_columns, ordered_asc = self._infer_ordering_props()
        cmp_exec = self._compile_comparators()
        return MergeSortPop.CompiledProps.from_input(input_props,
                                                     ordered_columns = ordered_columns,
                                                     ordered_asc = ordered_asc,
                                                     cmp_exec = cmp_exec)

    @cached_property
    def estimated(self) -> QPop.EstimatedProps:
        stats = self.context.zm.selection_stats(self.input.estimated.stats, None)
        num_passes = 1
        num_runs = ceil(stats.block_count() / self.num_memory_blocks)
        while num_runs > self.num_memory_blocks_final:
            num_passes += 1
            num_runs = ceil(num_runs / (self.num_memory_blocks - 1))
        block_self_reads = stats.block_count() * (num_passes-1)
        block_self_writes = stats.block_count() * (num_passes-1)
        return QPop.EstimatedProps(
            stats = stats,
            blocks = QPop.StatsInBlocks(
                self_reads = block_self_reads,
                self_writes = block_self_writes,
                overall = self.input.estimated.blocks.overall + block_self_reads + block_self_writes))

    def _tmp_file_create(self, level: int, run: int) -> HeapFile:
        """Create a temporary file for a result run in a given level with an ordinal run number.
        Levels start at ``0`` (results of initial sorting pass) and go up by one with each additional merge pass.
        Each level may contain multiple result runs, numbered from ``0``.
        The file name is chosen in a way to help deduce which ``Pop`` produced it and what level and run number it has.
        """
        f = self.context.sm.heap_file(self.context.tmp_tx, f'.tmp-{hex(id(self))}-{level}-{run}', [], create_if_not_exists=True)
        f.truncate()
        return f

    def _tmp_file_delete(self, run: HeapFile) -> None:
        """Delete a temporary file for a result run.
        """
        self.context.sm.delete_heap_file(self.context.tmp_tx, run.name)
        return

    @profile_generator()
    def execute(self) -> Generator[tuple, None, None]:
        buffer = ExtSortBuffer(self.compare,
                               self._tmp_file_create, self._tmp_file_delete,
                               self.num_memory_blocks, self.num_memory_blocks_final)
        logging.debug('***** pass 0: sort')
        for row in self.input.execute():
            buffer.add(row)
        yield from buffer.iter_and_clear()
        return
