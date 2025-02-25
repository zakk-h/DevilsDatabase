from typing import cast, Final, Iterable, Generator, Any
from dataclasses import dataclass
from functools import cached_property
import logging
import ctypes
import hashlib
from sys import getsizeof
from math import log, floor

from ...profile import profile_generator
from ...validator import ValExpr, valexpr
from ...primitives import CompiledValExpr
from ...storage import HeapFile
from ...globals import BLOCK_SIZE, DEFAULT_HASH_MAX_DEPTH

from ..util import BufferedWriter, BufferedReader
from ..interface import QPop

from .interface import JoinPop

class HashEqJoinPop(JoinPop['HashEqJoinPop.CompiledProps']):
    """Hash join physical operator.
    The left input will be used as the build table and the right as the probe table.
    It will use as many memory blocks as it is given.
    """

    @dataclass
    class CompiledProps(QPop.CompiledProps):
        eq_exec: CompiledValExpr
        """Executable for the join condition.
        """
        left_join_vals_exec: CompiledValExpr
        """Executable for extracting the join expression values from the left input.
        """
        right_join_vals_exec: CompiledValExpr
        """Executable for extracting the join expression values from the right input.
        """

        def pstr(self) -> Iterable[str]:
            yield from super().pstr()
            yield f'join condition code: {self.eq_exec}'
            return

    def __init__(self, left: QPop[QPop.CompiledProps], right: QPop[QPop.CompiledProps],
                 left_exprs: list[ValExpr],
                 right_exprs: list[ValExpr],
                 num_memory_blocks: int) -> None:
        """Construct a hash join between ``left`` and ``right`` inputs on the specified expressions
        (most commonly columns): ``left_exprs`` and ``right_exprs`` are to be evaluated over each row
        from left input and each row from right input, respectively.
        """
        super().__init__(left, right)
        self.left_exprs: Final = left_exprs
        self.right_exprs: Final = right_exprs
        self.num_memory_blocks: Final = num_memory_blocks
        return

    def memory_blocks_required(self) -> int:
        return self.num_memory_blocks

    def pstr_more(self) -> Iterable[str]:
        for left_expr, right_expr in zip(self.left_exprs, self.right_exprs):
            yield f'{left_expr.to_str()} = {right_expr.to_str()}'
        yield f'# memory blocks: {self.num_memory_blocks}'
        return

    @cached_property
    def compiled(self) -> 'HashEqJoinPop.CompiledProps':
        # ordered columns: 
        # if there is at least one pair of joined columns are both unique, all the uniqueness stay the same;
        # otherwise, nothing is guaranteed to remain unique.
        left_props = self.left.compiled
        right_props = self.right.compiled
        unique_columns: set[int] = set()
        col_i_offset = len(left_props.output_metadata.column_names)
        both_unique_flag = False
        for left_expr, right_expr in zip(self.left_exprs, self.right_exprs):
            if self.column_in_child(left_expr, 0) in left_props.unique_columns and \
            self.column_in_child(right_expr, 1) in right_props.unique_columns:
                # we got one:
                both_unique_flag = True
                break
        if both_unique_flag:
            unique_columns |= left_props.unique_columns
            unique_columns |= set(col_i_offset + col_i for col_i in right_props.unique_columns)
        # eq_code is a sequence of equality comparisons connected by 'and':
        eq_codes: list[CompiledValExpr] = list()
        left_signature_codes: list[CompiledValExpr] = list()
        right_signature_codes: list[CompiledValExpr] = list()
        for left_expr, right_expr in zip(self.left_exprs, self.right_exprs):
            this_code = self.compile_valexpr(left_expr, ['this', 'that'])
            that_code = self.compile_valexpr(right_expr, ['this', 'that'])
            eq_codes.append(CompiledValExpr.compare(this_code, '==', that_code))
            left_signature_codes.append(this_code)
            right_signature_codes.append(that_code)
        eq_exec = CompiledValExpr.logical('and', *eq_codes)
        # left/right joined tuple represents the joined columns/expressions that are wrapped up by a tuple
        # (only if there are multiple components)
        left_join_vals_exec = CompiledValExpr.tuple(*left_signature_codes, avoid_singleton=True)
        right_join_vals_exec = CompiledValExpr.tuple(*right_signature_codes, avoid_singleton=True)
        return HashEqJoinPop.CompiledProps.from_inputs(
            self.left.compiled, self.right.compiled,
            unique_columns = unique_columns,
            eq_exec = eq_exec,
            left_join_vals_exec = left_join_vals_exec,
            right_join_vals_exec = right_join_vals_exec)

    @cached_property
    def estimated(self) -> QPop.EstimatedProps:
        relativized_equalities = [
            cast(ValExpr, valexpr.relativize(
                valexpr.binary.EQ(e1, e2),
                [self.left.compiled.output_lineage, self.right.compiled.output_lineage]))
            for e1, e2 in zip(self.left_exprs, self.right_exprs)
        ]
        stats = self.context.zm.join_stats(
            self.left.estimated.stats,
            self.right.estimated.stats,
            valexpr.make_conjunction(relativized_equalities))
        # make some guess about how many reads/writes are needed:
        left_blocks = self.left.estimated.stats.block_count()
        right_blocks = self.right.estimated.stats.block_count()
        # number of partitoning passes can be as few as zero:
        estimated_passes = floor(log(left_blocks, self.num_memory_blocks - 1))
        reads = (left_blocks + right_blocks) * estimated_passes
        writes = (left_blocks + right_blocks) * estimated_passes
        return QPop.EstimatedProps(
            stats = stats,
            blocks = QPop.StatsInBlocks(
                self_reads = reads,
                self_writes = writes,
                overall = self.left.estimated.blocks.overall + self.right.estimated.blocks.overall +\
                    reads + writes))

    def _tmp_partition_file(self, side: str, depth: int, partition_id: int) -> HeapFile:
        """Create a temporary file for a partition in a given side (left/right), an ordinal depth, a partition id
        """
        f = self.context.sm.heap_file(self.context.tmp_tx, f'.tmp-{hex(id(self))}-{side}-{depth}-{partition_id}', [], create_if_not_exists=True)
        f.truncate()
        return f

    @staticmethod
    def hash(v: Any) -> int:
        # Python's hash() may give a negative integer, and it's just identity for integers, so let's scramble it more:
        x = ctypes.c_uint32(hash(v)).value
        x = ((x >> 16) ^ x) * 0x45d9f3b
        x = ((x >> 16) ^ x) * 0x45d9f3b
        x = (x >> 16) ^ x
        return x
        # a more heavy-weight alternative:
        # return int.from_bytes(hashlib.sha256(str(v).encode('utf-8')).digest(), 'big')

    @profile_generator()
    def execute(self) -> Generator[tuple, None, None]:
        # THIS IS WHERE YOUR IMPLEMENTATION SHOULD GO
        # but feel free to define other helper methods in this class as you see fit
        M = self.num_memory_blocks
        N = M-1
        
        
       
        yield from ()
        return
    
    def execute_recurse(num_memory_blocks, bucket, MAX_HASH_MAX_DEPTH):
        # one memory block to buffer writes to each output partition file
        writer = BufferedWriter(num_memory_blocks)
        # one to buffer reads from the input partition file
        reader = BufferedReader(num_memory_blocks)

        
