from typing import cast, Final, Iterable, Generator, Any
from dataclasses import dataclass
from functools import cached_property
from contextlib import closing
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
        left_buckets, mod = self.recursive_partition(self.left.execute(), depth=0)
        logging.debug(f"ZWH: Returned, starting join with {mod} buckets")
        right_reader = BufferedReader(self.num_memory_blocks-1)
        #left_reader = BufferedReader(1)
        for right_buffer in right_reader.iter_buffer(self.right.execute()):
            for right_row in right_buffer:
                right_key = self.compiled.right_join_vals_exec.eval(this=right_row, that=right_row)
                hashed = self.hash(right_key) % mod
                to_load = left_buckets.get(hashed)
                if to_load:
                    if isinstance(to_load, list): # in memory list
                        for left_row in to_load:
                            if self.compiled.eq_exec.eval(this=left_row, that=right_row):
                                yield (*left_row, *right_row)
                    else: # heapfile
                        with to_load as left_file:
                            left_reader = BufferedReader(1)
                            for left_buffer in left_reader.iter_buffer(left_file.iter_scan()):
                                for left_row in left_buffer:
                                    if self.compiled.eq_exec.eval(this=left_row, that=right_row):
                                        # logging.debug(f"ZWH: Yielding row in join with {mod} buckets")
                                        yield (*left_row, *right_row)

    def recursive_partition(self, partition, depth):
        logging.debug(f"ZWH: recursive_partition initiated with depth {depth}")
        if depth == 0:
            mod = self.num_memory_blocks
            buckets = {x: self._tmp_partition_file("left", depth, x) for x in range(mod)}
            writers = {x: BufferedWriter(buckets[x], 1) for x in range(mod)}
            has_flushed = False

            reader = BufferedReader(1)
            for buffer in reader.iter_buffer(partition):
                for row in buffer:
                    key = self.compiled.left_join_vals_exec.eval(this=row, that=row)
                    bucket_idx = self.hash(key) % mod
                    writers[bucket_idx].write(row)

            logging.debug(f"ZWH: Depth 0 - Writers flush counts: {[writer.num_blocks_flushed for writer in writers.values()]}")
            for writer in writers.values():
                if writer.num_blocks_flushed > 0:
                    has_flushed = True
                writer.flush()
                writer.file._close()
                logging.debug("ZWH: Manual flush and close")

            logging.debug(f"ZWH: In depth=0, has_flushed is {has_flushed}")

            if has_flushed and depth + 1 <= DEFAULT_HASH_MAX_DEPTH:
                logging.debug("ZWH: Calling recursive partition from depth=0 to depth=1")
                return self.recursive_partition(buckets, depth + 1)
            else:
                logging.debug("ZWH: No more hashing needs to be done, depth=0 is final")
                return buckets, mod

        else:  # depth > 0
            new_buckets = {}
            mod = self.num_memory_blocks * ((self.num_memory_blocks - 1) ** depth)
            logging.debug(f"ZWH: Depth {depth} - Using mod {mod}")
            has_flushed = False

            for d in partition.values():
                if isinstance(d, HeapFile):
                    d._open()
                    scan = d.iter_scan()
                else:
                    scan = d

                logging.debug(f"ZWH: Depth {depth} - Processing partition element")
                reader = BufferedReader(1)
                open_writers = {}
                for buffer in reader.iter_buffer(scan):
                    for row in buffer:
                        key = self.compiled.left_join_vals_exec.eval(this=row, that=row)
                        new_spot = self.hash(key) % mod
                        if new_spot not in new_buckets:
                            new_buckets[new_spot] = self._tmp_partition_file("left", depth, new_spot)
                        if new_spot not in open_writers:
                            open_writers[new_spot] = BufferedWriter(new_buckets[new_spot], 1)
                        open_writers[new_spot].write(row)
                if isinstance(d, HeapFile):
                    d._close()

                logging.debug(f"ZWH: Depth {depth} - Finished processing element with sub-buckets {list(open_writers.keys())}")
                for key, writer in open_writers.items():
                    if writer.num_blocks_flushed > 0:
                        has_flushed = True
                        writer.flush()
                        writer.file._close()
                    else:
                        new_buckets[key] = writer.buffer
                        logging.debug(f"ZWH: Type of writer.buffer: {type(writer.buffer)}")

            if has_flushed and depth + 1 <= DEFAULT_HASH_MAX_DEPTH:
                logging.debug(f"ZWH: Depth {depth} - Some buckets flushed; recursing deeper to depth {depth + 1}")
                return self.recursive_partition(new_buckets, depth + 1)
            else:
                logging.debug(f"ZWH: Depth {depth} - No flushes in new buckets; returning final buckets at depth {depth}")
                return new_buckets, mod
