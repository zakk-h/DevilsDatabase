from typing import cast, Final, Iterable, Generator, Sequence
from dataclasses import dataclass
from functools import cached_property, partial
import logging
logging.basicConfig(level=logging.DEBUG)

from ..profile import profile_generator
from ..storage import HeapFile
from ..validator import valexpr, ValExpr, OutputLineage
from ..primitives import CompiledValExpr
from ..metadata import TableMetadata, INTERNAL_ANON_COLUMN_NAME_FORMAT, INTERNAL_ANON_TABLE_NAME_FORMAT

from .interface import ExecutorException, QPop
from .util import ExtSortBuffer, BufferedWriter, BufferedReader

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
    
    def _tmp_file(self, name: str) -> HeapFile:
        f = self.context.sm.heap_file(self.context.tmp_tx, f'.tmp-{name}', [], create_if_not_exists=True)
        f.truncate()
        return f

    @profile_generator()
    def execute(self) -> Generator[tuple, None, None]:
        logging.debug("num_memory_blocks: %s, message: %s", self.num_memory_blocks, "zwh")

        needed = False
        for i in range(len(self.aggr_exprs)):
            if not self.aggr_exprs[i].is_incremental(): 
                needed = True # we have some memory, we can partition groups into files

        if needed:
            grouped_files = []
            currWriter = None
            currgroup = None
            inpreader = BufferedReader(self.memory_blocks_required() // 2) # half to read, half to write

            for buffer in inpreader.iter_buffer(self.input.execute()):
                for row in buffer:
                    grp = tuple(group_exec.eval(row0=row) for group_exec in self.compiled.groupby_execs) # this can be a tuple, multiple groupby things, so ties can be handled

                    if currWriter is None or currgroup is None or currgroup != grp: # None is for first iteration, if we have a group transition also
                        if currWriter is not None: # group transition, close
                            currWriter.flush()
                            currWriter.file._close()
                        # either way, open file and buffer writer
                        fle = self._tmp_file("-".join(map(str, grp)))
                        grouped_files.append((grp, fle)) # store actual group to yield at end
                        currWriter = BufferedWriter(fle, self.memory_blocks_required() // 2)
                        currgroup = grp

                    currWriter.write(row)
            if currWriter is not None: # need to flush for the last group because no transition
                currWriter.flush()
                currWriter.file._close()

        finalNeeded = {}

        if not needed:
            currgroup = None
            # now we process the rows on the outside            
            for row in self.input.execute():
                grp = tuple(group_exec.eval(row0=row) for group_exec in self.compiled.groupby_execs)
                grp_key = "-".join(map(str, grp)) # creates a string key for the dictionary based on the group values
                # ("Engineering", "New York") -> Engineering-New York
                # this conversion could cause problems if the group values themselves contained hyphens,
                # which is why we're now storing the original tuple alongside the states - 
                # we use the string only as a lookup key, but preserve the original values for the final output.
                # using it as a lookup is fine because it is injective
                '''
                The previous approach was problematic for the following reason.
                grp_key = "-".join(map(str, grp))  # E.g., "42-Engineering-2023-01-15"
                grp_tuple = tuple(grp_key.split("-"))  # Now becomes ("42", "Engineering", "2023-01-15")
                
                To use the strings as a lookup, we need the map to be injective, so no two tuples correspond to the same string.
                Tuple (1, 2, 3) → "1-2-3"
                Tuple ("1", "2-3") → "1-2-3"
                This is not guaranteed, see above, so this is unsafe.
                '''
                
                # new group we haven't seen before
                if grp_key not in finalNeeded:
                    # initialize state for all aggregates for this group
                    #finalNeeded[grp_key] = [exec.eval() for exec in self.compiled.aggr_init_execs]
                    finalNeeded[grp_key] = (grp, [exec.eval() for exec in self.compiled.aggr_init_execs])
                
                # process each aggregate for this row
                for i in range(len(self.aggr_exprs)):
                    curr = self.compiled.aggr_input_execs[i].eval(row0=row)
                    finalNeeded[grp_key][1][i] = self.compiled.aggr_add_execs[i].eval(
                        state=finalNeeded[grp_key][1][i], # finalNeeded[grp_key][i], # now we have this 2 piece of information store 
                        new_val=curr
                    )

        else:  # if we had one or more non-incremental expressions, we have this loop to go over the files for all of them. some may be incremental, some not
            # first pass through the grouped files to initialize all group states
            for grp, _ in grouped_files:
                grp_key = "-".join(map(str, grp))
                if grp_key not in finalNeeded:
                    #finalNeeded[grp_key] = [exec.eval() for exec in self.compiled.aggr_init_execs]
                    finalNeeded[grp_key] = (grp, [exec.eval() for exec in self.compiled.aggr_init_execs])
            
            # now process each aggregate expression -> aggregation expressions -> rows in groups
            # we can't do row loop and then aggregation expression inside or not because the way we sort (if applicable) depends on aggregation expression and column details
            # different aggregate expressions might require different sorting orders
            # the external sorting process is dependent on the specific aggregate expression being evaluated
            for grp, tmp_file in grouped_files:
                grp_key = "-".join(map(str, grp))
                
                # process each aggregate expression for this group's file, sorting if applicable
                for i in range(len(self.aggr_exprs)):
                    tmp_file._open('r')
                    
                    if not self.aggr_exprs[i].is_incremental(): # only do the sorting if we need to (i.e. duplicates need to be removed and that is what we need to do to remove them)
                        def compare_rows(row1, row2):
                            val1 = self.compiled.aggr_input_execs[i].eval(row0=row1)
                            val2 = self.compiled.aggr_input_execs[i].eval(row0=row2)
                            return -1 if val1 < val2 else (1 if val1 > val2 else 0)
                        
                        # some may be incremental and in this else loop of "needed" because we file-grouped every group in this case if 1 aggregate was non-incremental)
                        sort_buffer = ExtSortBuffer(
                            compare=compare_rows,
                            tmp_file_create=lambda level, run: self._tmp_file(f"sort-{i}-{level}-{run}"),
                            tmp_file_delete=lambda f: f._close(),
                            num_memory_blocks=self.memory_blocks_required(),
                            deduplicate=self.aggr_exprs[i].is_distinct
                        )
                        
                        reader = BufferedReader(1)
                        for buffer in reader.iter_buffer(tmp_file.iter_scan()):
                            for row in buffer:
                                sort_buffer.add(row) # need sorting
                        
                        previous = None
                        for row in sort_buffer.iter_and_clear():
                            curr = self.compiled.aggr_input_execs[i].eval(row0=row)
                            if self.aggr_exprs[i].is_distinct: # remove duplicates
                                if previous is not None and curr == previous: # if not first row and the same column value back to back
                                    continue # no need to add again
                            finalNeeded[grp_key][1][i] = self.compiled.aggr_add_execs[i].eval( # merge old state and new finding 
                                state=finalNeeded[grp_key][1][i], #finalNeeded[grp_key][i], 
                                new_val=curr
                            )
                            previous = curr
                        tmp_file._close()

                    else: # for incremental aggregates, process directly without sorting
                        reader = BufferedReader(1)
                        for buffer in reader.iter_buffer(tmp_file.iter_scan()):
                            for row in buffer:
                                curr = self.compiled.aggr_input_execs[i].eval(row0=row)
                                finalNeeded[grp_key][1][i] = self.compiled.aggr_add_execs[i].eval(
                                    state=finalNeeded[grp_key][1][i], #finalNeeded[grp_key][i], 
                                    new_val=curr
                                )
                        tmp_file._close()
           
        for grp_key, group_and_states in finalNeeded.items():
            original_group = group_and_states[0]  # first value element is the original group tuple, e.g. if we grouped by department, those names
            states = group_and_states[1] # second element is the list of aggregate states
            
            finals = [finalizer.eval(state=states[i]) for i, finalizer in enumerate(self.compiled.aggr_finalize_execs)] # taking iterative states that are completed to their final form, often that is just returning themselves, othertimes a state encorporated a lot of information that must be synthesized
            yield original_group + tuple(finals)