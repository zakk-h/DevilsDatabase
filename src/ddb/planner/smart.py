from typing import cast
import itertools

from ..globals import DEFAULT_BNLJ_BUFFER_SIZE
from ..validator import valexpr, ValExpr, SFWGHLop, BaseTableLop
from ..executor import StatementContext, QPop, BNLJoinPop, FilterPop, ProjectPop

from .interface import Planner, PlannerException
from .util import add_groupby_by_sorting, add_having_and_select
from .baseline import BaselinePlanner

class SmartPlanner(Planner):
    @classmethod
    def optimize_block(cls, context: StatementContext, block: SFWGHLop) -> QPop:
        # falls back to BaselinePlanner for now --- REPLACE WITH YOUR IMPLEMENTATION
        return BaselinePlanner.optimize_block(context, block)
