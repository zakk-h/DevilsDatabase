"""This package implements the query optimizer, which compiles the query for efficient execution.
After a query is validated,
the planner "compiles" the resulting logical plan (a :class:`.Lop` tree)
into a physical plan (a :class:`.QPop` tree), ready for execution.

Several planners, varying in their sophistication, are implemented;
the desired planner can be chosen by using ``SET PLANNER [NAIVE|BASELINE|SELINGER]`` inside a session.
Additional planner options (e.g., turning on/off hash join plans) can be futher set;
see :class:`.Planner.Options` for details.
"""

from .interface import PlannerException, Planner
from .naive import NaivePlanner
from .baseline import BaselinePlanner
from .smart import SmartPlanner