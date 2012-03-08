from pypet import ComputedLevel, Hierarchy, Dimension
from sqlalchemy.sql import func


class TimeLevel(ComputedLevel):

    def __init__(self, name, dim_column, time_slice=None):
        if time_slice is None:
            time_slice = name

        def partial_trunc(column):
            return func.date_trunc(time_slice, column)

        super(TimeLevel, self).__init__(name, dim_column,
                function=partial_trunc)


class TimeDimension(Dimension):

    def __init__(self, name, dim_column, time_levels):
        levels = [TimeLevel(level, dim_column) for level in time_levels]
        return super(TimeDimension, self).__init__(name, [Hierarchy('default',
            levels)])
