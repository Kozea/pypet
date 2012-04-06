from pypet import ComputedLevel, Hierarchy, Dimension, Query
from sqlalchemy.sql import func, extract

to_char = func.to_char

FORMAT_FUNCTIONS = {
        'year': lambda x: to_char(x, 'YYYY'),
        'month': lambda x: to_char(x, 'YYYY-MM'),
        'day': lambda x: to_char(x, 'YYYY-MM-DD'),
}


class TimeLevel(ComputedLevel):

    def __init__(self, name, dim_column, time_slice=None):
        if time_slice is None:
            time_slice = name

        def partial_trunc(column):
            return func.date_trunc(time_slice, column)

        def partial_extract(column):
            return extract(time_slice, column)
        label_expr = FORMAT_FUNCTIONS.get(time_slice, partial_extract)
        super(TimeLevel, self).__init__(name, dim_column,
                function=partial_trunc, label_expr=label_expr)


class TimeDimension(Dimension):

    def __init__(self, name, dim_column, time_levels):
        levels = [TimeLevel(level, dim_column) for level in time_levels]
        super(TimeDimension, self).__init__(name, [Hierarchy('default',
            levels)])
