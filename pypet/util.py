from pypet import ComputedLevel, Hierarchy, Dimension, Query
from sqlalchemy.sql import func, extract

to_char = func.to_char

FORMAT_FUNCTIONS = {
        'year': lambda x: to_char(x, 'YYYY'),
        'month': lambda x: to_char(x, 'YYYY-MM'),
        'day': lambda x: to_char(x, 'YYYY-MM-DD'),
}


class TimeLevel(ComputedLevel):

    def __init__(self, name, column, time_slice=None):
        if time_slice is None:
            time_slice = name

        def partial_trunc(column):
            return func.date_trunc(time_slice, column)

        def partial_extract(column):
            return extract(time_slice, column)
        label_expression = FORMAT_FUNCTIONS.get(time_slice, partial_extract)
        super(TimeLevel, self).__init__(name, column,
                function=partial_trunc, label_expression=label_expression)


class TimeDimension(Dimension):

    def __init__(self, name, column, time_levels):
        levels = [TimeLevel(level, column) for level in time_levels]
        super(TimeDimension, self).__init__(name, [Hierarchy('default',
            levels)])
