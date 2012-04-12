from sqlalchemy.sql import func, case, cast
from sqlalchemy import types
import abc


class Aggregator(object):

    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def __call__(self, column_clause, cuboid):
        raise NotImplemented("Not implemented!")

    @abc.abstractmethod
    def accumulator(self, old_value, new_value):
        raise NotImplemented("Not implemented!")


class identity_agg(Aggregator):

    def __call__(self, column_clause, cuboid):
        return column_clause

    def __nonzero__(self):
        return False

    def accumulator(self, old_value, new_value):
        raise NotImplemented("YOU SHOULD NOT USE IDENTITY AGG IN A TRIGGER")

identity_agg = identity_agg()


class avg(Aggregator):

    def __call__(self, column_clause, cuboid):
        if cuboid.fact_count_column is not None:
            count = func.sum(cuboid.fact_count_column)
            return case([(count == 0, 0)], else_=(
                    func.sum(column_clause * cuboid.fact_count_column) /
                           cast(count,
                               types.Numeric)))
        return func.avg(column_clause)

    def accumulator(self, oldrow, newrow):
        pass


avg = avg()


class sum(Aggregator):

    def __call__(self, column_clause, cuboid):
        return func.sum(column_clause)

    def accumulator(self, oldrow, newrow):
        pass

sum = sum()
