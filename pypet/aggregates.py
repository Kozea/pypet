from sqlalchemy.sql import func, case, cast
from sqlalchemy import types
import abc
import __builtin__


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

    def py_impl(self, collection):
        return collection

    def accumulator(self, column_name, new_row, agg_row):
        raise NotImplemented("YOU SHOULD NOT USE IDENTITY AGG IN A TRIGGER")


class avg(Aggregator):

    def __call__(self, column_clause, cuboid):
        if cuboid.fact_count_column is not None:
            count = func.sum(cuboid.fact_count_column)
            return case([(count == 0, 0)], else_=(
                func.sum(column_clause * cuboid.fact_count_column) /
                cast(count,
                     types.Numeric)))
        return func.avg(column_clause)

    def py_impl(self, collection):
        return __builtin__.sum(collection) / len(collection)

    def accumulator(self, column_name, new_row, agg_row, old_row=None):
        new_count = new_row.count
        new_total = new_row.c[column_name] * new_row.count
        if old_row is not None:
            new_count = new_count - old_row.count
            new_total = (new_total -
                        (old_row.c[column_name] * old_row.count))
        agg_count = func.coalesce(agg_row.count, 0)
        agg_value = func.coalesce(agg_row.c[column_name]) * agg_count
        total_count = new_count + agg_count
        return case([(total_count == 0, 0)],
                    else_=(agg_value + new_total) / total_count)


class sum(Aggregator):

    def __call__(self, column_clause, cuboid):
        return func.sum(column_clause)

    def py_impl(self, collection):
        return __builtin__.sum(collection)

    def accumulator(self, column_name, new_row, agg_row, old_row=None):
        total_sum = new_row.c[column_name]
        if old_row is not None:
            total_sum = total_sum - old_row.c[column_name]
        return (total_sum +
                func.coalesce(agg_row.c[column_name], 0))


class count(sum):

    def __call__(self, column_clause, cuboid):
        if cuboid.fact_count_column is not None:
            return func.sum(cuboid.fact_count_column)
        else:
            return func.count(1)

    def py_impl(self, collection):
        return len(collection)


identity_agg = identity_agg()
sum = sum()
avg = avg()
count = count()
