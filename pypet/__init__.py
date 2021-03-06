from sqlalchemy.sql import (func, over, operators,
                            select as sql_select,
                            cast,
                            ColumnCollection)
from sqlalchemy import types
from sqlalchemy.sql.expression import (
    literal,
    or_, and_, ColumnClause, _Generative, _generative, _literal_as_binds)
from collections import OrderedDict, defaultdict
from itertools import groupby
from functools import wraps

from pypet.internals import (ValueSelect, IdSelect, OverSelect, FilterSelect,
                             AggregateSelect, PostFilterSelect, LabelSelect,
                             OrderSelect, join_table_with_query, compile)

from pypet import aggregates


def wrap_const(const):
    if not isinstance(const, CubeObject):
        if not isinstance(const, ColumnClause):
            const = ConstantMeasure(const)
        else:
            const = Measure(str(const), const,
                            agg=aggregates.identity_agg)
    return const


def wrap_filter(filter):
    if isinstance(filter, Member):
        return Filter(operators.eq, filter.level,
                      filter.id)
    elif not isinstance(filter, Filter):
        raise ValueError("Expected Member or Filter, got %s" %
                         filter)
    return filter


def operator(fun):
    @wraps(fun)
    def op_fun(self, *args):
        return fun(self, *(wrap_const(other) for other in args))
    return op_fun

def is_agg(column):
    if getattr(column, '_is_agg', False):
        return [column._is_agg]
    is_agg = [getattr(col, '_is_agg', False) for col in column.base_columns]
    if all(is_agg):
        return is_agg
    return []


def rank(name, partition_by=None, order_by=None):
    fun = func.dense_rank
    measure = RelativeMeasure(name, ConstantMeasure(1, agg=aggregates.custom_agg(lambda x: fun())),
            order_levels=order_by,
            over_levels=partition_by,
            desc=True)
    return measure



class CubeObject(_Generative):
    pass


class MetaData(dict):

    def __getattr__(self, key):
        return self.get(key, None)





class Measure(CubeObject):
    """A cube Measure."""

    _select_class = ValueSelect

    def __init__(self, name, expression, agg=aggregates.sum, metadata=None, need_groups=None):
        self.expression = expression
        self.agg = agg
        self.name = name or self.expression.label
        self.metadata = metadata or MetaData()
        self.need_groups = need_groups or []

    def _unnest(self):
        return self

    @property
    def agg(self):
        return self._agg

    @agg.setter
    def agg(self, value):
        if value is None:
            value = aggregates.identity_agg
        if not isinstance(value, aggregates.Aggregator):
            raise ValueError("The aggregate must be an instance of Aggregator")
        self._agg = value

    def select_instance(self, cuboid, *args, **kwargs):
        clauses = []
        for group in self.need_groups:
            clauses.extend(s.column_clause for s in group._as_selects(cuboid))
        kwargs.setdefault('need_groups', clauses)
        base = self._select_class(self, *args, **kwargs)
        if is_agg(base.column_clause) == [self.agg]:
            base.column_clause._is_agg = aggregates.identity_agg
            return base
        if self.agg:
            col = self.agg(base.column_clause, cuboid)
            col._is_agg = self.agg
            return AggregateSelect(self,
                                   name=self.name,
                                   column_clause=col.label(self.name),
                                   dependencies=[base])
        else:
            return base

    def _as_selects(self, cuboid):
        return [self.select_instance(cuboid, column_clause=self.expression,
                                     name=self.name)]

    @property
    def _label_for_select(self):
        return self.name

    def _adapt(self, aggregate):
        return self.replace_expr(aggregate.measures_expr[self.name])

    def _simplify(self, query):
        cc = ColumnCollection(*query.inner_columns)
        groups = [g._simplify(query) for g in self.need_groups]
        self.need_groups = groups
        if self.name in cc:
            col = cc[self.name]
            expr = self.replace_expr(col)
            return expr.label(self.name)
        return self

    def _score(self, agg):
        return (1, []) if self.name in agg.measures_expr else (-1, [])

    @operator
    def __mul__(self, other):
        name = '%s * %s' % (self.name, other.name)
        return ComputedMeasure(name, operators.mul, (self, other))

    @operator
    def __add__(self, other):
        name = '%s + %s' % (self.name, other.name)
        return ComputedMeasure(name, operators.add, (self, other))

    @operator
    def __sub__(self, other):
        name = '%s - %s' % (self.name, other.name)
        return ComputedMeasure(name, operators.sub, (self, other))

    @operator
    def __div__(self, other):
        name = '%s / %s' % (self.name, other.name)
        return ComputedMeasure(name, operators.div, (self, other))

    def over(self, *levels):
        name = '%s OVER (%s)' % (self.name, ','.join(
            [level.name for level in levels]))
        return RelativeMeasure(name, self, levels)

    def label(self, name=None):
        return MeasureLabel(self, name)

    @_generative
    def aggregate_with(self, agg_fun):
        self.agg = agg_fun

    def percent_over(self, *levels):
        return (self / self.over(*levels) * 100)

    @_generative
    def replace_expr(self, expression):
        self.expression = expression


for op_name in ('__eq__', '__lt__', '__le__', '__gt__', '__ge__', '__ne__', 'between_op'):
    def dumb_closure():
        sql_op = getattr(operators, op_name.strip('_'))
        @operator
        def op(self, *args):
            return Filter(sql_op, self, *args)
        return op
    op = dumb_closure()
    op.__name__ == op_name
    if op_name.endswith('_op'):
        op_name = op_name[:-3]
    setattr(Measure, op_name, op)




class CountMeasure(Measure):

    def __init__(self, name, expr=literal(1), distinct=False):
        if distinct:
            agg = aggregates.count_distinct
        else:
            agg = aggregates.count
        super(CountMeasure, self).__init__(name, expr,
                                               agg=agg)

    def _adapt(self, aggregate):
        return self.replace_expr(aggregate.measures_expr[self.name])

    def _score(self, agg):
        return (1 * 0.8 ** (len(agg.levels))), []

    def _simplify(self, query):
        cc = ColumnCollection(*query.inner_columns)
        groups = [g._simplify(query) for g in self.need_groups]
        self.need_groups = groups
        if self.name in cc:
            col = cc[self.name].label(self.name)
            expr = self.replace_expr(col)
            isagg = is_agg(col)
            if isagg == [aggregates.count]:
                expr = expr.aggregate_with(aggregates.sum)
            elif is_agg == [aggregates.count_distinct]:
                raise ValueError('Cannot aggregate distinct')
            elif not is_agg:
                expr.column_clause._is_agg = aggregates.count
            return expr
        return self


class RelativeMeasure(Measure):

    _select_class = OverSelect

    def __init__(self, name, measure, over_levels=None, order_levels=None,
                 agg=aggregates.identity_agg, desc=True, metadata=None, need_groups=None):
        self.name = name
        self.measure = measure._unnest()
        self.over_levels = over_levels or []
        self.order_levels = order_levels or []
        self.agg = agg
        self.inner_agg = self.measure.agg
        self.desc = desc
        self.metadata = metadata or MetaData()
        self.need_groups = need_groups or []

    def _adapt(self, aggregate):
        over_levels = [over_level._adapt(aggregate) for over_level in
                       self.over_levels]
        order_levels = [order_level._adapt(aggregate) for order_level in
                        self.order_levels]
        return RelativeMeasure(self.name, self.measure._adapt(aggregate),
                               over_levels, order_levels)

    def _score(self, aggregate):
        over_score = 0
        order_score = 0
        over_dims = []
        order_dims = []
        over_scores, over_dims = zip(*[
            over_level._score(aggregate)
            for over_level in self.over_levels]) or ([1], [])
        over_dims = [d for dim in over_dims for d in dim]
        over_score = min(over_scores)
        order_scores, order_dims = zip(
            *[order_level._score(aggregate)
              for order_level in self.order_levels]) or ([1], [])
        order_score = min(order_scores)
        order_dims = [d for dim in order_dims for d in dim]
        measure_score, measure_dims = self.measure._score(aggregate)
        dims = over_dims + order_dims + measure_dims
        if any(score < 0 for score in
                (over_score, order_score, measure_score)):
            return -1, dims
        return sum([over_score, order_score, measure_score]), dims

    def _simplify(self, query):
        cc = ColumnCollection(*query.inner_columns)
        if self.name in cc:
            return Measure(self.name, cc[self.name], agg=self.agg)
        over_levels = [over_level._simplify(query) for over_level in
                       self.over_levels]
        order_levels = [order_level._simplify(query) for order_level in
                        self.order_levels]
        ms = self.measure._simplify(query)

        if not ms.agg:
            ms = ms.aggregate_with(self.inner_agg)
            ms.need_groups.extend(over_levels)
            return ms
        return RelativeMeasure(self.name, ms, over_levels, order_levels,
                               agg=self.agg, desc=self.desc)

    def _as_selects(self, cuboid):
        over_selects = order_selects = []
        measure_selects = self.measure._as_selects(cuboid)
        assert len(measure_selects) == 1
        ms = measure_selects[0]
        over_selects = [sel for over_level in self.over_levels
                        for sel in over_level._as_selects(cuboid)]
        partition = [over_select.column_clause for over_select in
                     over_selects] or None
        ms.dependencies.extend(over_selects)
        order_selects = [sel for order_level in self.order_levels
                         for sel in order_level._as_selects(cuboid)]
        sort_order = 'desc' if self.desc else 'asc'
        order = [getattr(order_select.column_clause, sort_order)()
                 for order_select in order_selects] or None
        ms.dependencies.extend(order_selects)
        col = self.inner_agg(ms.column_clause, cuboid)
        col._is_agg = self.inner_agg
        over_expr = over(col, partition_by=partition, order_by=order)
        over_expr._is_agg = self.agg
        return [self.select_instance(cuboid, column_clause=over_expr,
                                     name=self.name,
                                     dependencies=measure_selects + over_selects)]

    def aggregate_with(self, agg):
        new_relative = (super(RelativeMeasure, self)
                        .aggregate_with(aggregates.identity_agg))
        return ForceAgg(new_relative, agg)


class ConstantMeasure(Measure):

    def __init__(self, constant, agg=aggregates.identity_agg):
        self.constant = constant
        self.agg = agg

    @property
    def name(self):
        return str(self.constant)

    def _adapt(self, aggregate):
        return self

    def _simplify(self, query):
        return self

    def _score(self, aggregate):
        return 0, []

    def _as_selects(self, cuboid):
        col = _literal_as_binds(self.constant)
        col._is_agg = self.agg
        return [self._select_class(self, column_clause=col)]

    @_generative
    def replace_expr(self, expression):
        pass

    def __eq__(self, other):
        return (isinstance(other, ConstantMeasure) and
                self.constant == other.constant and
                self.agg == other.agg)


class ComputedMeasure(Measure):

    def __init__(self, name, operator, operands, agg=aggregates.identity_agg,
                 metadata=None):
        self.name = name
        self.operator = operator
        self.operands = operands
        self.agg = agg
        self.metadata = metadata or MetaData()
        self.need_groups = []

    def _adapt(self, aggregate):
        return ComputedMeasure(self.name, self.operator,
                               [op._adapt(aggregate) for op in self.operands],
                               self.agg)

    def _simplify(self, query):
        cc = ColumnCollection(*query.inner_columns)
        if self.name in cc:
            agg = self.agg
            return Measure(self.name, cc[self.name], agg)
        return ComputedMeasure(self.name, self.operator,
                               [op._simplify(query) for op in self.operands],
                               self.agg)

    def _score(self, aggregate):
        scores, dims = zip(*[op._score(aggregate) for op in self.operands])
        dims = [d for dim in dims for d in dim]
        return min(scores), dims

    def _as_selects(self, cuboid):
        sub_selects = reduce(
            list.__add__, [op._as_selects(cuboid) for op in
                           self.operands], [])
        self_expr = (self.operator(*[(sub.column_clause)
                                     for sub in sub_selects])
                     .label(self.name))
        if all(getattr(sub.column_clause, '_is_agg', False)
               for sub in sub_selects):
            self_expr._is_agg = self.agg
        return [self.select_instance(cuboid, column_clause=self_expr,
                dependencies=sub_selects,
                name=self.name)]


class MeasureLabel(ComputedMeasure):

    def __init__(self, measure, name=None):
        name = name or 'anon_%d' % id(measure)
        measure = measure._unnest()
        super(MeasureLabel, self).__init__(name, lambda x: x, (measure,),
                                           measure.agg)

    def _unnest(self):
        return self.operands[0]._unnest()

    def _simplify(self, query):
        return MeasureLabel(self.operands[0]._simplify(query), self.name)

    def label(self, name=None):
        return super(MeasureLabel, self).label(name)


class ForceAgg(Measure):

    def __init__(self, measure, agg):
        self.measure = measure._unnest()
        self.agg = agg

    def __getattr__(self, key):
        return getattr(self.measure, key)

    def _adapt(self, aggregate):
        return ForceAgg(self.measure._adapt(aggregate), agg=self.agg)

    def _simplify(self, query):
        cc = ColumnCollection(*query.inner_columns)
        base = self.measure._simplify(query)
        if self.name in cc:
            col = cc[self.name]
            agg = self.agg
            col_is_agg = is_agg(col)
            if col_is_agg == [self.agg]:
                return base.replace_expr(col.label(self.name)).label(self.name)
        return ForceAgg(base, self.agg).label(self.name)

    def _score(self, aggregate):
        return self.measure._score(aggregate)

    def _as_selects(self, cuboid):
        selects = self.measure._as_selects(cuboid)
        agg_selects = []
        for select in selects:
            col = self.agg(select.column_clause)
            col._is_agg = self.agg
            agg_selects.append(AggregateSelect(
                self,
                name=self.name,
                column_clause=col.label(self.name),
                dependencies=[select]))
        return agg_selects


class CutPoint(CubeObject):
    """Abstract class marking a class as suitable for a CutPoint."""

    def _unnest(self):
        return self

class Filter(CubeObject):

    _select_class = FilterSelect

    def __init__(self, operator, *operands):
        self.operands = [wrap_const(op) for op in operands]
        self.operator = operator

    def _score(self, aggregate):
        scores, dims = zip(*[op._score(aggregate) for op in self.operands])
        dims = [d for dim in dims for d in dim]
        return min(scores), dims

    def _adapt(self, aggregate):
        return self.__class__(self.operator, *[clause._adapt(aggregate)
                                               for clause in self.operands])

    def _simplify(self, query):
        return self.__class__(self.operator, *[clause._simplify(query)
                                               for clause in self.operands])

    def _as_selects(self, cuboid):
        sub_operands, deps = self._build_sub_selects_and_deps(cuboid)
        return [self._select_class(self, where_clause=self.operator(
            *[sub.column_clause for sub in sub_operands]),
            dependencies=sub_operands)]

    def __eq__(self, other):
        return (isinstance(other, Filter) and self.operator == other.operator
                and self.operands == other.operands)

    def __or__(self, other):
        return OrFilter(self, other)

    def __and__(self, other):
        return AndFilter(self, other)

    def _build_sub_selects_and_deps(self, cuboid, _all=False):
        if _all:
            sub_operands = [sub
                            for clause in self.operands
                            for sub in clause._as_selects(cuboid)]
        else:
            sub_operands = [clause._as_selects(cuboid)[0]
                            for clause in self.operands]
        for sub in sub_operands:
            if isinstance(sub, FilterSelect):
                sub.embedded = True
        return sub_operands, sub_operands


class AndFilter(Filter):

    def __init__(self, *operands):
        super(AndFilter, self).__init__(and_, *(wrap_filter(op) for op in operands))

    def _as_selects(self, cuboid):
        sub_operands, deps = self._build_sub_selects_and_deps(cuboid, _all=True)
        return [self._select_class(self, where_clause=and_(
            *[sub.where_clause
                for sub in sub_operands if sub.where_clause is not None]),
            dependencies=deps)]

    def _adapt(self, aggregate):
        return self.__class__(*[clause._adapt(aggregate)
                                for clause in self.operands])

    def _simplify(self, query):
        return self.__class__(*[clause._simplify(query)
                                for clause in self.operands])


class OrFilter(Filter):

    def __init__(self, *operands):
        super(OrFilter, self).__init__(or_, *(wrap_filter(op) for op in operands))

    def _adapt(self, aggregate):
        return self.__class__(*[clause._adapt(aggregate)
                                for clause in self.operands])

    def _simplify(self, query):
        return self.__class__(*[clause._simplify(query)
                                for clause in self.operands])

    def _as_selects(self, cuboid):
        sub_operands, deps = self._build_sub_selects_and_deps(cuboid, _all=True)
        return [self._select_class(self, where_clause=or_(
            *[sub.where_clause
                for sub in sub_operands if sub.where_clause is not None]),
            dependencies=deps)]


class PostFilter(Filter):
    _select_class = PostFilterSelect


class Member(CutPoint):
    """A member of a Level. Ex: The year 2010 is a member of the Year level of
    the time dimension."""

    def __init__(self, level, id, label, metadata=None):
        self.level = level
        self.id = id
        self.label = label
        self.label_expression = cast(_literal_as_binds(self.label),
                                     types.Unicode)
        self.id_expr = _literal_as_binds(self.id)
        self.metadata = metadata or MetaData()

    def _adapt(self, aggregate):
        return Member(self.level._adapt(aggregate), self.id, self.label)

    def _simplify(self, query):
        cc = ColumnCollection(*query.inner_columns)
        if self._label_for_select in cc:
            return Member(self.level._simplify(query), self.id, self.label)
        return Member(self.level._simplify(query), self.id, self.label)

    def _as_selects(self, cuboid=None):
        subs = [sub for sub in self.level._as_selects(cuboid)
                if isinstance(sub, IdSelect)]
        assert len(subs) == 1
        selects = [LabelSelect(self, column_clause=self.label_expression,
                               name=self._label_label_for_select,
                               joins=[self.level.label_column.table],
                               is_constant=True),
                   IdSelect(self, column_clause=self.id_expr,
                            joins=[self.level.column.table],
                            name=self._label_for_select, is_constant=True)]
        return selects

    @property
    def children_query(self):
        if self.level.child_level is None:
            raise ValueError("Cannot build a query for a level without child")
        query = self.level.child_level.members_query
        join_table_with_query(query, self.level.column.table)
        query = query.where(self.level._id_column == self.id)
        return query

    @property
    def children(self):
        if self.level.child_level is None:
            return []
        return [Member(self.level.child_level, v.id, v.label)
                for v in self.children_query.execute()]

    def _score(self, agg):
        return self.level._score(agg)

    @property
    def _label_for_select(self):
        return self.level._label_for_select

    @property
    def _label_label_for_select(self):
        return self.level._label_label_for_select


    @property
    def dimension(self):
        """Returns this level dimension."""
        return self.level.dimension


class Level(CutPoint):
    """A level in a dimension hierarchy."""

    def __init__(self, name, column=None, label_column=None,
                 label_expression=None,
                 metadata=None):
        self.label_column = (label_column if label_column is not None
                             else column)
        self.name = name
        self.is_label = False
        self.column = column
        if label_expression is None:
            label_expression = lambda x: x
        self.label_expression = label_expression
        self.child_level = None
        self.parent_level = None
        self.hierarchy = None
        self.metadata = metadata or MetaData()
        self._level_key = None
        self._level_label_key = None

    def bind(self, hierarchy):
        """Late binding of level to hierarchies."""
        self.hierarchy = hierarchy
        levels = hierarchy.levels.values()
        my_idx = hierarchy.level_index(self)
        if my_idx > 0:
            self.parent_level = levels[my_idx - 1]
        if my_idx + 1 < len(levels):
            self.child_level = levels[my_idx + 1]

    @property
    def dimension(self):
        """Returns this level dimension."""
        return self.hierarchy.dimension if self.hierarchy is not None else None

    def __getitem__(self, key):
        values = list(self.members_query.where(self._id_column == key)
                      .limit(1).execute())[0]
        return Member(self, values.id, values.label)

    def member_by_label(self, label):
        values = list(self.members_query.where(self._label_column == label)
                      .limit(1).execute())[0]
        return Member(self, values.id, values.label)

    def _score(self, agg):
        dim = self.dimension
        for agglevel in agg.levels:
            if agglevel.dimension == self.dimension:
                base_level = agglevel
                score = 1
                while(base_level is not None):
                    if ((base_level.dimension == self.dimension) and
                            self.name == base_level.name):
                        return score, [dim]

                    same_hierarchy_levels = self.hierarchy.levels.values()

                    for idx, l in enumerate(same_hierarchy_levels):
                        if l.name == base_level.name:
                            if idx >= 1:
                                base_level = same_hierarchy_levels[idx - 1]
                            else:
                                base_level = None
                                break
                    score *= 0.5
                    score *= 0.5
        return -1, [dim]

    @_generative
    def replace_expr(self, expr, label_column=None):
        self.column = expr
        self.child_level = None
        if label_column is not None:
            self.label_column = label_column

    @_generative
    def replace_label_expression(self, label_expression):
        self.label_expression = label_expression

    @_generative
    def replace_level(self, level):
        self.child_level = level

    @property
    def _label_column(self):
        return self.label_expression(self.label_column)

    @property
    def _id_column(self):
        return self.column

    @property
    @_generative
    def label_only(self):
        self.is_label = True

    @_generative
    def label(self, label):
        self._level_label_key = label

    @property
    def _label_label_for_select(self):
        return self._level_label_key or '%s_label' % self._label_for_select

    @property
    def _label_for_select(self):
        return self._level_key or '%s_%s' % (self.dimension.name, self.name)


    def _as_selects(self, cuboid=None):
        sub_selects = []
        sub_joins = []
        if self.child_level:
            sub_selects = self.child_level._as_selects(cuboid)
            sub_joins = [elem for alist in sub_selects
                         for elem in alist.joins]
        label_select = LabelSelect(self,
                            column_clause=self._label_column,
                            name=self._label_label_for_select,
                            dependencies=[],
                            joins=sub_joins + [self.column.table,
                                               self.label_column.table])

        if self.is_label:
            return [label_select]
        else:
            return [IdSelect(self, column_clause=self.column,
                             name=self._label_for_select,
                             dependencies=[],
                             joins=sub_joins + [self._id_column.table,
                                                self.label_column.table]),
                    label_select]

    def _adapt(self, aggregate):
        for level in aggregate.levels:
            if level.dimension == self.dimension:
                if level.name == self.name:
                    column = aggregate.levels.get(level)
                    return self.replace_expr(column)
                else:
                    if self.child_level is None:
                        raise KeyError('Cannot find matching level in'
                                       'aggregate!')
                    return self.replace_level(
                        self.child_level._adapt(aggregate))

    def _simplify(self, query):
        cc = ColumnCollection(*query.inner_columns)
        if self._label_for_select in cc:
            dim_expr = cc[self._label_for_select]
            label_col = self._label_label_for_select
            if label_col in cc:
                column_expr = cc[label_col]
                label_expr = lambda x: x
            else:
                column_expr = dim_expr
                label_expr = self.label_expression
            return (self.replace_expr(dim_expr, column_expr)
                    .replace_label_expression(label_expr))
        return self

    @property
    def members_query(self):
        return sql_select([self._id_column.label('id'),
                           self._label_column.label('label')])

    @property
    def members(self):
        return [Member(self, value.id, value.label)
                for value in self.members_query.distinct().execute()]


for op_name in ('__eq__', 'like_op', 'ilike_op', '__ne__'):
    def dumb_closure():
        sql_op = getattr(operators, op_name.strip('_'))
        @operator
        def op(self, *args):
            return Filter(sql_op, self, *args)
        return op
    op = dumb_closure()
    op.__name__ == op_name
    if op_name.endswith('_op'):
        op_name = op_name[:-3]
    setattr(Level, op_name, op)



class ComputedLevel(Level):

    def __init__(self, name, column=None, label_expression=None,
                 function=lambda x: x, metadata=None):
        super(ComputedLevel, self).__init__(
            name, column, label_expression=label_expression, metadata=None)
        self.function = function
        self.metadata = metadata or MetaData()

    @_generative
    def replace_expr(self, expr, label_column=None):
        self.column = expr
        self.child_level = None
        if label_column is not None:
            self.label_column = label_column
        else:
            self.label_column = self.column

    @_generative
    def replace_level(self, level):
        self.child_level = level
        self.column = level.column
        self.label_column = self.column

    def __getitem__(self, key):
        return super(ComputedLevel, self).__getitem__(self.function(key))

    @property
    def _id_column(self):
        return self.function(self.column).label(self.name)

    def _as_selects(self, cuboid=None):
        col = self._id_column
        dep = IdSelect(self, column_clause=self.column)
        return [IdSelect(self, name=self._label_for_select, column_clause=col,
                         dependencies=[dep]),
                LabelSelect(self, name=self._label_label_for_select,
                            column_clause=self._label_column,
                            dependencies=[dep])]


class AllLevel(Level):
    """A dummy, top-level level."""

    def __init__(self, name='All', label='All', metadata=None):
        self.label = label
        self.name = name
        self.label_expression = cast(_literal_as_binds(self.label),
                                     types.Unicode)
        self.parent_level = None
        self.metadata = metadata or MetaData()
        self.column = None
        self._level_key = name
        self._level_label_key = label

    def _as_selects(self, cuboid=None):
        return [LabelSelect(self, name=self._label_for_select + '_label',
                            column_clause=self.label_expression,
                            is_constant=True),
                IdSelect(self, name=self._label_for_select,
                         column_clause=self.label_expression,
                         is_constant=True)]

    def _simplify(self, query):
        return self

    def _adapt(self, aggregate):
        return self

    def _score(self, agg):
        score, dims = super(AllLevel, self)._score(agg)
        if score < 0:
            # The dimension itself is not in the table, therefore the rows
            # represent the total
            return 1, dims
        else:
            return score * 0.5, dims


class Hierarchy(object):
    """A dimensions hierarchy."""

    def __init__(self, name, levels, metadata=None):
        self.name = name
        self.levels = [AllLevel()] + levels
        self.default_level = self.levels[0]
        self.levels = OrderedDict((level.name, level) for level in self.levels)
        self.metadata = metadata or MetaData()

    def bind(self, dimension):
        """Late binding of this hierarchy to a dimension."""
        self.dimension = dimension
        for level in self.levels.values():
            level.bind(self)

    def level_index(self, searched):
        for idx, level in enumerate(self.levels):
            if level == searched.name:
                return idx

    @property
    def l(self):
        return self.levels


class Dimension(object):
    """A cube dimension."""

    def __init__(self, name, hierarchies, metadata=None):
        self.default_hierarchy = hierarchies[0]
        self.default_level = self.default_hierarchy.default_level
        self.name = name
        for hierarchy in hierarchies:
            hierarchy.bind(self)
        self.hierarchies = OrderedDict(
            (hiera.name, hiera) for hiera in hierarchies)
        self.metadata = metadata or MetaData()

    @property
    def levels(self):
        return self.default_hierarchy.levels

    @property
    def l(self):
        return self.levels

    @property
    def h(self):
        return self.hierarchies


class ResultProxy(OrderedDict):

    def __init__(self, query, result, label='All'):
        self.dims = query.axes
        self.label = label
        self.query = query
        self.orders = query.orders
        this = self

        class default_scalar_value(defaultdict):

            @property
            def measure_dict(self):
                return {m.name: m for m in query.measures}

            def keys(self):
                return [m.name for m in query.measures]

            def __missing__(self, key):
                if key not in self.keys():
                    raise KeyError('Not a valid value!')
                self[key] = self.measure_dict[key].agg.py_impl([
                    child.scalar_value[key] for child in this.values()])
                return self[key]

        self.scalar_value = default_scalar_value()
        super(ResultProxy, self).__init__()
        self.update(self._dims_dict(result))

    def _dims_dict(self, lines):
        result = OrderedDict()
        if len(self.dims) == 0:
            # Just a scalar!
            self.scalar_value = list(lines)[0]
            return result
        dim_key = self.dims[0]._label_for_select
        dim_label = self.dims[0]._label_label_for_select
        next_dims = self.dims[1:]
        append = lambda label, lines: ResultProxy(
            self.query.axis(*next_dims), lines, label)

        def key_func(x):
            key = getattr(x, dim_key)
            if dim_label in x:
                label = x[dim_label]
            else:
                label = key
            return key, label

        if len(self.dims) > 1 or not self.orders:
            lines = sorted(lines, key=key_func)
        for (key, label), lines in groupby(lines,
                                           key_func):
            result[key] = append(label, list(lines))
        return result

    def by_label(self):
        return OrderedDict((value.label, value) for value in self.values())

    def __getitem__(self, key):
        try:
            return super(ResultProxy, self).__getitem__(key)
        except KeyError:
            return self.scalar_value[key]

    def __getattr__(self, key):
        if self.scalar_value is not None:
            return getattr(self.scalar_value, key)
        raise AttributeError('This result is not a scalar')

    def __eq__(self, other):
        return (super(ResultProxy, self).__eq__(other) and
                dict(self.scalar_value) == dict(other.scalar_value))


class OrderClause(CubeObject):

    def __init__(self, measure, reverse=False):
        self.measure = measure._unnest()
        self.reverse = reverse

    def _score(self, agg):
        return self.measure._score(agg)

    def _adapt(self, agg):
        return OrderClause(self.measure._adapt(agg), self.reverse)

    def _simplify(self, query):
        return OrderClause(self.measure._simplify(query), self.reverse)

    def _as_selects(self, cuboid):
        sub_selects = [sel for sel in self.measure._as_selects(cuboid)]
        col = sub_selects[0].column_clause
        if is_agg(col):
            col._is_agg = is_agg(col)[0]
        return [OrderSelect(self, column_clause=col,
                dependencies=sub_selects, reverse=self.reverse)]


class Query(_Generative):

    def __init__(self, cuboid, axes, measures):
        self.cuboid = cuboid
        self.axes = axes
        self.measures = measures
        self.filter_clause = None
        self.orders = []

    def _generate(self):
        newself = super(Query, self)._generate()
        newself.orders = list(self.orders)
        newself.axes = list(self.axes)
        newself.measures = list(self.measures)
        return newself

    def _as_sql(self):
        best_agg = self.cuboid._find_best_agg(self.parts)
        query = self._adapt(best_agg)
        things = query.parts
        selects = [sel for t in things for sel in t._as_selects(best_agg)]
        query = sql_select([], from_obj=query.cuboid.selectable)
        return compile(selects, query, best_agg)

    @property
    def parts(self):
        values = self.axes + self.measures + self.orders
        if self.filter_clause is not None:
            values.append(self.filter_clause)
        return values

    @_generative
    def _adapt(self, agg):
        if agg != self.cuboid:
            self.axes = [axis._adapt(agg) for axis in self.axes]
            self.measures = [measure._adapt(agg) for measure in
                    self.measures]
            if self.filter_clause is not None:
                self.filter_clause = self.filter_clause._adapt(agg)
            self.orders = [order._adapt(agg) for order in self.orders]
            self.cuboid = agg

    def __eq__(self, other):
        if isinstance(other, Query):
            return (self.cuboid == other.cuboid and
                    self.axes == other.axes and
                    self.measures == other.measures and
                    self.filter_clause == other.filter_clause)
        return False

    @_generative
    def filter(self, *members):
        if members:
            members = [wrap_filter(member) for member in members]
            if len(members) > 1:
                member = OrFilter(*members)
            else:
                member = members[0]
            if self.filter_clause is not None:
                self.filter_clause = AndFilter(self.filter_clause, member)
            else:
                self.filter_clause = member

    def append_filter(self, filter):
        if self.filter_clause is not None:
            self.filter_clause = AndFilter(self.filter_clause, filter)
        else:
            self.filter_clause = filter

    @_generative
    def order_by(self, measure, reverse=False):
        self.orders.append(OrderClause(measure, reverse))

    @_generative
    def slice(self, level):
        assert isinstance(level, CutPoint), ("You must slice on a CutPoint"
            "(a level or a member, not a %s" % level.__class__.__name__)
        for idx, axis in enumerate(list(self.axes)):
            if axis.dimension.name == level.dimension.name:
                self.axes[idx] = level
                return
        self.axes.append(level)

    @_generative
    def measure(self, *measures):
        self.measures = list(measures)

    @_generative
    def axis(self, *axes):
        self.axes = list(axes)

    @_generative
    def top(self, n, expr, partition_by=None):
        if (not isinstance(partition_by, list) and partition_by is not None):
            partition_by = [partition_by]
        order_by = [expr]
        name = 'RANK OVER %s' % expr.name
        measure = rank(name, partition_by, order_by)
        self.orders.append(OrderClause(measure))
        return self.append_filter(PostFilter(operators.le,
                                            measure, ConstantMeasure(n)))

    def execute(self):
        return ResultProxy(self, self._as_sql().execute())

    def __getslice__(self, i, j):
        return ResultProxy(self, self._as_sql().offset(i).limit(j-i).execute())


class Aggregate(_Generative):

    def __init__(self, selectable, levels, measures, fact_count_column,
                 fact_count_measure=None):
        self.selectable = selectable
        self.fact_count_column = fact_count_column
        self.fact_count_column_name = fact_count_column.name
        self.fact_count_measure = fact_count_measure or  CountMeasure(self.fact_count_column_name).label('FACT_COUNT')
        measures = dict(measures)
        measures[self.fact_count_measure] = self.fact_count_column
        self.measures_expr = OrderedDict((measure.name, expr)
                for measure, expr in measures.items())
        self.measures = OrderedDict((measure.name, measure)
                for measure, expr in measures.items())
        self.levels = levels


    def score(self, things):
        scores, dims = zip(*[thing._score(self) for thing in things])
        if any(score < 0 for score in scores):
            return -100
        dims = set(d for dim in dims for d in dim)
        self_dims = set(l.dimension for l in self.levels)
        not_used_dims = self_dims - dims
        # Take not-used levels in consideration too.
        factor = 0
        for level in self.levels:
            if level.dimension in not_used_dims:
                factor += level.hierarchy.level_index(level)
        return sum(scores) + 0.3 ** factor


class Cube(_Generative):

    def __init__(self, metadata, fact_table, dimensions, measures,
            aggregates=None, fact_count_column=None,
            fact_count_measure_name='FACT_COUNT'):
        self.alchemy_md = metadata
        self.dimensions = OrderedDict((dim.name, dim) for dim in dimensions)
        self.measures = OrderedDict((measure.name, measure) for measure in
                measures)
        self.table = fact_table
        self.aggregates = aggregates or []
        self.fact_count_column = fact_count_column
        self.fact_count_measure_name = 'FACT_COUNT'
        self.fact_count_column_name = self.fact_count_measure_name
        self.fact_count_measure = CountMeasure(fact_count_measure_name)

        self.measures[fact_count_measure_name] = self.fact_count_measure


    @property
    def query(self):
        return Query(self, [dim.default_level for dim in
            self.dimensions.values()],
            self.measures.values())

    @property
    def selectable(self):
        return self.table

    @property
    def d(self):
        return self.dimensions

    @property
    def m(self):
        return self.measures

    def _find_best_agg(self, parts):
        agg_scores = ((agg, agg.score(parts))
                for agg in self.aggregates)
        best_agg, score = reduce(lambda (x, scorex), (y, scorey): (x, scorex)
                if scorex >= scorey
                else (y, scorey), agg_scores, (self, 0))
        return best_agg

    def best_agg_level(self, level):
        """Returns the level, using the best aggregate available."""
        return level._adapt(self._find_best_agg([level]))
