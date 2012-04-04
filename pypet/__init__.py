from sqlalchemy.sql import (func, over, operators,
        select as sql_select,
        ColumnCollection, cast)
from sqlalchemy import types
from sqlalchemy.sql.expression import (
        or_,
        and_,
        ColumnClause,
        _Generative, _generative, _literal_as_binds)
from collections import OrderedDict
from itertools import groupby
from functools import wraps

from pypet.internals import (ValueSelect, IdSelect, OverSelect, FilterSelect,
        AggregateSelect, PostFilterSelect, LabelSelect,
        OrderSelect,
        compile)

from pypet.aggregates import identity_agg, avg


def wrap_const(const):
    if not isinstance(const, CubeObject):
        if not isinstance(const, ColumnClause):
            const = ConstantMeasure(const)
        else:
            const = Measure(str(const), const,
                    agg=identity_agg)
    return const


def operator(fun):
    @wraps(fun)
    def op_fun(self, other):
        return fun(self, wrap_const(other))
    return op_fun


def is_agg(column):
    if all(hasattr(col, '_is_agg') for col in column.base_columns):
        column._is_agg = True
    return hasattr(column, '_is_agg')


class CubeObject(_Generative):
    pass


class Measure(CubeObject):
    """A cube Measure."""

    _select_class = ValueSelect

    def __init__(self, name, expression, agg=func.sum):
        self.expression = expression
        self.agg = agg
        self.name = name

    def _apply_agg(self, cuboid, column_clause):
        if self.agg == avg:
            if cuboid.fact_count_column is not None:
                return (func.sum(column_clause * cuboid.fact_count_column) /
                           cast(func.sum(cuboid.fact_count_column),
                               types.Numeric))
        return self.agg(column_clause)

    def select_instance(self, cuboid, *args, **kwargs):
        base = self._select_class(self, *args, **kwargs)
        if is_agg(base.column_clause):
            return base
        if self.agg:
            col = self._apply_agg(cuboid, base.column_clause)
            col._is_agg = True
            return AggregateSelect(self,
                    name=self.name,
                    column_clause=col,
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
        if self.name in cc:
            expr = self.replace_expr(cc[self.name])
            return expr
        return self

    def _score(self, agg):
        return (1, []) if self.name in agg.measures_expr else (-1, [])

    @operator
    def __mul__(self, other):
        name = '%s * %s' % (self.name, other.name)
        return ComputedMeasure(name, operators.mul, (self,
            other))

    @operator
    def __add__(self, other):
        name = '%s + %s' % (self.name, other.name)
        return ComputedMeasure(name, operators.add, (self,
            other))

    @operator
    def __sub__(self, other):
        name = '%s - %s' % (self.name, other.name)
        return ComputedMeasure(name, operators.sub, (self,
            other))

    @operator
    def __div__(self, other):
        name = '%s / %s' % (self.name, other.name)
        return ComputedMeasure(name, operators.div, (self,
            other))

    def over(self, level):
        name = '%s OVER %s' % (self.name, level.name)
        return RelativeMeasure(name, self, level)

    @_generative
    def label(self, name):
        self.name = name

    @_generative
    def aggregate_with(self, agg_fun):
        self.agg = agg_fun

    def percent_over(self, level):
        return (self / self.over(level) * 100)

    @_generative
    def replace_expr(self, expression):
        self.expression = expression


class RelativeMeasure(Measure):

    _select_class = OverSelect

    def __init__(self, name, measure, over_level=None, order_level=None,
            agg=identity_agg, desc=True):
        self.name = name
        self.measure = measure
        self.over_level = over_level
        self.order_level = order_level
        self.agg = agg
        self.inner_agg = self.measure.agg
        self.desc = desc

    def _adapt(self, aggregate):
        over_level = order_level = None
        if self.over_level:
            over_level = self.over_level._adapt(aggregate)
        if self.order_level:
            order_level = self.order_level._adapt(aggregate)
        return RelativeMeasure(self.name, self.measure._adapt(aggregate),
                over_level, order_level)

    def _score(self, aggregate):
        over_score = 0
        order_score = 0
        over_dims = []
        order_dims = []
        if self.over_level is not None:
            over_score, over_dims = self.over_level._score(aggregate)
        if self.order_level is not None:
            order_score, order_dims = self.order_level._score(aggregate)
        measure_score, measure_dims = self.measure._score(aggregate)
        dims = over_dims + order_dims + measure_dims
        if any(score < 0 for score in
                (over_score, order_score, measure_score)):
            return -1, dims
        return sum([over_score, order_score, measure_score]), dims

    def _simplify(self, query):
        cc = ColumnCollection(*query.inner_columns)
        if self.name in cc:
            cc[self.name]._is_agg = self.inner_agg
            return Measure(self.name, cc[self.name], agg=self.agg)
        over_level = order_level = None
        if self.over_level is not None:
            over_level = self.over_level._simplify(query)
        if self.order_level is not None:
            order_level = self.order_level._simplify(query)
        ms = self.measure._simplify(query)
        return RelativeMeasure(self.name, ms,
                over_level, order_level, agg=self.agg, desc=self.desc)

    def _as_selects(self, cuboid):
        over_selects = order_selects = []
        partition = order = None
        measure_selects = self.measure._as_selects(cuboid)
        assert len(measure_selects) == 1
        ms = measure_selects[0]
        if self.over_level:
            over_selects = [sel for sel in self.over_level._as_selects(cuboid)
                    if isinstance(sel, IdSelect)]
            assert len(over_selects) == 1
            partition = over_selects[0].column_clause
            ms.dependencies.append(over_selects[0])
        if self.order_level:
            order_selects = [sel for sel in
                    self.order_level._as_selects(cuboid)
                    if isinstance(sel, (IdSelect, ValueSelect))]
            assert len(order_selects) == 1
            order = order_selects[0].column_clause
            if self.desc:
                order = order.desc()
            ms.dependencies.append(order_selects[0])
        col = self.inner_agg(ms.column_clause)
        col._is_agg = self.inner_agg
        over_expr = over(col,
                partition_by=partition,
                order_by=order)
        over_expr._is_agg = self.agg
        return [self.select_instance(cuboid, column_clause=over_expr,
            name=self.name, dependencies=measure_selects)]


class ConstantMeasure(Measure):

    def __init__(self, constant, agg=identity_agg):
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
        return [self._select_class(self,
            column_clause=col)]

    @_generative
    def replace_expr(self, expression):
        pass


class ComputedMeasure(Measure):

    def __init__(self, name, operator, operands, agg=identity_agg):
        self.name = name
        self.operator = operator
        self.operands = operands
        self.agg = agg

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
                [op._simplify(query) for op in self.operands], self.agg)

    def _score(self, aggregate):
        scores, dims = zip(*[op._score(aggregate) for op in self.operands])
        dims = [d for dim in dims for d in dim]
        return min(scores), dims

    def _as_selects(self, cuboid):
        sub_selects = reduce(
            list.__add__, [op._as_selects(cuboid) for op in
            self.operands], [])
        self_expr = (self.operator(*[(sub.column_clause)
            for sub in sub_selects]).label(self.name))
        if all(hasattr(sub.column_clause, '_is_agg') for sub in sub_selects):
            self_expr._is_agg = self.agg
        return [self.select_instance(cuboid, column_clause=self_expr,
                dependencies=sub_selects,
                name=self.name)]


class CutPoint(CubeObject):
    """Abstract class marking a class as suitable for a CutPoint."""
    pass


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
        sub_operands = [sel for clause in self.operands
                for sel in clause._as_selects(cuboid)
                if not isinstance(sel, LabelSelect)]
        return [self._select_class(self, where_clauses=[self.operator(
            *[sub.column_clause for sub in sub_operands])],
            dependencies=sub_operands)]


class OrFilter(Filter):

    def __init__(self, *operands):
        super(OrFilter, self).__init__(or_, *operands)

    def _adapt(self, aggregate):
        return self.__class__(*[clause._adapt(aggregate)
            for clause in self.operands])

    def _simplify(self, query):
        return self.__class__(*[clause._simplify(query)
            for clause in self.operands])

    def _as_selects(self, cuboid):
        sub_operands = [sel for clause in self.operands
                for sel in clause._as_selects(cuboid)]
        return [self._select_class(self, where_clauses=[or_(
            *[and_(*sub.where_clauses)
                for sub in sub_operands if sub.where_clauses])],
            dependencies=[])]


class PostFilter(Filter):
    _select_class = PostFilterSelect


class Member(CutPoint):
    """A member of a Level. Ex: The year 2010 is a member of the Year level of
    the time dimension."""

    def __init__(self, level, id, label, filter=True):
        self.level = level
        self.id = id
        self.label = label
        self.filter = filter
        self.label_expr = cast(_literal_as_binds(self.label), types.Unicode)

    def _adapt(self, aggregate):
        return Member(self.level._adapt(aggregate), self.id, self.label)

    def _simplify(self, query):
        cc = ColumnCollection(*query.inner_columns)
        if self._label_for_select in cc:
            return Member(self.level._simplify(query), self.id, self.label,
                    filter=False)
        return Member(self.level._simplify(query), self.id, self.label)

    def _as_selects(self, cuboid):
        subs = [sub for sub in self.level._as_selects(cuboid)
                if isinstance(sub, IdSelect)]
        assert len(subs) == 1
        id_expr = subs[0]
        selects = [LabelSelect(self, column_clause=self.label_expr,
                    name=self._label_for_select, is_constant=True)]
        if self.filter:
            selects.append(FilterSelect(self,
                where_clauses=[id_expr.column_clause == self.id],
                dependencies=[id_expr],
                joins=id_expr.joins))
        return selects

    def __eq__(self, other):
        if isinstance(other, Member):
            return (self.id == other.id and
                    self.level == other.level)
        return False

    def _score(self, agg):
        return self.level._score(agg)

    @property
    def _label_for_select(self):
        return self.level._label_for_select

    @property
    def dimension(self):
        """Returns this level dimension."""
        return self.level.dimension


class Level(CutPoint):
    """A level in a dimension hierarchy."""

    def __init__(self, name, dim_column=None, label_column=None,
            label_expr=lambda x: x):
        self.label_column = (label_column if label_column is not None
                else dim_column)
        self.name = name
        self.dim_column = dim_column
        self.label_expr = label_expr
        self.child_level = None
        self.parent_level = None
        self.hierarchy = None

    def bind(self, hierarchy):
        """Late binding of level to hierarchies."""
        self.hierarchy = hierarchy
        levels = hierarchy.levels.values()
        my_idx = levels.index(self)
        if my_idx > 0:
            self.parent_level = levels[my_idx - 1]
        if my_idx + 1 < len(levels):
            self.child_level = levels[my_idx + 1]

    @property
    def dimension(self):
        """Returns this level dimension."""
        return self.hierarchy.dimension

    def __getitem__(self, key):
        values = list(sql_select([self.dim_column,
                self.label_expr(self.label_column)])
                .where(self.dim_column == key)
                .limit(1).execute())[0]
        return Member(self, values[0], values[1])

    def member_by_label(self, label):
        values = list(sql_select([self.dim_column.label('id'),
            self.label_expr(self.label_column).label('label')])
            .where(self.label_expr(self.label_column) == label)
            .limit(1).execute())[0]
        return Member(self, values.id, values.label)

    def _score(self, agg):
        dim = self.dimension
        for agglevel in agg.levels:
            if agglevel.dimension == self.dimension:
                base_level = agglevel
                score = 1
                while(base_level is not None):
                    if base_level == self:
                        return score, [dim]
                    base_level = base_level.parent_level
                    score *= 0.5
        return -1, [dim]

    @property
    def _label_for_select(self):
        return '%s_%s' % (self.dimension.name, self.name)

    @_generative
    def replace_expr(self, expr, label_column=None):
        self.dim_column = expr
        self.child_level = None
        if label_column is not None:
            self.label_column = label_column

    @_generative
    def replace_label_expr(self, label_expr):
        self.label_expr = label_expr

    @_generative
    def replace_level(self, level):
        self.child_level = level

    def _as_selects(self, cuboid):
        sub_selects = []
        sub_joins = []
        if self.child_level:
            sub_selects = self.child_level._as_selects(cuboid)
            sub_joins = [elem for alist in sub_selects
                    for elem in alist.joins]
        return [LabelSelect(self,
            column_clause=self.label_expr(self.label_column),
                    name='%s_label' % self._label_for_select,
                    dependencies=[],
                    joins=sub_joins + [self.dim_column.table,
                        self.label_column.table]),
                IdSelect(self, column_clause=self.dim_column,
                    name=self._label_for_select,
                    dependencies=[],
                    joins=sub_joins + [self.dim_column.table,
                        self.label_column.table])]

    def _adapt(self, aggregate):
        for level in aggregate.levels:
            if level.dimension == self.dimension:
                if level.name == self.name:
                    dim_column = aggregate.levels.get(level)
                    return self.replace_expr(dim_column)
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
            label_col = '%s_label' % self._label_for_select
            if label_col in cc:
                column_expr = cc[label_col]
            else:
                column_expr = dim_expr

            return self.replace_expr(dim_expr, column_expr)
        return self


class ComputedLevel(Level):

    def __init__(self, name, dim_column=None, label_expr=None,
            function=lambda x: x):
        super(ComputedLevel, self).__init__(name, dim_column,
                label_expr=label_expr)
        self.function = function

    @_generative
    def replace_level(self, level):
        self.child_level = level
        self.dim_column = level.dim_column

    def _as_selects(self, cuboid):
        col = self.function(self.dim_column).label(self.name)
        dep = IdSelect(self, column_clause=self.dim_column)
        return [IdSelect(self, name=self._label_for_select, column_clause=col,
            dependencies=[dep]),
            LabelSelect(self, name='%s_label' % self._label_for_select,
                column_clause=self.label_expr(col),
                dependencies=[dep])]

    def member_by_label(self, label):
        values = list(sql_select([self.function(self.dim_column),
            self.label_expr(self.dim_column)])
            .where(self.label_expr(self.dim_column) == label)
            .limit(1).execute())[0]
        return Member(self, values[0], values[1])


class _AllLevel(Level):
    """A dummy, top-level level."""

    def __init__(self, name='All', label='All'):
        self.label = label
        self.name = name
        self.label_expr = _literal_as_binds(self.label)
        self.parent_level = None

    def _as_selects(self, cuboid):
        return [LabelSelect(self, name=self._label_for_select,
            column_clause=self.label_expr, is_constant=True)]

    def _simplifiy(self, query):
        return self


class Hierarchy(object):
    """A dimensions hierarchy."""

    def __init__(self, name, levels):
        self.name = name
        self.levels = [_AllLevel()] + levels
        self.default_member = self.levels[0]
        self.levels = OrderedDict((level.name, level) for level in self.levels)

    def bind(self, dimension):
        """Late binding of this hierarchy to a dimension."""
        self.dimension = dimension
        for level in self.levels.values():
            level.bind(self)

    @property
    def l(self):
        return self.levels


class Dimension(object):
    """A cube dimension."""

    def __init__(self, name, hierarchies):
        self.default_hierarchy = hierarchies[0]
        self.default_member = self.default_hierarchy.default_member
        self.name = name
        for hierarchy in hierarchies:
            hierarchy.bind(self)
        self.hierarchies = {hiera.name: hiera for hiera in hierarchies}

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
        self.scalar_value = None
        self.query = query
        self.orders = query.orders
        super(ResultProxy, self).__init__()
        self.update(self._dims_dict(result))

    def _dims_dict(self, lines):
        result = OrderedDict()
        if len(self.dims) == 0:
            # Just a scalar!
            self.scalar_value = list(lines)[0]
            return result
        dim_key = self.dims[0]._label_for_select
        next_dims = self.dims[1:]
        append = lambda label, lines: ResultProxy(
                self.query.axis(*next_dims), lines, label)

        def key_func(x):
            label_key = '%s_label' % dim_key
            key = getattr(x, dim_key)
            if label_key in x:
                label = x[label_key]
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
        if self.scalar_value:
            return getattr(self.scalar_value, key)
        return super(ResultProxy, self).__getitem__(key)

    def __getattr__(self, key):
        if self.scalar_value is not None:
            return getattr(self.scalar_value, key)
        raise AttributeError('This result is not a scalar')


class OrderClause(CubeObject):

    def __init__(self, measure, reverse=False):
        self.measure = measure
        self.reverse = reverse

    def _score(self, agg):
        return self.measure._score(agg)

    def _adapt(self, agg):
        return OrderClause(self.measure._adapt(agg), self.reverse)

    def _simplify(self, query):
        return OrderClause(self.measure._simplify(query), self.reverse)

    def _as_selects(self, cuboid):
        sub_selects = self.measure._as_selects(cuboid)
        assert len(sub_selects) == 1
        col = sub_selects[0].column_clause
        if self.reverse:
            col = col.desc()
        return [OrderSelect(self, column_clause=col,
                dependencies=sub_selects)]


class Query(_Generative):

    def __init__(self, cuboid, axes, measures):
        self.cuboid = cuboid
        self.axes = axes
        self.measures = measures
        self.filters = []
        self.orders = []

    def _generate(self):
        newself = super(Query, self)._generate()
        newself.filters = list(self.filters)
        newself.orders = list(self.orders)
        newself.axes = list(self.axes)
        newself.measures = list(self.measures)
        return newself

    def _as_sql(self):
        agg_scores = ((agg, agg.score(self.parts))
                for agg in self.cuboid.aggregates)
        best_agg, score = reduce(lambda (x, scorex), (y, scorey): (x, scorex)
                if scorex >= scorey
                else (y, scorey), agg_scores, (self.cuboid, 0))
        query = self._adapt(best_agg)
        things = query.parts
        selects = [sel  for t in things for sel in t._as_selects(best_agg)]
        query = sql_select([], query.cuboid.selectable)
        return compile(selects, query, best_agg)

    @property
    def parts(self):
        return self.axes + self.measures + self.filters + self.orders

    @_generative
    def _adapt(self, agg):
        if agg != self.cuboid:
            self.axes = [axis._adapt(agg) for axis in self.axes]
            self.measures = [measure._adapt(agg) for measure in
                    self.measures]
            self.filters = [filter._adapt(agg) for filter in self.filters]
            self.orders = [order._adapt(agg) for order in self.orders]
            self.cuboid = agg

    def __eq__(self, other):
        if isinstance(other, Query):
            return (self.cuboid == other.cuboid and
                    self.axes == other.axes and
                    self.measures == other.measures and
                    self.filters == other.filters)
        return False

    @_generative
    def filter(self, *members):
        if len(members) > 1:
            member = OrFilter(*members)
        else:
            member = members[0]
        self.filters.append(member)

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
        name = 'RANK OVER %s' % expr.name
        fun = func.dense_rank()
        fun._is_agg = True
        measure = RelativeMeasure(name, ConstantMeasure(fun,
                agg=lambda x: x),
                order_level=expr,
                over_level=partition_by,
                desc=True)
        self.filters.append(PostFilter(operators.le,
            measure, ConstantMeasure(n)))
        self.orders.append(OrderClause(measure))

    def execute(self):
        return ResultProxy(self, self._as_sql().execute())


class Aggregate(object):

    def __init__(self, selectable, levels, measures, fact_count_column):
        self.selectable = selectable
        self.measures_expr = OrderedDict((measure.name, expr)
                for measure, expr in measures.items())
        self.measures = OrderedDict((measure.name, measure)
                for measure, expr in measures.items())
        self.levels = levels
        self.fact_count_column = fact_count_column

    def score(self, things):
        scores, dims = zip(*[thing._score(self) for thing in things])
        if any(score < 0 for score in scores):
            return -100
        dims = len(set(d.name for dim in dims for d in dim))
        self_dims = len(set(l.dimension.name for l in self.levels))
        return sum(scores) + 0.3 * (dims - self_dims)

    def find_expression(self, measure):
        return self.measures_expr.get(measure.name, measure.expression)

    def find_level(self, level):
        for agglevel in self.levels:
            if agglevel.dimension == level.dimension:
                expression = self.levels.get(agglevel)
                return level.column(expression)
        raise ValueError("Aggregate (%s) is not suitable for level %s[%s]" %
                (self.table.name, level.dimension.name, level.name))


class Cube(object):

    def __init__(self, metadata, fact_table, dimensions, measures,
            aggregates=None, fact_count_column=None):
        self.dimensions = OrderedDict((dim.name, dim) for dim in dimensions)
        self.measures = OrderedDict((measure.name, measure) for measure in
                measures)
        self.table = fact_table
        self.aggregates = aggregates or []
        self.fact_count_column = fact_count_column

    @property
    def query(self):
        return Query(self, [dim.default_member for dim in
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
