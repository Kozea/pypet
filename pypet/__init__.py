from sqlalchemy.sql import (func, util as sql_util, over, operators,
        literal_column, select)
from sqlalchemy.util import OrderedSet
from sqlalchemy.sql.expression import (
        ColumnClause,
        _Generative, _generative, _literal_as_binds)
from collections import OrderedDict
from itertools import groupby
from functools import wraps


class DictOrListMixin(object):
    """Simple mixin allowing to access a particular 'children' attribute as
    either a list or a dictionary indexed by name."""

    def __getitem__(self, key):
        if isinstance(key, int):
            return self._children[key]
        return self._children_dict.get(key, Member(key, self))

    @property
    def _children(self):
        """Returns the children as a list."""
        return getattr(self, self._children_attr)

    @property
    def _children_dict(self):
        """Returns the children as a dict."""
        return {child.name: child for child in self._children}


def operator(fun):
    @wraps(fun)
    def op_fun(self, other):
        if not isinstance(other, Measure):
            if not isinstance(other, ColumnClause):
                other = ConstantMeasure(other)
            else:
                other = Measure(str(other), other,
                    agg=lambda x: x)
        return fun(self, other)
    return op_fun




class Measure(_Generative):
    """A cube Measure."""

    def __init__(self, name, expression, agg=func.sum):
        self.expression = expression
        self.agg = agg
        self.name = name

    def _raw_expression(self, aggregate):
        expression = aggregate.find_expression(self)
        return expression.label(self.name)

    def aggregate(self, aggregate):
        """Returns the aggregated expression of this measure."""
        expr = self._raw_expression(aggregate)
        return self.agg(expr).label(self.name)

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
        self.agg = agg_fun or (lambda x: x)

    @_generative
    def replace_expr(self, expression):
        self.expression = expression


class RelativeMeasure(Measure):

    def __init__(self, name, measure, over_level):
        self.name = name
        self.measure = measure
        self.agg = func.avg
        self.over_level = over_level

    def _raw_expression(self, aggregate):
        over_expr = aggregate.find_level(self.over_level)
        expression = self.measure.aggregate(aggregate)

        return over(expression, partition_by=over_expr)


class ConstantMeasure(Measure):

    def __init__(self, constant):
        self.constant = constant
        self.agg = lambda x: x

    @property
    def name(self):
        return str(self.constant)

    def _raw_expression(self, aggregate):
        return _literal_as_binds(self.constant)

    @_generative
    def replace_expr(self, expression):
        pass


class ComputedMeasure(Measure):

    def __init__(self, name, operator, operands, agg=lambda x:x):
        self.name = name
        self.operator = operator
        self.operands = operands
        self.agg = agg

    def _raw_expression(self, aggregate):
        expression = self.operator(*(op.aggregate(aggregate)
            for op in self.operands))
        return expression.label(self.name)


class CutPoint(object):
    """Abstract class marking a class as suitable for a CutPoint."""
    pass


class Member(CutPoint):
    """A member of a Level. Ex: The year 2010 is a member of the Year level of
    the time dimension."""

    def __init__(self, name, level):
        self.name = name
        self.level = level
        self.label_expr = lambda x: _literal_as_binds(name)

    def _add_to_query(self, query, cuboid, groupby=True, label=None):
        """Appends this member to the query as a cutpoint.

        This is equivalent to a filter.
        """
        label = label or self.dimension.name
        label_expr, expression = cuboid.find_level(self.level)
        return (query.column(self.label_expr(expression).label(label))
                    .where(expression == self.name))

    @property
    def dimension(self):
        """Returns this level dimension."""
        return self.level.dimension


class Level(CutPoint):
    """A level in a dimension hierarchy."""

    def __init__(self, name, dim_column=None, label_expr=lambda x:x):
        self.name = name
        self.dim_column = dim_column
        self.label_expr = label_expr
        self.child_level = None
        self.parent_level = None
        self.hierarchy = None

    def bind(self, hierarchy):
        """Late binding of level to hierarchies."""
        self.hierarchy = hierarchy
        my_idx = hierarchy.levels.index(self)
        if my_idx > 0:
            self.parent_level = hierarchy.levels[my_idx - 1]
        if my_idx + 1 < len(hierarchy.levels):
            self.child_level = hierarchy.levels[my_idx + 1]

    @property
    def dimension(self):
        """Returns this level dimension."""
        return self.hierarchy.dimension

    def _add_to_query(self, query, cuboid, groupby=True, label=None):
        """Appends this level to a query as a cutpoint.
        """
        label = label or self.dimension.name
        label_expr, expression = cuboid.find_level(self)
        if expression == self.dim_column:
            query = self._join(query, cuboid.selectable)
        query = (query.column(label_expr.label(label))
                .correlate(query))
        if groupby:
            query = query.group_by(expression)
        return query

    def _join(self, query, left):
        """Recursively builds a join against this level's dimensions table."""
        if not hasattr(self.dim_column, 'table'):
            return query
        if self.child_level:
            query = self.child_level._join(query, self.dim_column.table)
        # Test if our dimension table is already in the joins
        replace_clause_index, orig_clause = sql_util.find_join_source(
                                                query._froms,
                                                self.dim_column.table)
        if orig_clause is not None:
            # The 'left' table is already in the froms, don't do anything about
            # it.
            return query
        # Test if the left side is in the query: then, replace it with our side
        # of the join
        replace_clause_index, orig_clause = sql_util.find_join_source(
                                                query._froms,
                                                left)
        if query._from_obj:
            froms = query._from_obj
            base_clause = froms[0]
        else:
            # It's a very simple query yet
            froms = query._froms
            base_clause = froms[replace_clause_index]
        if orig_clause is not None:
            # Replace the query
            query._from_obj = OrderedSet(
                    query._from_obj[:replace_clause_index] +
                    [(base_clause.join(self.dim_column.table))] +
                    query._from_obj[replace_clause_index + 1:])
        else:
            base_clause = froms[0]
            clause = base_clause.join(self.dim_column.table).join(left)
            if len(query._froms) > 1:
                rest = tuple(query._from_obj[1:])
            else:
                rest = tuple()
            query._from_obj = OrderedSet((clause,) + rest)
        return query

    def column(self, base):
        return self.label_expr(self.dim_column), self.dim_column

    def __getitem__(self, key):
        """Item access for query construction."""
        if self.child_level:
            if key == self.child_level.name:
                return self.child_level
            else:
                return Member(key, self.child_level)
        else:
            raise ValueError('Cannot access item on last level')


class ComputedLevel(Level):

    def __init__(self, name, dim_column=None, label_func=None, function=lambda
            x:x):
        super(ComputedLevel, self).__init__(name, dim_column, )
        self.function = function

    def column(self, base):
        col = self.function(base)
        return self.label_expr(col), col


class _AllLevel(Level):
    """A dummy, top-level level."""

    def __init__(self, name='All', label='All'):
        self.label = label
        self.name = name
        self.label_expr = lambda x: _literal_as_binds(label)
        self.parent_level = None

    def _add_to_query(self, query, cuboid, groupby=True, label=None):
        label = label or self.dimension.name
        return query.column(self.label_expr(None).label(label))

    def column(self, base):
        return self.label_expr(base), self.label_expr


class Hierarchy(DictOrListMixin):
    """A dimensions hierarchy."""

    _children_attr = 'levels'

    def __init__(self, name, levels):
        self.name = name
        self.levels = [_AllLevel()] + levels
        self.default_member = self.levels[0]

    def bind(self, dimension):
        """Late binding of this hierarchy to a dimension."""
        self.dimension = dimension
        for level in self.levels:
            level.bind(self)


class Dimension(DictOrListMixin):
    """A cube dimension."""

    _children_attr = 'default_levels'

    def __init__(self, name, hierarchies):
        self.name = name
        self.hierarchies = hierarchies
        for hierarchy in hierarchies:
            hierarchy.bind(self)
        self.default_member = self.hierarchies[0].default_member

    @property
    def default_levels(self):
        return self.hierarchies[0].levels


class ResultProxy(OrderedDict):

    def __init__(self, dims, result):
        self.dims = dims
        self.scalar_value = None
        super(ResultProxy, self).__init__()
        self.update(self._dims_dict([dim.name for dim in self.dims.keys()],
            result))

    def _dims_dict(self, dims, lines):
        result = OrderedDict()
        if len(dims) == 0:
            # Just a scalar!
            self.scalar_value = list(lines)[0]
            return result
        dim_key = dims[0]
        key_func = lambda x: getattr(x, dim_key)
        if len(dims) > 1:
            next_dims = dims[1:]
            append = lambda lines: self._dims_dict(next_dims, lines)
        else:
            append = lambda lines: lines[0]
        for key, lines in groupby(sorted(lines, key=key_func), key_func):
            result[key] = append(list(lines))
        return result

    def __getattr__(self, key):
        if self.scalar_value is not None:
            return getattr(self.scalar_value, key)
        raise AttributeError('This result is not a scalar')


class Query(_Generative):

    def __init__(self, cuboid, cuts, measures):
        self.cuboid = cuboid
        self.cuts = cuts
        self.measures = measures

    def _as_sql(self):
        agg_scores = ((agg, agg.score(self.cuts.values()))
                for agg in self.cuboid.aggregates)
        best_agg, score = reduce(lambda (x, scorex), (y, scorey): (x, scorex)
                if scorex >= scorey
                else (y, scorey), agg_scores, (self.cuboid, 0))
        cuboid = CubeProxy(best_agg, self.cuts, self.measures)
        return cuboid.selectable

    @_generative
    def slice(self, level):
        assert isinstance(level, CutPoint), ("You must slice on a CutPoint"
            "(a level or a member, not a %s" % level.__class__.__name__)
        self.cuts[level.dimension] = level

    @_generative
    def measure(self, *measures):
        self.measures = OrderedDict((measure.name, measure)
                for measure in measures)

    @_generative
    def axis(self, *axes):
        self.cuts = OrderedDict((axis.dimension, axis)
                for axis in axes)

    def execute(self):
        return ResultProxy(self.cuts, self._as_sql().execute())


class CubeProxy(object):

    def __init__(self, cuboid, levels, measures):
        self.cuboid = cuboid
        self.selectable = self.cuboid.selectable
        self.selects = []
        self.base_query = None
        self.need_subquery = False
        self.measures = OrderedDict()
        self.rel_selects = []
        self._joined_levels = []
        for key, measure in measures.items():
            self.measures[key] = self._extract_relative_measure(measure)
        self.levels = levels
        # Clear out all columns, but keep the from clause
        self.selectable = (self.selectable.select()
                            .with_only_columns([])
                            .select_from(self.selectable))
        for member in self._joined_levels:
            self.selectable = member._join(self.selectable,
                    self.selectable)

        if self.need_subquery:
            for dim, member in self.levels.items():
                self.selectable = member._add_to_query(self.selectable, self,
                        groupby=False)
            for column in (self.selects + self.rel_selects):
                self.selectable = self.selectable.column(column).correlate(self.selectable)
            self.base_query = self.selectable
            self.selectable = select(columns=[], whereclause=None,
                    from_obj=[self.base_query.alias()])
            for dim, member in self.levels.items():
                self.selectable = (self.selectable
                        .column(self.base_query.c[dim.name])
                        .group_by(self.base_query.c[dim.name])
                        .correlate(self.base_query))
        else:
            for dim, member in self.levels.items():
                self.selectable = member._add_to_query(self.selectable,
                        self.cuboid,
                        groupby=True).correlate(self.selectable)
            self.measures = measures
        for key, measure in self.measures.items():
            self.selectable = self.selectable.column(measure.aggregate(self))
        self.selectable = self.selectable.correlate(self.base_query)

    def _extract_relative_measure(self, measure):
        if isinstance(measure, ComputedMeasure):
            new_operands = []
            for operand in measure.operands:
                op = self._extract_relative_measure(operand)
                if op.name in [a.name for a in self.selects]:
                    new_operands.append(op.replace_expr(literal_column('"%s"' %
                        op.name)))
                else:
                    new_operands.append(op)
            temp_measure = ComputedMeasure(measure.name, measure.operator,
                    new_operands, measure.agg)
            select = temp_measure._raw_expression(self)
            return temp_measure
        elif isinstance(measure, RelativeMeasure):
            self.need_subquery = True
            self._joined_levels.append(measure.over_level)
            select = measure._raw_expression(self).label(measure.name)
            self.rel_selects.append(select)
            return Measure(measure.name, literal_column('"%s"' % measure.name),
                    agg=measure.agg)
        else:
            # "Raw" measure, no need to make a fuss
            self.selects.append(measure._raw_expression(self.cuboid).label(measure.name))
            return measure

    @property
    def selects_dict(self):
        return {a.name: a for a in self.selects}

    def find_expression(self, measure):
        if measure.name in self.measures:
            return self.measures[measure.name]._raw_expression(self.cuboid)
        elif self.base_query is not None and measure.name in self.base_query.c:
            return self.base_query.c[measure.name]
        elif measure.name in self.selectable.c:
            return self.selectable.c[measure.name]
        else:
            return self.cuboid.find_expression(measure)

    def find_level(self, level):
        return self.cuboid.find_level(level)


class Aggregate(object):

    def __init__(self, selectable, levels, measures):
        self.selectable = selectable
        self.measures_expr = OrderedDict((measure.name, expr) for measure, expr in
            measures.items())
        self.measures = OrderedDict((measure.name, measure) for measure, expr in
            measures.items())
        self.levels = levels

    def _score(self, level):
        for agglevel in self.levels:
            if agglevel.dimension == level.dimension:
                base_level = agglevel
                score = 1
                while(base_level is not None):
                    if base_level == level:
                        return score
                    base_level = base_level.parent_level
                    score *= 0.5
        return -1

    def score(self, levels):
        levels = [level if isinstance(level, Level) else level.level
                for level in levels]
        scores = [self._score(level) for level in levels]
        if any(score < 0 for score in scores):
            return -1
        if len(self.levels) < len(levels):
            return -1
        return sum(scores) - (0.3 *
            (len(self.levels) - len(levels)))

    def find_expression(self, measure):
        return self.measures_expr.get(measure.name, measure.expression)

    def find_level(self, level):
        for agglevel in self.levels:
            if agglevel.dimension == level.dimension:
                expression = self.levels.get(agglevel)
                return level.column(expression)
        raise ValueError("Aggregate (%s) is not suitable for level %s[%s]" %
                (self.table.name, level.dimension.name, level.name))


class Cube(DictOrListMixin):

    _children_attr = 'levels'

    def __init__(self, metadata, fact_table, dimensions, measures,
            aggregates=None):
        self.dimensions = dimensions
        self.measures = OrderedDict((measure.name, measure) for measure in
                measures)
        self.table = fact_table
        self.aggregates = aggregates or []

    @property
    def query(self):
        return Query(self, OrderedDict([(dim, dim.default_member) for dim in
            self.dimensions]),
            self.measures)

    @property
    def selectable(self):
        return self.table

    @property
    def levels(self):
        return [dim.default_member for dim in self.dimensions]

    @property
    def _children_dict(self):
        return {dim.name: dim.default_member for dim in self.dimensions}

    def find_expression(self, measure):
        return measure.expression

    def find_level(self, level):
        return level.column(level.dim_column)

    def __getitem__(self, key):
        if isinstance(key, int):
            return self._children[key]
        member = self._children_dict.get(key, None)
        if member is None:
            raise KeyError('No dimension named %s' % key)
        return member
