from sqlalchemy.sql import func, util as sql_util
from sqlalchemy.util import OrderedSet
from sqlalchemy.sql.expression import (
        _Generative, _generative, _literal_as_binds)
from collections import OrderedDict
from itertools import groupby


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


class Measure(object):
    """A cube Measure."""

    def __init__(self, name, expression, agg=func.sum):
        self.expression = expression
        self.agg = agg
        self.name = name

    @property
    def aggregate(self):
        """Returns the aggregated expression of this measure."""
        return self.agg(self.expression).label(self.name)


class CutPoint(object):
    """Abstract class marking a class as suitable for a CutPoint."""
    pass


class Member(CutPoint):
    """A member of a Level. Ex: The year 2010 is a member of the Year level of
    the time dimension."""

    def __init__(self, name, level):
        self.name = name
        self.level = level
        self.label_expr = _literal_as_binds(name)

    def _add_to_query(self, query, cube_query):
        """Appends this member to the query as a cutpoint.

        This is equivalent to a filter.
        """
        return (query.column(self.label_expr.label(self.level.dimension.name))
                    .where(self.level.dim_column == self.name))

    @property
    def dimension(self):
        """Returns this level dimension."""
        return self.level.dimension


class Level(CutPoint):
    """A level in a dimension hierarchy."""

    def __init__(self, name, dim_column=None, label_expr=None):
        self.name = name
        self.dim_column = dim_column
        self.label_expr = label_expr if label_expr is not None else dim_column
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

    def _add_to_query(self, query, cube_query):
        """Appends this level to a query as a cutpoint.
        """
        query = self._join(query, cube_query.cuboid.selectable())
        return (query.column(self.dim_column.label(self.dimension.name))
                .group_by(self.dim_column))

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
        if orig_clause is not None:
            # Replace the query
            query._from_obj = OrderedSet(
                    query._from_obj[:replace_clause_index] +
                    [(query._from_obj[replace_clause_index]
                            .join(self.dim_column.table))] +
                    query._from_obj[replace_clause_index + 1:])
        else:
            if query._from_obj:
                base_clause = query._from_obj[0]
            else:
                # It's a very simple query yet
                base_clause = query._froms[0]
            clause = base_clause.join(self.dim_column.table).join(left)
            if len(query._froms) > 1:
                rest = tuple(query._from_obj[1:])
            else:
                rest = tuple()
            query._from_obj = OrderedSet((clause,) + rest)
        return query

    def __getitem__(self, key):
        """Item access for query construction."""
        if self.child_level:
            if key == self.child_level.name:
                return self.child_level
            else:
                return Member(key, self.child_level)
        else:
            raise ValueError('Cannot access item on last level')


class _AllLevel(Level):
    """A dummy, top-level level."""

    def __init__(self, name='All', label='All'):
        self.label = label
        self.name = name
        self.label_expr = _literal_as_binds(label)

    def _add_to_query(self, query, cube_query):
        return query.column(self.label_expr.label(self.dimension.name))


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
        super(ResultProxy, self).__init__()
        self.update(self._dims_dict([dim.name for dim in self.dims.keys()],
            result))

    def _dims_dict(self, dims, lines):
        dim_key = dims[0]
        result = OrderedDict()
        key_func = lambda x: getattr(x, dim_key)
        if len(dims) > 1:
            next_dims = dims[1:]
            append = lambda lines: self._dims_dict(next_dims, lines)
        else:
            append = lambda lines: lines[0]
        for key, lines in groupby(sorted(lines, key=key_func), key_func):
            result[key] = append(list(lines))
        return result


class Query(_Generative):

    def __init__(self, cuboid, cuts, measures):
        self.cuboid = cuboid
        self.cuts = cuts
        self.measures = measures

    def _as_sql(self):
        query = self.cuboid.selectable().select()
        query = query.with_only_columns([measure.aggregate
            for measure in self.measures])
        for dim, member in self.cuts.items():
            query = member._add_to_query(query, self)
        return query

    @_generative
    def slice(self, level):
        assert isinstance(level, CutPoint), ("You must slice on a CutPoint"
            "(a level or a member, not a %s" % level.__class__.__name__)
        self.cuts[level.dimension] = level

    def execute(self):
        return ResultProxy(self.cuts, self._as_sql().execute())


class Cube(DictOrListMixin):

    _children_attr = 'levels'

    def __init__(self, metadata, fact_table, dimensions, measures):
        self.dimensions = dimensions
        self.measures = measures
        self.table = fact_table

    @property
    def query(self):
        return Query(self, OrderedDict([(dim, dim.default_member) for dim in
            self.dimensions]),
            self.measures)

    def selectable(self):
        return self.table

    @property
    def levels(self):
        return [dim.default_member for dim in self.dimensions]

    @property
    def _children_dict(self):
        return {dim.name: dim.default_member for dim in self.dimensions}

    def __getitem__(self, key):
        if isinstance(key, int):
            return self._children[key]
        member = self._children_dict.get(key, None)
        if member is None:
            raise KeyError('No dimension named %s' % key)
        return member
