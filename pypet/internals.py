from sqlalchemy.sql import (util as sql_util,
        ColumnCollection)
from sqlalchemy.util import OrderedSet
from sqlalchemy.sql.expression import (
        and_, _Generative, _generative, func, Join)
from operator import and_ as builtin_and
from functools import reduce


def find_join(_from, table):
    if isinstance(_from, Join):
        join = find_join(_from.left, table)
        if join is not None:
            _from.left = join
            return _from
        join = find_join(_from.right, table)
        if join is not None:
            _from.right = join
            return _from
    for fk in table.foreign_keys:
        if fk.references(_from):
            return _from.join(table)
    for fk in _from.foreign_keys:
        if fk.references(table):
            return _from.join(table)



def join_table_with_query(query, table):
    """Find a join between a query and a table, modifying the from clause in
    place."""
    # Check if the join is needed.
    _, orig_clause = sql_util.find_join_source(
                                            query._froms,
                                            table)
    if orig_clause is not None:
        # The join is already in the query
        return
    replace_clause_index = None
    found = False
    for index, _from in enumerate(query._froms):
        replacement = find_join(_from, table)
        if replacement is not None:
            query._from_obj = OrderedSet(
                query._from_obj[:index] +
                [replacement] +
                query._from_obj[index + 1:])
            found = True
            break
    if not found:
        raise ValueError('Cannot find join between %s and %s' % (table,
                                                                 query))
    return

class Select(_Generative):

    def __init__(self, comes_from, column_clause=None, name=None,
            dependencies=None, joins=None, where_clause=None,
            is_constant=False, need_groups=None):
        self.column_clause = column_clause
        self.comes_from = comes_from
        self.name = name
        self.dependencies = dependencies or []
        self.joins = joins or []
        self.where_clause = where_clause
        self.is_constant = is_constant
        self.need_groups = need_groups or []

    def _trim_dependency(self, query):
        _froms_col = [col for _from in query._froms for col in _from.c]
        for dep in self.dependencies:
            if isinstance(dep, (AggregateSelect, OverSelect)):
                continue
            dep._trim_dependency(query)
            if any(col.key == dep.name for col in
                    query.inner_columns) or (any(col.key ==
                        dep.name for col in _froms_col)):
                self.dependencies.remove(dep)

    def simplify(self, query, cuboid):
        new_selects = self.comes_from._simplify(query)._as_selects(cuboid)
        for select in new_selects:
            select._trim_dependency(query)
        return new_selects

    def depth(self):
        sub_depth = max([0] + [sub.depth() for sub in self.dependencies])
        if self.need_subquery():
            return sub_depth + 1
        return sub_depth

    @_generative
    def rename(self, name):
        self.name = name

    def need_column(self, column):
        return self.name == column.key or any(dep.need_column(column)
                for dep in self.dependencies)

    def _append_join(self, query, **kwargs):
        for join in self.joins:
            join_table_with_query(query, join)
        return query

    def _replace_column(self, query, column):
        columns = ColumnCollection(*query.inner_columns)
        columns.replace(column)
        return query.with_only_columns(columns)

    def _append_column(self, query, **kwargs):
        if self.column_clause is not None:
            return self._replace_column(query,
                        self.column_clause.label(self.name))
        return query

    def _append_where(self, query, **kwargs):
        if self.where_clause is not None and not self.need_subquery():
            return query.where(self.where_clause)
        return query

    def _append_to_query(self, query, **kwargs):
        query = self._append_where(query, **kwargs)
        query = self._append_column(query, **kwargs)
        query = query.group_by(*self.need_groups)
        return query

    def visit(self, fun):
        for dep in self.dependencies:
            dep.visit(fun)
        fun(self)

    def need_subquery(self):
        return any(isinstance(dep, (AggregateSelect, OverSelect))
                for dep in self.dependencies)


class ValueSelect(Select):

    def need_subquery(self):
        return any(isinstance(dep, (AggregateSelect, OverSelect))
                or dep.need_subquery()
                for dep in self.dependencies)

    def _append_column(self, query, **kwargs):
        if kwargs['in_group'] and not getattr(self.column_clause, '_is_agg',
                False):
            agg = self.comes_from.agg or func.max
            col = agg(self.column_clause).label(self.name)
            return self._replace_column(query, col)
        else:
            return super(ValueSelect, self)._append_column(query)


class AggregateSelect(ValueSelect):

    def _append_column(self, query, **kwargs):
        return self._replace_column(query,
                    self.column_clause.label(self.name))

    def _trim_dependency(self, query):
        pass


class OverSelect(Select):

    def _append_column(self, query, **kwargs):
        col = self.column_clause.label(self.name)
        if hasattr(self.column_clause, '_is_agg'):
            col._is_agg = self.column_clause._is_agg
        query = self._replace_column(query, col)
        if kwargs['in_group']:
            for attr in ('order_by', 'partition_by'):
                value = getattr(self.column_clause, attr)
                if value is not None:
                    # Remove "DESC" OR "ASC" from the column clause.
                    if hasattr(value, 'clauses'):
                        clauses = value.clauses
                    else:
                        clauses = [value]
                    for cl in clauses:
                        while hasattr(cl, 'element'):
                            cl = cl.element
                        cl._keep_group = True
                        query = query.group_by(cl)
            for clause in self.column_clause.func.base_columns:
                if hasattr(clause, '_is_agg'):
                    clauses = list(clause.clauses)
                else:
                    clauses = [clause]
                for cl in clauses:
                    cl._keep_group = True
                    query = query.group_by(cl)
        return query


class GroupingSelect(Select):

    def _append_column(self, query, **kwargs):
        query = super(GroupingSelect, self)._append_column(query, **kwargs)
        if kwargs['in_group'] and not self.is_constant:
            query = query.group_by(self.column_clause)
        return query


class IdSelect(GroupingSelect):
    pass


class LabelSelect(GroupingSelect):
    pass


class FilterSelect(Select):

    def __init__(self, *args, **kwargs):
        super(FilterSelect, self).__init__(*args, **kwargs)
        self.embedded = False

    def _append_where(self, query, **kwargs):
        if self.embedded:
            return query
        else:
            return super(FilterSelect, self)._append_where(query, **kwargs)

    def _contains_where(self, whereclause):
        """Isolates components of a where clause.


        Returns true if this filter whereclause is already contained in the
        given whereclause
        """
        while hasattr(whereclause, 'element'):
            whereclause = whereclause.element
        if whereclause is self.where_clause:
            return True
        elif getattr(whereclause, 'operator', None) == builtin_and:
            return any(self._contains_where(clause)
                for clause in whereclause.get_children())
        return False

    def simplify(self, query, cuboid):
        for _from in query._froms:
            while(hasattr(_from, 'element')):
                _from = _from.element
            if self._contains_where(getattr(_from, '_whereclause', None)):
                return []
        return super(FilterSelect, self).simplify(query, cuboid)

    def need_subquery(self):
        return any(isinstance(dep, (AggregateSelect, OverSelect))
                for dep in self.dependencies) or any(
                        dep.need_subquery() for dep in self.dependencies)


class OrderSelect(Select):

    def __init__(self, comes_from, reverse=False, **kwargs):
        super(OrderSelect, self).__init__(comes_from, **kwargs)
        self.reverse = reverse

    def need_subquery(self):
        return any(d.need_subquery() or isinstance(d, OverSelect)
                   for d in self.dependencies)

    def _append_column(self, query, **kwargs):
        sort_col = self.column_clause
        if self.reverse:
            sort_col = sort_col.desc()
        else:
            sort_col = sort_col.asc()
        query = query.order_by(sort_col)
        if kwargs['in_group']:
            if not getattr(self.column_clause, '_is_agg', False):
                self.column_clause._keep_group = True
                query = query.group_by(self.column_clause)
        return query


class PostFilterSelect(FilterSelect):
    pass


def by_class(selects):
    selects_dicts = {clz: [] for clz in (
        ValueSelect, LabelSelect, IdSelect, FilterSelect, OverSelect,
        PostFilterSelect, AggregateSelect, OrderSelect)}
    for select in selects:
        selects_dicts[select.__class__].append(select)
    return selects_dicts


def process_selects(query, selects, **kwargs):
    for select in selects:
        query = select._append_join(query, **kwargs)
    for select in selects:
        query = select._append_to_query(query, **kwargs)
    return query


def compile(selects, query, cuboid, level=0):
    if level > 10:
        raise Exception('Not convergent query, abort, abort!')
    simples = [sel for sub in selects for sel in
                sub.simplify(query, cuboid)]
    subqueries = {}
    tags = {}

    def visit_sub(dep):
        select_list = subqueries.setdefault(dep.depth(), [])
        select_list.append(dep)
        for sub in dep.dependencies:
            current_tag = tags.setdefault(sub, 0)
            tags[sub] = current_tag + 1
    for select in simples:
        select.visit(visit_sub)
    subqueries = [sorted(val, key=lambda x: -tags.get(x, 0))
        for _, val in sorted(list(subqueries.items()), key=lambda x: x[0])]
    values = subqueries[0]

    kwargs = {'in_group': False}
    if any(isinstance(a, AggregateSelect) for a in values):
        kwargs['in_group'] = True
        if any(isinstance(a, OverSelect) and not a.need_groups for a in values):
            kwargs['in_group'] = False
            for a in list(values):
                if isinstance(a, AggregateSelect):
                    values.remove(a)
                    values.extend(a.dependencies)
    if any(getattr(a, 'need_groups', []) for a in values):
        kwargs['in_group'] = True
    idx = 0
    query = process_selects(query, values, **kwargs)
    columns_to_keep = []
    group_bys = []
    for column in list(query.inner_columns):
        if ((column.key in [sel.name for sel in selects]) or
            any(sub.need_column(column)
                    for sub in reduce(list.__add__, subqueries[idx + 1:],
                        []))):
                columns_to_keep.append(column)
        if hasattr(column, 'partition_by'):
            group_bys.append(column.partion_by)
    for column in query._group_by_clause:
        if any(col.shares_lineage(column) for col in columns_to_keep):
            group_bys.append(column)
        elif hasattr(column, '_keep_group'):
            group_bys.append(column)
    if len(subqueries) > 1:
        for column in query._order_by_clause:
            column = column.element
            if column.key not in [c.key for c in columns_to_keep]:
                columns_to_keep.append(column)
    query = query.with_only_columns(columns_to_keep)
    query._group_by_clause = []
    query = query.group_by(*set(group_bys))
    if len(subqueries) > 1:
        query = query.alias().select()
        if cuboid.fact_count_column is not None:
            cuboid = cuboid._generate()
            new_fc = None
            for col in query.inner_columns:
                if col.name == cuboid.fact_count_column_name:
                    new_fc = col
            cuboid.fact_count_column = new_fc
        return compile(simples, query, cuboid, level=level + 1)
    return query
