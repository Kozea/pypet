from sqlalchemy.sql import (util as sql_util,
        ColumnCollection)
from sqlalchemy.util import OrderedSet
from sqlalchemy.sql.expression import (
        and_, _Generative, _generative, func, over)


class Select(_Generative):

    def __init__(self, comes_from, column_clause=None, name=None,
            dependencies=None, joins=None, where_clauses=None, agg=None,
            is_constant=False, force_subquery=False):
        self.column_clause = column_clause
        self.comes_from = comes_from
        self.name = name
        self.dependencies = dependencies or []
        self.joins = joins or []
        self.where_clauses = where_clauses or []
        self.agg = agg
        self.is_constant = is_constant
        self.force_subquery = force_subquery

    def simplify(self, query):
        new_selects = self.comes_from._simplify(query)._as_selects()
        _froms_col = [col for _from in query._froms for col in _from.c]
        for select in new_selects:
            for dependency in list(select.dependencies):
                if any(col.key == dependency.name for col in
                        query.inner_columns) or (any(col is
                            dependency.column_clause for col in _froms_col)):
                    select.dependencies.remove(dependency)
        return new_selects

    def depth(self):
        sub_depth = max([0] + [sub.depth() for sub in self.dependencies])
        if any(dep.need_subquery() for dep in self.dependencies):
            return sub_depth + 1
        return sub_depth

    @_generative
    def rename(self, name):
        self.name = name

    def need_column(self, column):
        return self.name == column.key or any(dep.need_column(column)
                for dep in self.dependencies)

    def need_subquery(self):
        return (self.force_subquery or
            any(sub.need_subquery() for sub in self.dependencies))

    def _append_join(self, query, **kwargs):
        for join in self.joins:
            # Check if the join is needed.
            _, orig_clause = sql_util.find_join_source(
                                                    query._froms,
                                                    join)
            if orig_clause is not None:
                # The join is already in the query
                continue
            replace_clause_index = None
            for index, _from in enumerate(query._froms):
                for fk in _from.foreign_keys:
                    if fk.references(join):
                        replace_clause_index, orig_clause = index, _from
                        break
                # Replace the query
            if replace_clause_index is None:
                raise ValueError('Cannot find join between %s and %s' % (join,
                    query))
            query._from_obj = OrderedSet(
                    query._from_obj[:replace_clause_index] +
                    [(orig_clause.join(join))] +
                    query._from_obj[replace_clause_index + 1:])
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
        return query.where(and_(*self.where_clauses))

    def _append_to_query(self, query, **kwargs):
        query = self._append_where(query, **kwargs)
        query = self._append_column(query, **kwargs)
        return query

    def visit(self, fun):
        for dep in self.dependencies:
            dep.visit(fun)
        fun(self)


class ValueSelect(Select):

    def _append_column(self, query, **kwargs):
        if self.agg and 'in_group' in kwargs:
            return self._replace_column(query,
                    self.agg(self.column_clause).label(self.name))
        else:
            return super(ValueSelect, self)._append_column(query)


class OverSelect(ValueSelect):

    def need_subquery(self):
        return True

    def _append_column(self, query, **kwargs):
        query = super(OverSelect, self)._append_column(query, **kwargs)
        return query


class RankingSelect(OverSelect):

    def _append_column(self, query, **kwargs):
        if 'in_group' in kwargs:
            col = self.agg(self.column_clause)
            query = query.group_by(col)
        else:
            col = self.column_clause
        return self._replace_column(query, over(func.dense_rank(),
            order_by=col))


class GroupingSelect(Select):

    def _append_column(self, query, **kwargs):
        query = super(GroupingSelect, self)._append_column(query, **kwargs)
        if 'in_group' in kwargs and not self.is_constant:
            query = query.group_by(self.column_clause)
        return query


class IdSelect(GroupingSelect):
    pass


class LabelSelect(GroupingSelect):
    pass


class FilterSelect(Select):
    pass


class PostFilterSelect(Select):

    def need_subquery(self):
        return True


def by_class(selects):
    selects_dicts = {clz: [] for clz in (
        ValueSelect, LabelSelect, IdSelect, FilterSelect, OverSelect,
        PostFilterSelect)}
    for select in selects:
        selects_dicts[select.__class__].append(select)
    return selects_dicts


def process_selects(query, selects, **kwargs):
    typed_selects = by_class(selects)
    values = typed_selects[ValueSelect] + typed_selects[OverSelect]
    if all(value.agg for value in values):
        kwargs['in_group'] = True
    for select in selects:
        query = select._append_join(query, **kwargs)
    for value in values:
        query = value._append_to_query(query, **kwargs)
    for ids in typed_selects[IdSelect]:
        query = ids._append_to_query(query, **kwargs)
    for label in typed_selects[LabelSelect]:
        query = label._append_to_query(query, **kwargs)
    for filter in typed_selects[FilterSelect]:
        query = filter._append_to_query(query, **kwargs)
    for filter in typed_selects[PostFilterSelect]:
        query = filter._append_to_query(query, **kwargs)
    return query


def compile(selects, query, level=0):
    if level > 10:
        raise Exception('Not convergent query, abort, abort!')
    simples = [sel for sub in selects for sel in
                sub.simplify(query)]
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
        for _, val in sorted(subqueries.items(), key=lambda x: x[0])]
    values = subqueries[0]
    idx = 0
    query = process_selects(query, values)
    columns_to_keep = []
    group_bys = []
    for column in list(query.inner_columns):
        if ((column.key in [sel.name for sel in selects]) or
            any(sub.need_column(column)
                    for sub in reduce(list.__add__, subqueries[idx + 1:],
                        []))):
                columns_to_keep.append(column)
    for column in query._group_by_clause:
        if any(col.shares_lineage(column) for col in columns_to_keep):
            group_bys.append(column)
        elif hasattr(column, '_preserve_for_over'):
            group_bys.append(column)
    query = query.with_only_columns(columns_to_keep)
    query._group_by_clause = []
    query = query.group_by(*set(group_bys))
    if len(subqueries) > 1:
        query = query.alias().select()
        return compile(simples, query, level=level + 1)
    return query
