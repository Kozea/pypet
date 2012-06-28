from sqlalchemy.schema import (PrimaryKeyConstraint, ForeignKeyConstraint,
    AddConstraint, Index)
from sqlalchemy.sql import select, func, and_
from sqlalchemy.sql.expression import (Executable, ClauseElement, Select,
    FromClause, ColumnCollection)
from sqlalchemy.ext.compiler import compiles
from pypet import (Level, ComputedLevel, Aggregate, AllLevel, Measure,
    CountMeasure, aggregates)
from psycopg2.extensions import adapt as sqlescape
import re


class NewRowToAgg(Select):

    def __init__(self, orig_table):
        self.orig_table = orig_table
        self.name = 'NEW'
        self.use_labels = True
        columns = []
        self.named_with_column = True
        for col in orig_table.c:
            newcol = col.copy()
            newcol.table = self
            columns.append(newcol)
        self._columns = ColumnCollection(*columns)

    def is_derived_from(self, from_clause):
        return self.orig_table.is_derived_from(from_clause)

    def corresponding_column(self, column, require_embedded=False):
        col = self.orig_table.corresponding_column(column,
            require_embedded)
        if col is not None:
            return self._columns[col.name]


@compiles(NewRowToAgg)
def visit_new_row_trigger_from_clause(element, compiler, **kw):
    query_parts = []
    for c in element.c:
        query_parts.append(compiler.process(c).replace('"NEW"', 'NEW'))
    return '( SELECT %s ) AS "%s"' % (', '.join(query_parts), element.name)


class AccumulatorRow(FromClause):

    def __init__(self, selectable, agg):
        self.selectable = selectable
        self.count = selectable.c[agg.fact_count_column.name]
        self.selectable = selectable
        self._columns = selectable.c


@compiles(AccumulatorRow)
def visit_accumulator_row(element, compiler, **kw):
    return compiler.process(element.selectable, **kw)


class SelectInto(Select):

    def __init__(self, selectable, into):
        super(SelectInto, self).__init__(selectable.c)
        self.selectable = selectable
        self.into = into


@compiles(SelectInto)
def visit_select_into(element, compiler, **kw):
    return ("SELECT * INTO %s from (%s) t" % (
            element.into,
            compiler.process(element.selectable, **kw)))


class CreateTableAs(Executable, ClauseElement):

    def __init__(self, table_name, select, columns=None, schema=None):
        self.table_name = table_name
        self.schema = schema
        self.select = select
        self.columns = columns


@compiles(CreateTableAs)
def visit_create_table_as(element, compiler, **kw):
    preparer = compiler.dialect.identifier_preparer
    table_name = preparer.quote_identifier(element.table_name)
    if element.schema is not None:
        table_name = '%s.%s' % (preparer.quote_identifier(element.schema),
                table_name)
    return "CREATE TABLE %s AS %s" % (
        table_name,
        compiler.process(element.select)
    )


class CreateFunction(Executable, ClauseElement):

    def __init__(self, function_name, args, return_type, body,
            language='plpgsql', schema=None):
        self.function_name = function_name
        self.args = args
        self.return_type = return_type
        self.language = language
        self.body = body
        self.schema = schema


@compiles(CreateFunction)
def visit_create_function(element, compiler, **kw):
    preparer = compiler.dialect.identifier_preparer
    fn_name = preparer.quote_identifier(element.function_name)
    if element.schema is not None:
        fn_name = '%s.%s' % (preparer.quote_identifier(element.schema),
                fn_name)
    params = []
    for name, type in element.args.items():
        if not isinstance(type, basestring):
            type = compiler.process(type, **kw)
        params.append('%s %s' % (name, type))
    return_type = element.return_type
    if not isinstance(return_type, basestring):
        return_type = compiler.process(type, **kw)
    result = ('CREATE FUNCTION %s (%s) RETURNS %s as $fn_body$ \n' %
                (fn_name, ','.join(params), return_type))
    result += element.body
    result += '\n $fn_body$ language %s' % (element.language)
    return result


def adapt_query(query, base_table):
    new_base_table = NewRowToAgg(base_table)
    return query.replace_selectable(base_table,
                new_base_table)


class NamingConvention(object):
    """A namingconvention describe how aggregates table and column names should
    be matched."""

    table_name = 'agg_{levels}'
    level_name = '{level.dimension.name}_{level.name}'
    measure_name = '{measure.name}'
    table_level_name = level_name
    table_measure_name = measure_name
    level_name_separator = '_'
    measure_name_separator = '_'
    fact_count_column_name = 'fact_count'
    trigger_function_name = 'trigger_function_{tablename}'
    trigger_name = 'trigger_{tablename}'
    idx_name = 'idx_{tablename}_{levelname}'

    @classmethod
    def build_level_name(cls, level):
        return cls.level_name.format(level=level)

    @classmethod
    def build_measure_name(cls, measure):
        return cls.measure_name.format(measure=measure)

    @classmethod
    def build_table_name(cls, levels, measures):
        levels_str = []
        measures_str = []
        for level in levels:
            levels_str.append(cls.table_level_name.format(level=level))
        for measure in measures:
            measures_str.append(cls.table_measure_name.format(measure=measure))
        levels_str = cls.level_name_separator.join(levels_str)
        measures_str = cls.measure_name_separator.join(measures_str)
        return cls.table_name.format(levels=levels_str,
                measures=measures_str)

    @classmethod
    def build_fact_count_column_name(cls):
        return cls.fact_count_column_name

    @classmethod
    def matches_table_name(cls, cube, table):
        table_name_re = cls.table_name.replace('{levels}', '.*')
        table_name_re = table_name_re.replace('{measures}', '.*')
        return re.match(table_name_re, table.name)

    @classmethod
    def find_column_as_level(cls, cube, column):
        splitted = column.name.split('_')
        if len(splitted) == 2:
            # It may be a level
            if splitted[0] in cube.d:
                for hierarchy in cube.d[splitted[0]].h.values():
                    if splitted[1] in hierarchy.l:
                        return hierarchy.l[splitted[1]]

    @classmethod
    def find_column_as_measure(cls, cube, column):
        if column.name in cube.m:
            return cube.m[column.name]

    @classmethod
    def find_column_as_fact_count(cls, cube, column):
        if cube.fact_count_column is not None:
            if cube.fact_count_column.name == column.name:
                return column
        if column.name == cls.fact_count_column_name:
            return column

    @classmethod
    def build_trigger_name(cls, tablename):
        return cls.trigger_name.format(tablename=tablename)

    @classmethod
    def build_trigger_function_name(cls, tablename):
        return cls.trigger_function_name.format(tablename=tablename)


def table_to_aggregate(cube, table, naming_convention=NamingConvention):
    if naming_convention.matches_table_name(cube, table):
        measures = {}
        levels = {}
        fact_count_column = None
        for col in table.columns:
            as_fc = naming_convention.find_column_as_fact_count(cube, col)
            if as_fc is not None:
                fact_count_column = as_fc
            as_ms = naming_convention.find_column_as_measure(cube, col)
            if as_ms:
                measures[as_ms] = col
            as_level = naming_convention.find_column_as_level(cube, col)
            if as_level:
                levels[as_level] = col
        if measures and levels and fact_count_column is not None:
            return Aggregate(table, levels, measures, fact_count_column)


def reflect_aggregates(cube, naming_convention=NamingConvention):
    """Reflect aggregates from the cube definition.

    The sqlalchemy metadata should have been populated beforehand (via
    "reflect")
    """
    for table in cube.alchemy_md.tables.values():
        agg = table_to_aggregate(cube, table, naming_convention)
        if agg is not None:
            cube.aggregates.append(agg)


class AggBuilder(object):
    """Aggregate builder.

    An aggregate builder can be used to automatically create an aggregate table
    from a pypet query, provided the query meet certain criteria:

        - It must involve at most one level per cube dimension
        - It must not involve any computed measure.
        - It must not involve any filters, or orders.

    """

    def __init__(self, query, naming_convention=NamingConvention):
        """Creates an AggregateBuilder instance, using the given query and
        naming convention.
        """
        self.naming_convention = naming_convention
        self.query = query
        # Check that the query is indeed suitable for an aggregate
        seen_dimensions_names = []
        for axis in query.axes:
            if axis.dimension.name in seen_dimensions_names:
                raise ValueError('The query must not query axes for more than'
                    'one level in each dimension (%s dimension is on several'
                    'axis)' % axis.dimension.name)
            seen_dimensions_names.append(axis.dimension.name)
            if not isinstance(axis, Level):
                raise ValueError('All axis MUST be levels,'
                    '%s are not supported' % axis.__class__.name)
        if query.filter_clause is not None:
            raise ValueError('An aggregate query MUST NOT contain any filter')
        if query.orders:
            raise ValueError('An aggregate query MUST NOT contain any order')
        for measure in query.measures:
            if measure not in query.cuboid.m.values():
                raise ValueError('An aggregate query MUST NOT contain'
                        'any measure not defined on the cube itself')

    def build_trigger(self, conn, cube, sql_query, agg,
            nc=NamingConvention):
        # Adapt the query to the "new" table structure
        new_query = adapt_query(sql_query, cube.selectable).alias()

        # Declare two new from_clauses, corresponding to the NEW row, and the
        # matching AGG row.
        new_row = AccumulatorRow(new_query, agg)
        agg_row = AccumulatorRow(agg.selectable, agg)
        transformations = {}
        primary_keys = {}
        # Add the necessary transformations through the measures accumulators
        for name, expr in agg.measures_expr.items():
            measure = agg.measures[name]
            transformations[expr.name] = (measure.agg.accumulator(expr.name,
                new_row, agg_row).label(expr.name))

        # Update the fact count
        transformations[agg.fact_count_column.name] = (
                new_row.c[agg.fact_count_column.name] +
                func.coalesce(agg_row.c[agg.fact_count_column.name], 0)).label(
                        agg.fact_count_column.name)

        filter_clause = []
        for name, expr in agg.levels.items():
            primary_keys[expr.name] = new_row.c[expr.name]
            filter_clause.append(new_row.c[expr.name] == agg_row.c[expr.name])

        agg_column_names = [col.name for col in agg.selectable.c]

        values = sorted(transformations.values() + primary_keys.values(),
                key=lambda x: agg_column_names.index(x.name))

        from_obj = new_row.join(agg.selectable,
            onclause=and_(*filter_clause))
        select_statement = (select(values, from_obj=from_obj)
                                .correlate(new_query, agg.selectable))

        fn_name = nc.build_trigger_function_name(
                agg.selectable.name)
        variable_name = 'temp_row_for_update'
        intostmt = SelectInto(select_statement, variable_name).compile()
        params = {}
        for k, v in intostmt.params.items():
            params[k] = sqlescape(v)
        intostmt = intostmt.string % params
        intostmt_when_null = SelectInto(new_query, variable_name).compile()
        params = {}
        for k, v in intostmt_when_null.params.items():
            params[k] = sqlescape(v)
        intostmt_when_null = intostmt_when_null.string % params

        values = []
        for name in transformations:
            values.append('"%s" = %s."%s"' % (name, variable_name, name))
        pk_values = []
        for name in primary_keys:
            pk_values.append('"%s" = %s."%s"' % (name, variable_name, name))
        insert_keys = []
        insert_values = []
        if agg.selectable.schema is not None:
            qualified_table_name = '"%s"."%s"' % (agg.selectable.schema,
                    agg.selectable.name)
        else:
            qualified_table_name = '"%s"' % agg.selectable.name
        for col in agg.selectable.c:
            insert_keys.append('"%s"' % col.name)
            insert_values.append('%s."%s"' % (variable_name, col.name))
        insert_stmt = 'INSERT INTO %s (%s) (SELECT %s) ' % (
                qualified_table_name,
                ', '.join(insert_keys),
                ', '.join(insert_values))
        fn_body = """DECLARE
                        %s %s;
                     BEGIN
                        %s;
                        IF (%s IS NULL) THEN
                            %s;
                        END IF;
                        UPDATE %s set %s WHERE %s;
                        IF NOT FOUND THEN
                            %s ;
                        END IF;
                        RETURN NEW;
                     END;
        """ % (variable_name, qualified_table_name, intostmt,
                variable_name,
                intostmt_when_null,
                qualified_table_name,
                ', '.join(values),
                ' AND '.join(pk_values),
                insert_stmt
                )
        function_declaration = CreateFunction(fn_name, {}, 'TRIGGER', fn_body,
                schema=agg.selectable.schema)
        conn.execute(function_declaration)
        if cube.selectable.schema is not None:
            base_table_qualified_name = '"%s"."%s"' % (cube.selectable.schema,
                    cube.selectable.name)
        else:
            base_table_qualified_name = '"%s"' % cube.table.name
        trigger_name = '"%s"' % nc.build_trigger_name(agg.selectable.name)
        fn_name = '"%s"' % fn_name
        if agg.selectable.schema is not None:
            trigger_name = '"%s".%s' % (agg.selectable.schema, trigger_name)
            fn_name = '"%s".%s' % (agg.selectable.schema, fn_name)
        trigger_declaration = ("""CREATE TRIGGER %s BEFORE INSERT ON %s
            FOR EACH ROW EXECUTE PROCEDURE %s()""" % (
                nc.build_trigger_name(agg.selectable.name),
                base_table_qualified_name,
                fn_name))
        conn.execute(trigger_declaration)


    def build(self, schema=None, with_trigger=False, with_indexes=True):
        """Creates the actual aggregate table.

        It will create and populate the table with a name and column names
        according to the NamingConvention, as well as a primary key and the
        needed foreign keys.

        THIS CAN TAKE A LONG, LONG TIME !

        ```schema```: if given, will create the table in the specified schema.
        ```with_trigger```: Add a trigger to the fact table to automatically
        maintain the aggregate table.

        """
        axis_columns = {}
        measure_columns = []
        cube = self.query.cuboid
        axes = filter(lambda x: not isinstance(x, AllLevel), self.query.axes)
        measures = filter(lambda x: type(x) == Measure, self.query.measures)
        table_name = self.naming_convention.build_table_name(self.query.axes,
                measures)
        query = self.query._generate()
        base_agg = cube._find_best_agg(query.parts)
        fact_count_column_name = (self.naming_convention.
                build_fact_count_column_name())
        query.measures.append(CountMeasure(fact_count_column_name))
        sql_query = query._as_sql()
        # Work on the "raw" query to add the fact count column
        sql_query = sql_query.alias()
        fact_count_col = (sql_query.c[fact_count_column_name]
                .label(fact_count_column_name))
        # Build aliases for axes and measures
        for axis in axes:
            label = self.naming_convention.build_level_name(axis)
            axis_columns[axis] = (sql_query.c[axis._label_for_select]
                    .label(label))
        for measure in measures:
            label = self.naming_convention.build_measure_name(measure)
            measure_columns.append(sql_query.c[measure.name].label(label))

        # Create table
        sql_query = select(axis_columns.values() + measure_columns +
                [fact_count_col])
        conn = sql_query.bind.connect()
        tr = conn.begin()
        conn.execute(CreateTableAs(table_name, sql_query, schema=schema))

        # Add it to the metadata via reflection
        cube.alchemy_md.reflect(bind=conn, schema=schema,
                only=[table_name])
        if schema:
            metadata_table_key = '%s.%s' % (schema, table_name)
        else:
            metadata_table_key = table_name
        table = cube.alchemy_md.tables[metadata_table_key]

        if axis_columns:
        # Add PK and FK constraints
            pk = PrimaryKeyConstraint(*[table.c[col.key]
                for axis, col in axis_columns.items()])
            conn.execute(AddConstraint(pk))
        for axis, column in axis_columns.items():
            if isinstance(axis, (ComputedLevel, AllLevel)):
                # DO NOT add foreign key for computed and all levels!
                continue
            fk = ForeignKeyConstraint(columns=[column.name],
                    refcolumns=[axis.column],
                    table=table,
                    deferrable=True)
            conn.execute(AddConstraint(fk))
        axes = {axis: table.c[column.name] for axis, column in
            axis_columns.items()}
        # Append the aggregate definition to the cube
        agg = Aggregate(table, axes,
            {measure: table.c[measure.name]
                for measure in measures},
            fact_count_column=table.c[fact_count_column_name])

        if with_trigger:
            self.build_trigger(conn, base_agg, sql_query, agg, self.naming_convention)
        if with_indexes:
            for column in axis_columns.values():
                Index(('ix_%s_%s' % (table.name, column.key))[:63], table.c[column.key]).create(bind=conn)
        tr.commit()

        cube.aggregates.append(agg)
