from sqlalchemy.schema import (PrimaryKeyConstraint, ForeignKeyConstraint,
    AddConstraint)
from sqlalchemy.sql import select, func
from sqlalchemy.sql.expression import Executable, ClauseElement
from sqlalchemy.ext.compiler import compiles
from pypet import Level, ComputedLevel, Aggregate


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


class NamingConvention(object):
    """A namingconvention describe how aggregates table and column names should
    be matched."""

    table_name = 'agg_{levels}_{measures}'
    level_name = '{level.dimension.name}_{level.name}'
    measure_name = '{measure.name}'
    table_level_name = level_name
    table_measure_name = measure_name
    level_name_separator = '_'
    measure_name_separator = '_'
    fact_count_column_name = 'fact_count'

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


def reflect_agg(cube, naming_convention):
    pass


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
        if query.filters:
            raise ValueError('An aggregate query MUST NOT contain any filter')
        if query.orders:
            raise ValueError('An aggregate query MUST NOT contain any order')
        for measure in query.measures:
            if measure not in query.cuboid.m.values():
                raise ValueError('An aggregate query MUST NOT contain'
                        'any measure not defined on the cube itself')

    def build(self, schema=None, with_trigger=False):
        """Creates the actual aggregate table.

        It will create and populate the table with a name and column names
        according to the NamingConvention, as well as a primary key and the
        needed foreign keys.

        THIS CAN TAKE A LONG, LONG TIME !

        ```schema```: if given, will create the table in the specified schema.
        ```with_trigger```: Add a trigger to the fact table to automatically
        maintain the aggregate table.

        """
        sql_query = self.query._as_sql()
        axis_columns = {}
        measure_columns = []
        cube = self.query.cuboid
        table_name = self.naming_convention.build_table_name(self.query.axes,
                self.query.measures)

        # Work on the "raw" query to add the fact count column
        fact_count_column_name = (self.naming_convention.
                build_fact_count_column_name())
        if cube.fact_count_column is not None:
            fact_count_col = (func.sum(cube.fact_count_column)
                    .label(fact_count_column_name))
        else:
            fact_count_col = (func.count(1).label(fact_count_column_name()))
        sql_query = sql_query.column(fact_count_col)
        sql_query = sql_query.alias()
        fact_count_col = sql_query.c[fact_count_column_name]

        # Build aliases for axes and measures
        for axis in self.query.axes:
            label = self.naming_convention.build_level_name(axis)
            axis_columns[axis] = (sql_query.c[axis._label_for_select]
                    .label(label))
        for measure in self.query.measures:
            label = self.naming_convention.build_measure_name(measure)
            measure_columns.append(sql_query.c[measure.name].label(label))

        # Create table
        query = select(axis_columns.values() + measure_columns +
                [fact_count_col])
        conn = query.bind.connect()
        conn.execute(CreateTableAs(table_name, query, schema=schema))

        # Add it to the metadata via reflection
        cube.alchemy_md.reflect(bind=conn, schema=schema,
                only=[table_name])
        table = cube.alchemy_md.tables[table_name]

        # Add PK and FK constraints
        pk = PrimaryKeyConstraint(*[table.c[col.key] for col in
            axis_columns.values()])
        conn.execute(AddConstraint(pk))
        for axis, column in axis_columns.items():
            if isinstance(axis, ComputedLevel):
                # DO NOT add foreign key for computed levels!
                continue
            fk = ForeignKeyConstraint(columns=[column.name],
                    refcolumns=[axis.dim_column],
                    table=table)
            conn.execute(AddConstraint(fk))

        # Append the aggregate definition to the cube
        agg = Aggregate(table, {axis: table.c[column.name] for axis, column in
            axis_columns.items()},
            {measure: table.c[measure.name]
                for measure in self.query.measures},
            fact_count_column=table.c[fact_count_column_name])
        cube.aggregates.append(agg)