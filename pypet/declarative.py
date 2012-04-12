import pypet
from sqlalchemy import create_engine
from sqlalchemy.schema import MetaData


def table(name_or_table, metadata):
    if isinstance(name_or_table, basestring):
        table = metadata.tables.get(name_or_table, None)
        if table is None:
            raise ValueError(
                'Fact table %s not found' % name_or_table)
        return table
    return name_or_table


def column(name_or_column, table):
    if isinstance(name_or_column, basestring):
        column = getattr(table.c, name_or_column, None)
        if column is None:
            raise ValueError(
                'Column %s not found' % name_or_column)
        return column
    return name_or_column


class Declarative(object):

    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs


class Level(Declarative):
    """Declarative level with instance counter"""
    __level_counter = 0

    def __init__(self, *args, **kwargs):
        self.__class__.__level_counter += 1
        self._count = self.__level_counter
        super(Level, self).__init__(*args, **kwargs)

    def __call__(self, name):
        return pypet.Level(name, *self.args, **self.kwargs)


class Measure(Declarative):
    """Declarative measure"""

    def __call__(self, name, table):
        expression = column(self.args[0] if len(self.args) else name, table)
        return pypet.Measure(name, expression, *self.args, **self.kwargs)


class MetaHierarchy(type):
    def __new__(cls, classname, bases, classdict):
        if bases == (Declarative,):  # If Cube do nothing
            return type.__new__(cls, classname, bases, classdict)

        classdict['_declaratives'] = {}
        for base in bases:
            if hasattr(base, '_declaratives'):
                for key, value in base._declaratives.items():
                    classdict['_declaratives'][key] = value
                    classdict[key] = value
        levels = {}
        for key, value in classdict.items():
            if isinstance(value, Level):
                classdict['_declaratives'][key] = value
                order = value._count
                levels[order] = value(key)

        levels = [level for _, level
                  in sorted(levels.items(), key=lambda x: x[0])]
        hierarchy = pypet.Hierarchy('_unbound_', levels)
        hierarchy.definition = type.__new__(cls, classname, bases, classdict)
        for level in levels:
            if not hasattr(hierarchy, level.name):
                setattr(hierarchy, level.name, level)
        return hierarchy


class Hierarchy(Declarative):
    """Declarative hierarchy"""
    __metaclass__ = MetaHierarchy


class MetaDimension(type):
    def __new__(cls, classname, bases, classdict):
        dimension_class = type.__new__(cls, classname, bases, classdict)
        if bases == (Declarative,):  # If Cube do nothing
            return dimension_class

        hierarchies = []
        for key in dir(dimension_class):
            value = getattr(dimension_class, key)
            if isinstance(value, pypet.Hierarchy):
                value.name = key
                hierarchies.append(value)

        dimension = pypet.Dimension('_unbound_', hierarchies)
        dimension.definition = dimension_class
        for hierarchy in hierarchies:
            if not hasattr(dimension, hierarchy.name):
                setattr(dimension, hierarchy.name, hierarchy)
        return dimension


class Dimension(Declarative):
    """Declarative dimension"""
    __metaclass__ = MetaDimension


class MetaCube(type):
    def __new__(cls, classname, bases, classdict):
        cube_class = type.__new__(cls, classname, bases, classdict)
        if bases == (Declarative,):  # If Cube do nothing
            return cube_class
        metadata = classdict.get('__metadata__', None)
        if not metadata:
            connection = classdict.get('__connection__', None)
            if not connection:
                raise ValueError(
                    'Cube must have at least a __metadata__'
                    ' or a __connection__ attribute')
            metadata = MetaData(bind=create_engine(connection))
            metadata.reflect()
        fact_table = classdict.get('__fact_table__', None)
        if fact_table is None:
                raise ValueError(
                    'Cube must have a __fact_table__ attribute')
        fact_table = table(fact_table, metadata)

        dimensions = []
        measures = []
        for key in dir(cube_class):
            value = getattr(cube_class, key)
            if isinstance(value, pypet.Dimension):
                value.name = key
                dimensions.append(value)
            elif isinstance(value, Measure):
                measures.append(value(key, fact_table))

        cube = pypet.Cube(
            metadata, fact_table, dimensions, measures,
            aggregates=classdict.get('__aggregates__', None),
            fact_count_column=column(
                classdict.get('__fact_count_column__', None), fact_table))

        for thing in dimensions + measures:
            if not hasattr(cube, thing.name):
                setattr(cube, thing.name, thing)
        return cube


class Cube(Declarative):
    """Declarative measure"""
    __metaclass__ = MetaCube
