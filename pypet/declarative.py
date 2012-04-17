import pypet
from sqlalchemy import create_engine
from sqlalchemy.schema import MetaData

_LEVEL_COUNTER = 0


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


class MetaLevel(type):

    def __new__(cls, classname, bases, classdict):
        global _LEVEL_COUNTER
        _LEVEL_COUNTER += 1
        level_class = type.__new__(cls, classname, bases, classdict)
        if bases == (Declarative,):  # If Level do nothing
            return level_class

        metadata = pypet.MetaData()
        for key in dir(level_class):
            if key not in ('column', 'label_column', 'label_expression'
            ) and not key.startswith('__'):
                metadata[key] = getattr(level_class, key)

        level = pypet.Level(
            '__unbound__',
            classdict.get('column', None),
            classdict.get('label_column', None),
            classdict.get('label_expression', None), metadata=metadata)
        level.definition = level_class
        level._count = _LEVEL_COUNTER
        return level


class Level(Declarative):
    """Declarative level with instance counter"""
    __metaclass__ = MetaLevel

    def __new__(cls, *args, **kwargs):
        global _LEVEL_COUNTER
        _LEVEL_COUNTER += 1

        args = tuple(['_unbound_'] + list(args))
        level = pypet.Level(*args, **kwargs)
        level.definition = cls
        level._count = _LEVEL_COUNTER
        return level


class Measure(Declarative):
    """Declarative measure"""

    def __call__(self, name, table):
        if len(self.args):
            expr = self.args[0]
            self.args = self.args[1:]
        else:
            expr = name
        expression = column(expr, table)
        return pypet.Measure(name, expression, *self.args, **self.kwargs)


class MetaHierarchy(type):
    def __new__(cls, classname, bases, classdict):
        hierarchy_class = type.__new__(cls, classname, bases, classdict)
        if bases == (Declarative,):  # If Hierarchy do nothing
            return hierarchy_class

        levels = []
        metadata = pypet.MetaData()
        for key in dir(hierarchy_class):
            value = getattr(hierarchy_class, key)
            if isinstance(value, pypet.Level):
                value.name = key
                levels.append(value)
            elif not key.startswith('__'):
                metadata[key] = value

        levels = sorted(levels, key=lambda x: x._count)
        hierarchy = pypet.Hierarchy('_unbound_', levels, metadata=metadata)
        hierarchy.definition = hierarchy_class
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
        if bases == (Declarative,):  # If Dimension do nothing
            return dimension_class

        hierarchies = []
        default_levels = {}
        metadata = pypet.MetaData()
        for key in dir(dimension_class):
            value = getattr(dimension_class, key)
            if isinstance(value, pypet.Hierarchy):
                value.name = key
                hierarchies.append(value)
            elif isinstance(value, pypet.Level):
                value.name = key
                default_levels[value._count] = value
            elif not key.startswith('__'):
                metadata[key] = value

        if len(default_levels):
            levels = [level for _, level
                      in sorted(default_levels.items(), key=lambda x: x[0])]
            default_hierarchy = pypet.Hierarchy('default', levels)
            for level in levels:
                if not hasattr(default_hierarchy, level.name):
                    setattr(default_hierarchy, level.name, level)
            hierarchies.append(default_hierarchy)

        dimension = pypet.Dimension(
            '_unbound_', hierarchies, metadata=metadata)
        dimension.definition = type.__new__(cls, classname, bases, classdict)
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
        cube.definition = cube_class

        for thing in dimensions + measures:
            if not hasattr(cube, thing.name):
                setattr(cube, thing.name, thing)
        return cube


class Cube(Declarative):
    """Declarative measure"""
    __metaclass__ = MetaCube
