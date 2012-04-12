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


class Level(object):
    """Declarative level with instance counter"""
    __level_counter = 0

    def __init__(self, *args, **kwargs):
        self.__class__.__level_counter += 1
        self._count = self.__level_counter
        self.args = args
        self.kwargs = kwargs

    def _get(self, name):
        return pypet.Level(name, *self.args, **self.kwargs)


class Declarative(object):

    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs


class MetaHierarchy(type):
    def __new__(cls, classname, bases, classdict):
        classdict['_declaratives'] = {}

        for base in bases:
            if hasattr(base, '_declaratives'):
                for key, value in base._declaratives.items():
                    classdict['_declaratives'][key] = value
                    classdict.setdefault(key, value._get(key))

        for key, value in classdict.items():
            if isinstance(value, Level) or isinstance(value, Declarative):
                classdict['_declaratives'][key] = value
                classdict[key] = value._get(key)
        return type.__new__(cls, classname, bases, classdict)


class Hierarchy(Declarative):
    """Declarative hierarchy"""
    __metaclass__ = MetaHierarchy

    def _get(self, name):
        ordered_keys = getattr(self, '_ordered_keys', None) or [
            key for key, _ in sorted(
                self._declaratives.items(), key=lambda x: x[1]._count)]
        levels = [getattr(self, key) for key in ordered_keys]
        hierarchy = pypet.Hierarchy(name, levels, *self.args, **self.kwargs)
        for level in levels:
            if not hasattr(hierarchy, level.name):
                setattr(hierarchy, level.name, level)
        return hierarchy


class MetaDimension(type):
    def __new__(cls, classname, bases, classdict):
        classdict['_declaratives'] = {}

        for base in bases:
            if hasattr(base, '_declaratives'):
                for key, value in base._declaratives.items():
                    classdict['_declaratives'][key] = value
                    classdict.setdefault(key, value._get(key))

        for key, value in classdict.items():
            if isinstance(value, type) and issubclass(value, Hierarchy):
                value = value()
            if isinstance(value, Hierarchy):
                classdict['_declaratives'][key] = value
                classdict[key] = value._get(key)
        return type.__new__(cls, classname, bases, classdict)


class Dimension(Declarative):
    """Declarative dimension"""
    __metaclass__ = MetaDimension

    def _get(self, name):
        hierarchies = [getattr(self, key) for key in self._declaratives.keys()]
        dimension = pypet.Dimension(
            name, hierarchies, *self.args, **self.kwargs)
        for level in hierarchies:
            if not hasattr(dimension, level.name):
                setattr(dimension, level.name, level)
        return dimension


class Measure(Declarative):
    """Declarative measure"""

    def _get(self, name, table):
        if len(self.args):
            expression = self.args[0]
        else:
            expression = name
        expression = column(expression, table)
        return pypet.Measure(name, expression, *self.args, **self.kwargs)


class MetaCube(type):
    def __new__(cls, classname, bases, classdict):
        if bases == (object,):  # If Cube do nothing
            return type.__new__(cls, classname, bases, classdict)
        classdict['_declaratives'] = {}
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

        for base in bases:
            if hasattr(base, '_declaratives'):
                for key, value in base._declaratives.items():
                    classdict['_declaratives'][key] = value
                    classdict.setdefault(key, value._get(key))

        dimensions = []
        measures = []
        for key, value in classdict.items():
            if isinstance(value, type) and issubclass(value, Dimension):
                dimensions.append(value()._get(key))
            elif isinstance(value, Measure):
                measures.append(value._get(key, fact_table))

        cube = pypet.Cube(
            metadata, fact_table, dimensions, measures,
            aggregates=classdict.get('__aggregates__', None),
            fact_count_column=column(
                classdict.get('__fact_count_column__', None), fact_table))

        for thing in dimensions + measures:
            if not hasattr(cube, thing.name):
                setattr(cube, thing.name, thing)
        return cube


class Cube(object):
    """Declarative measure"""
    __metaclass__ = MetaCube
