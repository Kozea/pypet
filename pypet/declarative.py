import pypet

from functools import wraps
from sqlalchemy import create_engine
from sqlalchemy.schema import MetaData


UNKNOWN_VALUE = object()


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

_COUNTERS = {}


class MetaDeclarative(type):
    def __new__(mcs, classname, bases, classdict):
        global _COUNTERS
        cls = bases[0]
        class_ = type.__new__(mcs, classname, bases, classdict)
        if bases == (Declarative,):  # If Level do nothing
            return class_

        _COUNTERS[cls] = _COUNTERS.get(cls, 0) + 1
        instance = mcs._make_instance(mcs, class_, classdict)
        instance._count = _COUNTERS[cls]
        instance.definition = class_

        return instance


class Declarative(object):
    """Declarative"""

    def __new__(cls, *args, **kwargs):
        global _COUNTERS
        _COUNTERS[cls] = _COUNTERS.get(cls, 0) + 1
        instance = cls._make_instance(cls, *args, **kwargs)
        instance._count = _COUNTERS[cls]
        instance.definition = cls
        return instance

    @staticmethod
    def _make_instance(cls, *args, **kwargs):
        args = tuple(['_unbound_'] + list(args))
        return getattr(pypet, cls.__name__)(*args, **kwargs)


class MetaLevel(MetaDeclarative):

    @staticmethod
    def _make_instance(mcs, class_, classdict):
        metadata = pypet.MetaData()
        for key in dir(class_):
            if key not in ('column', 'label_column', 'label_expression'
            ) and not key.startswith('_'):
                metadata[key] = getattr(class_, key)
        label_expression = classdict.get('label_expression', None)
        if label_expression is not None:
            # Wraps the expression to provide dummy self.
            def make_label_expression(label_expression):
                @wraps(label_expression)
                def curried_label_expression(value):
                    return label_expression(None, value)
                return curried_label_expression
            label_expression = make_label_expression(label_expression)
        return pypet.Level(
            class_.__name__,
            classdict.get('column', None),
            classdict.get('label_column', None),
            label_expression,
            metadata=metadata)


class Level(Declarative):
    """Declarative level with instance counter"""
    __metaclass__ = MetaLevel


class MetaMeasure(MetaDeclarative):

    @staticmethod
    def _make_instance(mcs, class_, classdict):
        metadata = pypet.MetaData()
        for key in dir(class_):
            if key not in ('expression', 'agg'
            ) and not key.startswith('_'):
                metadata[key] = getattr(class_, key)

        return pypet.Measure(
            class_.__name__,
            classdict.get('expression', UNKNOWN_VALUE),
            classdict.get('agg', None),
            metadata=metadata)


class Measure(Declarative):
    """Declarative measure with instance counter"""
    __metaclass__ = MetaMeasure

    @staticmethod
    def _make_instance(cls, *args, **kwargs):
        args = ['_unbound_'] + list(args)
        if len(args) == 1:
            args = args + [UNKNOWN_VALUE]
        return pypet.Measure(*args, **kwargs)


class MetaHierarchy(MetaDeclarative):

    @staticmethod
    def _make_instance(mcs, class_, classdict):
        levels = []
        metadata = pypet.MetaData()
        for key in dir(class_):
            value = getattr(class_, key)
            if isinstance(value, pypet.Level):
                value.name = key
                levels.append(value)
            elif not key.startswith('_'):
                metadata[key] = value

        levels = sorted(levels,
                        key=lambda x: getattr(x, '_count', float('inf')))
        hierarchy = pypet.Hierarchy(class_.__name__, levels, metadata=metadata)
        for level in levels:
            if not hasattr(hierarchy, level.name):
                setattr(hierarchy, level.name, level)
        return hierarchy


class Hierarchy(Declarative):
    """Declarative hierarchy"""
    __metaclass__ = MetaHierarchy


class MetaDimension(MetaDeclarative):

    @staticmethod
    def _make_instance(mcs, class_, classdict):
        hierarchies = []
        default_levels = {}
        metadata = pypet.MetaData()
        for key in dir(class_):
            value = getattr(class_, key)
            if isinstance(value, pypet.Hierarchy):
                value.name = key
                hierarchies.append(value)
            elif isinstance(value, pypet.Level):
                value.name = key
                default_levels[value._count] = value
            elif not key.startswith('_'):
                metadata[key] = value

        if len(default_levels):
            levels = [level for _, level
                      in sorted(default_levels.items(),
                        key=lambda x: x[0])]
            default_hierarchy = pypet.Hierarchy('default', levels)
            default_hierarchy._count = -1
            for level in levels:
                if not hasattr(default_hierarchy, level.name):
                    setattr(default_hierarchy, level.name, level)
            hierarchies.append(default_hierarchy)
        hierarchies = sorted(hierarchies,
                        key=lambda x: getattr(x, '_count', float('inf')))
        dimension = pypet.Dimension(
            class_.__name__, hierarchies, metadata=metadata)
        for hierarchy in hierarchies:
            if not hasattr(dimension, hierarchy.name):
                setattr(dimension, hierarchy.name, hierarchy)
        return dimension


class Dimension(Declarative):
    """Declarative dimension"""
    __metaclass__ = MetaDimension


class MetaCube(MetaDeclarative):

    @staticmethod
    def _make_instance(mcs, class_, classdict):
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
        for key in dir(class_):
            value = getattr(class_, key)
            if isinstance(value, pypet.Dimension):
                value.name = key
                dimensions.append(value)
            elif isinstance(value, pypet.Measure):
                value.name = key
                expr = getattr(value, 'expression', None)
                if expr is UNKNOWN_VALUE:
                    value.expression = key
                if expr is not None:
                    value.expression = column(value.expression, fact_table)
                measures.append(value)

        dimensions = sorted(dimensions,
                        key=lambda x: getattr(x, '_count', float('inf')))
        measures = sorted(measures,
                        key=lambda x: getattr(x, '_count', float('inf')))
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
