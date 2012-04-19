import pypet
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


class MetaLevel(MetaDeclarative):

    @staticmethod
    def _make_instance(mcs, class_, classdict):
        metadata = pypet.MetaData()
        for key in dir(class_):
            if key not in ('column', 'label_column', 'label_expression'
            ) and not key.startswith('_'):
                metadata[key] = getattr(class_, key)

        return pypet.Level(
            '_unbound_',
            classdict.get('column', None),
            classdict.get('label_column', None),
            classdict.get('label_expression', None),
            metadata=metadata)


class Level(Declarative):
    """Declarative level with instance counter"""
    __metaclass__ = MetaLevel

    @staticmethod
    def _make_instance(cls, *args, **kwargs):
        args = tuple(['_unbound_'] + list(args))
        return pypet.Level(*args, **kwargs)


class MetaMeasure(MetaDeclarative):

    def __new__(cls, classname, bases, classdict):
        measure_class = type.__new__(cls, classname, bases, classdict)
        if bases == (Declarative,):  # If Measure do nothing
            return measure_class

        metadata = pypet.MetaData()
        for key in dir(measure_class):
            if key not in ('expression', 'agg'
            ) and not key.startswith('_'):
                metadata[key] = getattr(measure_class, key)

        measure = pypet.Measure(
            '_unbound_',
            classdict.get('expression', UNKNOWN_VALUE),
            classdict.get('agg', None),
            metadata=metadata)
        measure.definition = measure_class
        return measure


class Measure(Declarative):
    """Declarative measure with instance counter"""
    __metaclass__ = MetaMeasure

    def __new__(cls, *args, **kwargs):
        args = ['_unbound_'] + list(args)
        if len(args) == 1:
            args = args + [UNKNOWN_VALUE]
        measure = pypet.Measure(*args, **kwargs)
        measure.definition = cls
        return measure


class MetaHierarchy(MetaDeclarative):
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
            elif not key.startswith('_'):
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


class MetaDimension(MetaDeclarative):
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
            elif not key.startswith('_'):
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


class MetaCube(MetaDeclarative):
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
            elif isinstance(value, pypet.Measure):
                value.name = key
                expr = getattr(value, 'expression', None)
                if expr is UNKNOWN_VALUE:
                    value.expression = key
                if expr is not None:
                    value.expression = column(value.expression, fact_table)
                measures.append(value)

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
