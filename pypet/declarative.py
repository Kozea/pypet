import pypet


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
        levels = []
        ordered_keys = getattr(self, '_ordered_keys', None) or [
            key for key, _ in sorted(
                self._declaratives.items(), key=lambda x: x[1]._count)]
        levels = [getattr(self, key) for key in ordered_keys]
        return pypet.Hierarchy(name, levels, *self.args, **self.kwargs)


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
        hierarchies = []
        for attr in dir(self):
            val = getattr(self, attr)
            if Hierarchy in val.__mro__:
                val = val()
            if isinstance(val, Hierarchy):
                hierarchies.append(val._get(attr))
        return pypet.Dimension(name, hierarchies, *self.args, **self.kwargs)


class Measure(Declarative):
    """Declarative measure"""

    def _get(self, name):
        return pypet.Measure(name, *self.args, **self.kwargs)


class Cube(object):
    """Declarative measure"""
