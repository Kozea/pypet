import pypet
from pypet.test import BaseTestCase
from pypet.declarative import Level, Hierarchy, Dimension, Measure, Cube


def test_level():
    l1 = Level()
    l2 = Level()
    l3 = Level()
    levels = [l2, l3, l1]
    sorted_levels = sorted(levels, key=lambda x: x._count)
    assert sorted_levels == [l1, l2, l3]


def test_hierarchy():

    class TimeHierarchy(Hierarchy):
        l1 = Level()
        l2 = Level()
        l3 = Level()

    class SubTimeHierarchy(TimeHierarchy.definition):
        l2 = Level()

    class SubSubTimeHierarchy(SubTimeHierarchy.definition):
        l3 = Level()

    assert isinstance(TimeHierarchy, pypet.Hierarchy)
    assert TimeHierarchy.levels.keys() == ['All', 'l1', 'l2', 'l3']
    assert SubTimeHierarchy.levels.keys() == ['All', 'l1', 'l2', 'l3']
    assert SubSubTimeHierarchy.levels.keys() == ['All', 'l1', 'l2', 'l3']

    for key in ('l1', 'l2', 'l3'):
        assert isinstance(getattr(TimeHierarchy, key), pypet.Level)
        assert isinstance(getattr(SubTimeHierarchy, key), pypet.Level)
        assert isinstance(getattr(SubSubTimeHierarchy, key), pypet.Level)

        assert getattr(TimeHierarchy, key).name == key
        assert getattr(TimeHierarchy, key) == getattr(TimeHierarchy, key)
        assert getattr(TimeHierarchy, key) != getattr(SubTimeHierarchy, key)
        assert getattr(TimeHierarchy, key) != getattr(SubSubTimeHierarchy, key)
        assert getattr(SubTimeHierarchy, key) != getattr(
            SubSubTimeHierarchy, key)


def test_dimension():

    class TimeHierarchy(Hierarchy):
        l1 = Level()
        l2 = Level()
        l3 = Level()

    class TimeHierarchy2(TimeHierarchy.definition):
        l1_2 = Level()
        l2_2 = Level()
        l3_2 = Level()

    class TimeDimension(Dimension):
        h1 = TimeHierarchy
        h2 = TimeHierarchy2

    assert isinstance(TimeDimension.h1, pypet.Hierarchy)
    assert isinstance(TimeDimension.h1.l1, pypet.Level)
    assert TimeDimension.h1.l1 == TimeDimension.h1.l1
    assert TimeDimension.h1.l1 != TimeDimension.h2.l1

    assert TimeDimension.h1.levels.keys() == ['All', 'l1', 'l2', 'l3']
    assert TimeDimension.h2.levels.keys() == ['All', 'l1', 'l2', 'l3',
                                        'l1_2', 'l2_2', 'l3_2']
    assert len(TimeDimension.hierarchies) == 2
    assert len([level for h in TimeDimension.hierarchies.values()
           for level in h.levels]) == 11


class TestCube(BaseTestCase):
    def test_cube(self):

        class TimeHierarchy(Hierarchy):
            year = Level()
            month = Level()
            day = Level()

        class TimeDimension(Dimension):
            time = TimeHierarchy

        class TestCube(Cube):
            __connection__ = 'postgresql://pypet@localhost/pypet'
            __fact_table__ = 'facts_table'
            __fact_count_column__ = 'qty'
            time = TimeDimension

            price = Measure()
            quantity = Measure('qty')

        assert isinstance(TestCube, pypet.Cube)
        assert isinstance(TestCube.price, pypet.Measure)
        assert isinstance(TestCube.quantity, pypet.Measure)
        assert isinstance(TestCube.time, pypet.Dimension)
        assert isinstance(TestCube.time.time, pypet.Hierarchy)
        assert isinstance(TestCube.time.time.day, pypet.Level)
        assert isinstance(TestCube.query, pypet.Query)
        # TestCube.query.axis(TimeDimension.h1.l1)
