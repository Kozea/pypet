import pypet
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

    class SubTimeHierarchy(TimeHierarchy):
        l2 = Level()

    class SubSubTimeHierarchy(SubTimeHierarchy):
        l3 = Level()

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

    # real_hierarchy = TimeHierarchy()._get('t')
    # assert real_hierarchy.levels.keys() == ['All', 'l1', 'l2', 'l3']


def test_dimension():

    class TimeHierarchy(Hierarchy):
        l1 = Level()
        l2 = Level()
        l3 = Level()

    class TimeHierarchy2(TimeHierarchy):
        l1_2 = Level()
        l2_2 = Level()
        l3_2 = Level()

    class TimeDimension(Dimension):
        h1 = TimeHierarchy
        h2 = TimeHierarchy2

    assert isinstance(TimeDimension.h1, pypet.Hierarchy)
    # assert isinstance(TimeDimension.h1.l1, pypet.Level)
    # assert TimeDimension.h1.l1 == TimeDimension.h1.l1
    # assert TimeDimension.h1.l1 != TimeDimension.h2.l1

    # real_dimension = TimeDimension()._get('d')
    # assert len(real_dimension.hierarchies) == 2
    # assert len([level for h in real_dimension.hierarchies.values()
           # for level in h.levels]) == 8


def test_cube():
    class TimeHierarchy(Hierarchy):
        l1 = Level()
        l2 = Level()
        l3 = Level()

    class TimeDimension(Dimension):
        __metadata__ = 'lol'
        h1 = TimeHierarchy

    class TestCube(Cube):
        d1 = TimeDimension
        m1 = Measure()

    assert TestCube.m1
    # TestCube.query.axis(TimeDimension.h1.l1)

    # assert TestCube.q
