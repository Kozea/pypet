import pypet
from pypet.test import BaseTestCase
from pypet import aggregates
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
        l4 = Level()

    assert isinstance(TimeHierarchy, pypet.Hierarchy)
    assert TimeHierarchy.levels.keys() == ['All', 'l1', 'l2', 'l3']
    assert SubTimeHierarchy.levels.keys() == ['All', 'l1', 'l2', 'l3']
    assert hasattr(SubSubTimeHierarchy, 'l4')
    assert SubSubTimeHierarchy.levels.keys() == ['All', 'l1', 'l2', 'l3', 'l4']

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

    class SpaceHierarchy(Hierarchy):
        l1 = Level()

    class TimeDimension(Dimension):
        h1 = TimeHierarchy
        h2 = TimeHierarchy2

    class SpaceDimension(Dimension):
        h1 = SpaceHierarchy

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
    assert TimeDimension.h1 != SpaceDimension.h1
    assert TimeDimension.h1.l1 != SpaceDimension.h1.l1


def test_level_in_dimension():

    class TimeHierarchy(Hierarchy):
        l1 = Level()
        l2 = Level()
        l3 = Level()

    class TimeDimension(Dimension):
        l1 = Level()
        h1 = TimeHierarchy
        l2 = Level()
        l3 = Level()
        l4 = Level()

    assert len(TimeDimension.h1.l) == 4
    assert hasattr(TimeDimension, 'default')
    assert len(TimeDimension.default.l) == 5
    assert TimeDimension.default.l.keys() == ['All', 'l1', 'l2', 'l3', 'l4']
    assert hasattr(TimeDimension.default, 'l1')
    assert hasattr(TimeDimension.default, 'l2')


class TestCube(BaseTestCase):
    def test_cube(self):
        def c(col):
            table, column = col.split('.')
            return self.metadata.tables[table].columns[column]

        class StoreDimension(Dimension):
            region = Level(c('region.region_id'), c('region.region_name'))
            country = Level(c('country.country_id'), c('country.country_name'))
            store = Level(c('store.store_id'), c('store.store_name'))

        class ProductDimension(Dimension):
            category = Level(c('product_category.product_category_id'),
                             c('product_category.product_category_name'))
            product = Level(c('product.product_id'),
                            c('product.product_name'))

        # class TimeDimension(Dimension):
        #     year = Level()
        #     month = Level()
        #     day = Level()

        class TestCube(Cube):
            __metadata__ = self.metadata
            __fact_table__ = 'facts_table'
            __fact_count_column__ = 'qty'

            store = StoreDimension
            product = ProductDimension
            time = self.time_dim

            price = Measure()
            quantity = Measure('qty', agg=aggregates.sum)

        assert isinstance(TestCube, pypet.Cube)
        assert isinstance(TestCube.price, pypet.Measure)
        assert isinstance(TestCube.quantity, pypet.Measure)
        assert isinstance(TestCube.time, pypet.Dimension)
        # assert isinstance(TestCube.time.default, pypet.Hierarchy)
        # assert isinstance(TestCube.time.default.day, pypet.Level)
        assert isinstance(TestCube.query, pypet.Query)
        TestCube.query.axis(StoreDimension.default.region).execute()
