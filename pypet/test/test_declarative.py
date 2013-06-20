import pypet
from pypet.test import BaseTestCase
from pypet import aggregates
from pypet.declarative import Level, Hierarchy, Dimension, Measure, Cube


def test_level():
    l1 = Level()
    l2 = Level()
    l3 = Level()

    class l4(Level):
        foo = "bar"
    l5 = Level()
    l6 = Level()

    class l7(Level):
        foo = "baz"
    l8 = Level()

    levels = [l2, l3, l5, l1, l7, l8, l6, l4]
    sorted_levels = sorted(levels, key=lambda x: x._count)
    assert sorted_levels == [l1, l2, l3, l4, l5, l6, l7, l8]
    assert l4.metadata == {'foo': 'bar'}
    assert l7.metadata == {'foo': 'baz'}


def test_hierarchy():

    class TimeHierarchy(Hierarchy):
        label = "This is time"
        thing = "This is one thing"
        l1 = Level()
        l2 = Level()
        l3 = Level()

    assert isinstance(TimeHierarchy, pypet.Hierarchy)
    assert list(TimeHierarchy.levels.keys()) == ['All', 'l1', 'l2', 'l3']
    assert TimeHierarchy.metadata == {
        'label': "This is time",
        'thing': "This is one thing"}

    for key in ('l1', 'l2', 'l3'):
        assert isinstance(getattr(TimeHierarchy, key), pypet.Level)

        assert getattr(TimeHierarchy, key).name == key


def test_dimension():

    class TimeHierarchy(Hierarchy):
        l1 = Level()
        l2 = Level()
        l3 = Level()

    class TimeHierarchy2(Hierarchy):
        l1_2 = Level()
        l2_2 = Level()
        l3_2 = Level()

    class SpaceHierarchy(Hierarchy):
        l1 = Level()

    class TimeDimension(Dimension):
        time = 'Label it is'
        h1 = TimeHierarchy
        h2 = TimeHierarchy2

    class SpaceDimension(Dimension):
        h1 = SpaceHierarchy

    assert isinstance(TimeDimension.h1, pypet.Hierarchy)
    assert isinstance(TimeDimension.h1.l1, pypet.Level)
    assert TimeDimension.h1.name == 'h1'
    assert TimeDimension.h1.l1.name == 'l1'
    assert TimeDimension.metadata == {'time': 'Label it is'}

    assert list(TimeDimension.h1.levels.keys()) == ['All', 'l1', 'l2', 'l3']
    assert list(TimeDimension.h2.levels.keys()) == ['All', 'l1_2', 'l2_2', 'l3_2']
    assert len(TimeDimension.hierarchies) == 2
    assert len([level for h in list(TimeDimension.hierarchies.values())
           for level in h.levels]) == 8
    assert list(TimeDimension.h.keys()) == ['h1', 'h2']
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
    assert list(TimeDimension.default.l.keys()) == ['All', 'l1', 'l2', 'l3', 'l4']
    assert hasattr(TimeDimension.default, 'l1')
    assert hasattr(TimeDimension.default, 'l2')
    assert list(TimeDimension.h.keys()) == ['default', 'h1']


class TestCube(BaseTestCase):
    def test_cube(self):
        def c(col):
            table, column = col.split('.')
            return self.metadata.tables[table].columns[column]

        class StoreDimension(Dimension):
            region = Level(c('region.region_id'), c('region.region_name'))
            country = Level(c('country.country_id'), c('country.country_name'))
            store = Level(c('store.store_id'), c('store.store_name'))

        class ProductHierarchy(Hierarchy):
            category = Level(c('product_category.product_category_id'),
                             c('product_category.product_category_name'))
            product = Level(c('product.product_id'),
                            c('product.product_name'))

        class ProductDimension(Dimension):
            default = ProductHierarchy

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

        assert list(TestCube.d.keys()) == ['store', 'product', 'time']
        assert list(TestCube.m.keys()) == ['price', 'quantity', 'FACT_COUNT']
        assert isinstance(TestCube, pypet.Cube)
        assert isinstance(TestCube.price, pypet.Measure)
        assert TestCube.price.name == 'price'
        assert TestCube.price.expression == c('facts_table.price')
        assert isinstance(TestCube.quantity, pypet.Measure)
        assert isinstance(TestCube.time, pypet.Dimension)
        # assert isinstance(TestCube.time.default, pypet.Hierarchy)
        # assert isinstance(TestCube.time.default.day, pypet.Level)
        assert isinstance(TestCube.query, pypet.Query)
        assert len(
            TestCube.query.axis(StoreDimension.default.region).execute()) == 2

    def test_cube_nested(self):
        def c(col):
            table, column = col.split('.')
            return self.metadata.tables[table].columns[column]

        class TestCube(Cube):
            __metadata__ = self.metadata
            __fact_table__ = 'facts_table'
            __fact_count_column__ = 'qty'

            class product(Dimension):
                class default(Hierarchy):
                    class category(Level):
                        column = c('product_category.product_category_id')
                        label_column = c(
                            'product_category.product_category_name')

                        def label_expression(self, col):
                            return col + ' %'

                    class product(Level):
                        column = c('product.product_id')
                        label_column = c('product.product_name')

            class store(Dimension):
                region = Level(
                    c('region.region_id'),
                    c('region.region_name'))
                country = Level(
                    c('country.country_id'),
                    c('country.country_name'))
                store = Level(
                    c('store.store_id'),
                    c('store.store_name'))

            time = self.time_dim

            class price(Measure):
                label = 'prix'

            quantity = Measure('qty', agg=aggregates.sum)

        assert isinstance(TestCube, pypet.Cube)
        assert isinstance(TestCube.price, pypet.Measure)
        assert isinstance(TestCube.quantity, pypet.Measure)
        assert isinstance(TestCube.time, pypet.Dimension)
        assert isinstance(TestCube.product, pypet.Dimension)
        assert isinstance(TestCube.product.default, pypet.Hierarchy)
        assert isinstance(TestCube.product.default.category, pypet.Level)
        assert TestCube.product.default.category.name == 'category'
        assert TestCube.product.default.category.label_expression(
            '42') == '42 %'
        assert TestCube.product.default.category.column == c(
            'product_category.product_category_id')
        assert TestCube.product.default.category.label_column == c(
            'product_category.product_category_name')
        assert isinstance(TestCube.store.default, pypet.Hierarchy)
        # assert isinstance(TestCube.time.default, pypet.Hierarchy)
        # assert isinstance(TestCube.time.default.day, pypet.Level)
        assert isinstance(TestCube.query, pypet.Query)
        assert len(
            TestCube.query.axis(TestCube.store.default.region).execute()) == 2


# TODO: rename when it is supposed to work, so that it can be collected again
def fail_inheritence_hierarchy():

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
    assert list(TimeHierarchy.levels.keys()) == ['All', 'l1', 'l2', 'l3']
    assert list(SubTimeHierarchy.levels.keys()) == ['All', 'l1', 'l2', 'l3']
    assert hasattr(SubSubTimeHierarchy, 'l4')
    assert list(SubSubTimeHierarchy.levels.keys()) == ['All', 'l1', 'l2', 'l3', 'l4']

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
