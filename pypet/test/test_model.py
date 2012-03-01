from pypet import Cube, Dimension, Hierarchy, Level, Measure, Member
from sqlalchemy.schema import MetaData, Table, Column, ForeignKey
from sqlalchemy.sql import func
from sqlalchemy import create_engine, types
from itertools import cycle, izip
from datetime import date

class TestCase(object):

    def setUp(self):
        engine = create_engine('sqlite:///')
        self.metadata = MetaData(bind=engine)
        self.store_table = Table('store', self.metadata,
                Column('store_id', types.Integer, primary_key=True),
                Column('store_name', types.String),
                Column('store_region', types.Integer,
                    ForeignKey('region.region_id')))
        self.region_table = Table('region', self.metadata,
                Column('region_id', types.Integer, primary_key=True),
                Column('region_name', types.String))
        self.product_table = Table('product', self.metadata,
                Column('product_id', types.Integer, primary_key=True),
                Column('product_name', types.String),
                Column('product_category_id', types.Integer,
                   ForeignKey('product_category.product_category_id')))
        self.product_category_table = Table('product_category', self.metadata,
                Column('product_category_id', types.Integer, primary_key=True),
                Column('product_category_name', types.String))
        self.facts_table = Table('facts_table', self.metadata,
                Column('store_id', types.Integer, ForeignKey('store.store_id')),
                Column('date', types.Date),
                Column('product_id', types.Integer,
                    ForeignKey('product.product_id')),
                Column('price', types.Integer),
                Column('qty', types.Integer))
        self.metadata.create_all()

        self.store_dim = Dimension('store', [
            Hierarchy('default', [
                Level('region', self.region_table.c.region_name),
                Level('store', self.store_table.c.store_name)])])

        self.product_dim = Dimension('product', [
            Hierarchy('default', [
                Level('category', self.product_category_table.c
                    .product_category_name),
                Level('product', self.product_table.c.product_name)])])

        self.time_dim = Dimension('time', [
            Hierarchy('default', [
                Level('year', func.strftime('%Y',
                    self.facts_table.c.date)),
                Level('month', func.strftime('%Y %m',
                    self.facts_table.c.date)),
                Level('day', func.strftime('%Y %m %d',
                    self.facts_table.c.date))])])

        self.cube = Cube(self.metadata, self.facts_table, [self.store_dim,
            self.product_dim, self.time_dim], [
                Measure('Unit Price', self.facts_table.c.price, func.avg),
                Measure('Quantity', self.facts_table.c.qty, func.sum),
                Measure('Price', self.facts_table.c.price *
                    self.facts_table.c.qty, func.sum)])

        self.region_table.insert({'region_id': 1, 'region_name':
            'Europe'}).execute()
        self.region_table.insert({'region_id': 2, 'region_name':
            'USA'}).execute()
        self.store_table.insert({
            'store_id': 1,
            'store_name': 'ACME.eu',
            'store_region': 1}).execute()
        self.store_table.insert({
            'store_id': 2,
            'store_name': 'Food Mart.eu',
            'store_region': 1}).execute()
        self.store_table.insert({
            'store_id': 3,
            'store_name': 'ACME.us',
            'store_region': 2}).execute()
        self.store_table.insert({
            'store_id': 4,
            'store_name': 'Food Mart.us',
            'store_region': 2}).execute()

        self.product_category_table.insert({
            'product_category_id': 1,
            'product_category_name': 'Vegetables'}).execute()
        self.product_category_table.insert({
            'product_category_id': 2,
            'product_category_name': 'Shoes'}).execute()


        self.product_table.insert({
            'product_id': 1,
            'product_category_id': 1,
            'product_name': 'Carrots'}).execute()
        self.product_table.insert({
            'product_id': 2,
            'product_category_id': 1,
            'product_name': 'Bananas'}).execute()
        self.product_table.insert({
            'product_id': 3,
            'product_category_id': 2,
            'product_name': 'Red shoes'}).execute()
        self.product_table.insert({
            'product_id': 4,
            'product_category_id': 2,
            'product_name': 'Green shoes'}).execute()
        self.product_table.insert({
            'product_id': 5,
            'product_category_id': 2,
            'product_name': 'Blue shoes'}).execute()

        years = cycle([2009, 2010, 2011])
        months = cycle([1, 5, 8, 9, 11])
        days = cycle([3, 12, 21, 29])
        prices = iter(cycle([100, 500, 1000]))
        quantities = iter(cycle([1, 5, 1, 2, 3, 20, 8]))
        values = iter((date(*value) for value in izip(years, months, days)))
        for value in self.product_table.select().with_only_columns([
            self.product_table.c.product_id,
            self.store_table.c.store_id]).execute():
            self.facts_table.insert({
                'product_id': value.product_id,
                'store_id': value.store_id,
                'date': next(values),
                'qty': next(quantities),
                'price': next(prices)}).execute()

    def test_dimensions(self):
        assert len(self.cube.dimensions) == 3
        assert self.cube['store'] == self.store_dim.default_levels[0]
        assert self.cube['store']['region'] == self.store_dim.default_levels[1]
        acme_member = self.cube['store']['ACME']
        assert isinstance(acme_member, Member)
        assert acme_member.name == 'ACME'
        assert acme_member.level == self.cube['store']['region']

    def test_sql(self):
        query = self.cube.query
        expected = (u'SELECT avg(facts_table.price) AS "Unit Price",'
            ' sum(facts_table.qty) AS "Quantity", sum(facts_table.price *'
            ' facts_table.qty)'
            ' AS "Price",'
            ' ? AS store, ? AS product, ? AS time '
            '\nFROM facts_table')
        assert unicode(query._as_sql()) == expected
        query = query.slice(self.cube['store']['region'])
        expected = (u'SELECT avg(facts_table.price) AS "Unit Price",'
            ' sum(facts_table.qty) AS "Quantity", sum(facts_table.price *'
            ' facts_table.qty)'
            ' AS "Price", region.region_name AS store,'
            ' ? AS product, ? AS time '
            '\nFROM facts_table JOIN store ON store.store_id ='
            ' facts_table.store_id JOIN region ON region.region_id ='
            ' store.store_region GROUP BY region.region_name')
        assert unicode(query._as_sql()) == expected
        expected = (u'SELECT avg(facts_table.price) AS "Unit Price",'
            ' sum(facts_table.qty) AS "Quantity", sum(facts_table.price *'
            ' facts_table.qty)'
            ' AS "Price", region.region_name AS store,'
            ' ? AS product,'
            ' strftime(?, facts_table.date) AS time '
            '\nFROM facts_table JOIN store ON store.store_id ='
            ' facts_table.store_id JOIN region ON region.region_id ='
            ' store.store_region GROUP BY region.region_name,'
            ' strftime(?, facts_table.date)')
        query = query.slice(self.cube['time']['year'])
        assert unicode(query._as_sql()) == expected
        second_query = query.slice(self.cube['product']['category'])
        expected = (u'SELECT avg(facts_table.price) AS "Unit Price",'
            ' sum(facts_table.qty) AS "Quantity", sum(facts_table.price *'
            ' facts_table.qty)'
            ' AS "Price",'
            ' region.region_name AS store,'
            ' product_category.product_category_name AS product,'
            ' strftime(?, facts_table.date) AS time '
            '\nFROM facts_table'
            ' JOIN store ON store.store_id ='
            ' facts_table.store_id JOIN region ON region.region_id ='
            ' store.store_region'
            ' JOIN product ON product.product_id ='
            ' facts_table.product_id JOIN product_category ON'
            ' product_category.product_category_id ='
            ' product.product_category_id'
            ' GROUP BY'
            ' region.region_name,'
            ' product_category.product_category_name,'
            ' strftime(?, facts_table.date)'
        )
        assert unicode(second_query._as_sql()) == expected
        query = query.slice(self.cube['product']['category']['product'])
        expected = (u'SELECT avg(facts_table.price) AS "Unit Price",'
            ' sum(facts_table.qty) AS "Quantity", sum(facts_table.price *'
            ' facts_table.qty)'
            ' AS "Price",'
            ' region.region_name AS store,'
            ' product.product_name AS product,'
            ' strftime(?, facts_table.date) AS time '
            '\nFROM facts_table'
            ' JOIN store ON store.store_id ='
            ' facts_table.store_id JOIN region ON region.region_id ='
            ' store.store_region'
            ' JOIN product ON product.product_id ='
            ' facts_table.product_id'
            ' GROUP BY'
            ' region.region_name,'
            ' product.product_name,'
            ' strftime(?, facts_table.date)'
        )
        assert unicode(query._as_sql()) == expected

    def test_result(self):
        results = self.cube.query.execute()
        assert results.dims.keys() == [self.store_dim, self.product_dim,
                self.time_dim]
        assert results.keys() == ['All']
        assert results['All'].keys() == ['All']
        assert results['All']['All'].keys() == ['All']
        assert results['All']['All']['All'].Price == 64100
        results = self.cube.query.slice(self.cube['time']['year']).execute()
        assert results['All']['All'].keys() == [u'2009', u'2010', u'2011']

    def tearDown(self):
        self.metadata.drop_all()
