from pypet import Cube, Dimension, Hierarchy, Level, Measure, Member, Aggregate
from pypet.util import TimeDimension
from sqlalchemy.schema import MetaData, Table, Column, ForeignKey
from sqlalchemy.sql import func
from sqlalchemy import create_engine, types
from itertools import cycle, izip
from datetime import date


class TestCase(object):

    def setUp(self):
        engine = create_engine('postgresql://pypet@localhost/pypet', echo=False)
        self.metadata = MetaData(bind=engine)

        self.store_table = Table('store', self.metadata,
                Column('store_id', types.Integer, primary_key=True),
                Column('store_name', types.String),
                Column('country_id', types.Integer,
                    ForeignKey('country.country_id')))

        self.country_table = Table('country', self.metadata,
                Column('country_id', types.Integer, primary_key=True),
                Column('country_name', types.String),
                Column('region_id', types.Integer,
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
                Column('store_id', types.Integer,
                    ForeignKey('store.store_id')),
                Column('date', types.Date),
                Column('product_id', types.Integer,
                    ForeignKey('product.product_id')),
                Column('price', types.Float),
                Column('qty', types.Integer))

        self.agg_by_month_table = Table('agg_by_month', self.metadata,
                Column('store_id', types.Integer,
                    ForeignKey('store.store_id')),
                Column('time_month', types.Date),
                Column('product_id', types.Integer,
                    ForeignKey('product.product_id')),
                Column('price', types.Float),
                Column('qty', types.Integer))

        self.agg_by_year_country_table = Table('agg_by_year_country',
                self.metadata,
                Column('store_country', types.Integer,
                    ForeignKey('country.country_id')),
                Column('time_year', types.Date),
                Column('product_id', types.Integer,
                    ForeignKey('product.product_id')),
                Column('price', types.Float),
                Column('qty', types.Integer))

        self.metadata.create_all()

        self.store_dim = Dimension('store', [
            Hierarchy('default', [
                Level('region', self.region_table.c.region_name),
                Level('country', self.country_table.c.country_name),
                Level('store', self.store_table.c.store_name)])])

        self.product_dim = Dimension('product', [
            Hierarchy('default', [
                Level('category', self.product_category_table.c
                    .product_category_name),
                Level('product', self.product_table.c.product_name)])])

        self.time_dim = TimeDimension('time', self.facts_table.c.date,
                ['year', 'month', 'day'])

        unit_price = Measure('Unit Price', self.facts_table.c.price, func.avg)
        quantity = Measure('Quantity', self.facts_table.c.qty, func.sum)
        price = (unit_price.aggregate_with(None) *
                quantity.aggregate_with(None)).aggregate_with(func.sum).label('Price')

        self.cube = Cube(self.metadata, self.facts_table, [self.store_dim,
            self.product_dim, self.time_dim], [unit_price, quantity, price])

        self.region_table.insert({'region_id': 1, 'region_name':
            'Europe'}).execute()

        self.country_table.insert({'region_id': 1, 'country_name':
            'France', 'country_id': 1}).execute()

        self.country_table.insert({'region_id': 1, 'country_name':
            'Germany', 'country_id': 2}).execute()

        self.region_table.insert({'region_id': 2, 'region_name':
            'America'}).execute()

        self.country_table.insert({'region_id': 2, 'country_name':
            'USA', 'country_id': 3}).execute()

        self.country_table.insert({'region_id': 2, 'country_name':
            'Canada', 'country_id': 4}).execute()


        self.store_table.insert({
            'store_id': 1,
            'store_name': 'ACME.fr',
            'country_id': 1}).execute()

        self.store_table.insert({
            'store_id': 2,
            'store_name': 'ACME.de',
            'country_id': 2}).execute()

        self.store_table.insert({
            'store_id': 3,
            'store_name': 'Food Mart.fr',
            'country_id': 1}).execute()

        self.store_table.insert({
            'store_id': 4,
            'store_name': 'Food Mart.de',
            'country_id': 2}).execute()

        self.store_table.insert({
            'store_id': 5,
            'store_name': 'ACME.us',
            'country_id': 3}).execute()

        self.store_table.insert({
            'store_id': 6,
            'store_name': 'Food Mart.us',
            'country_id': 3}).execute()

        self.store_table.insert({
            'store_id': 7,
            'store_name': 'ACME.ca',
            'country_id': 4}).execute()

        self.store_table.insert({
            'store_id': 8,
            'store_name': 'Food Mart.ca',
            'country_id': 4}).execute()

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
        results = (self.facts_table.select().with_only_columns([
                func.avg(self.facts_table.c.price).label('price'),
                func.sum(self.facts_table.c.qty).label('qty'),
                self.facts_table.c.product_id,
                self.facts_table.c.store_id,
                func.date_trunc('month',
                    self.facts_table.c.date).label('time_month')])
            .group_by(func.date_trunc('month', self.facts_table.c.date),
                self.facts_table.c.product_id,
                self.facts_table.c.store_id)
            .execute())
        for res in results:
            self.agg_by_month_table.insert().execute(dict(res))
        second_agg = (self.facts_table.select().with_only_columns([
            func.avg(self.facts_table.c.price).label('price'),
            func.sum(self.facts_table.c.qty).label('qty'),
            self.facts_table.c.product_id,
            self.store_table.c.country_id.label('store_country'),
            func.date_trunc('year',
                self.facts_table.c.date).label('time_year')])
            .group_by(self.facts_table.c.product_id,
            self.store_table.c.country_id.label('store_country'),
            func.date_trunc('year',
                self.facts_table.c.date).label('time_year'))
            .execute())
        for res in second_agg:
            self.agg_by_year_country_table.insert().execute(dict(res))


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
        expected = (u'SELECT'
            ' %(param_1)s AS store, %(param_2)s AS product,'
            ' %(param_3)s AS time,'
            ' avg(facts_table.price) AS "Unit Price",'
            ' sum(facts_table.qty) AS "Quantity",'
            ' sum(facts_table.price * facts_table.qty) AS "Price"'
            ' \nFROM facts_table')
        assert unicode(query._as_sql()) == expected
        query = query.slice(self.cube['store']['region'])
        expected = (u'SELECT'
            ' region.region_name AS store,'
            ' %(param_1)s AS product,'
            ' %(param_2)s AS time,'
            ' avg(facts_table.price) AS "Unit Price",'
            ' sum(facts_table.qty) AS "Quantity",'
            ' sum(facts_table.price * facts_table.qty) AS "Price"'
            ' \nFROM facts_table JOIN store ON store.store_id ='
            ' facts_table.store_id'
            ' JOIN country ON country.country_id = store.country_id'
            ' JOIN region ON region.region_id = country.region_id'
            ' GROUP BY region.region_name')
        assert unicode(query._as_sql()) == expected
        expected = (u'SELECT'
            ' region.region_name AS store,'
            ' %(param_1)s AS product,'
            ' EXTRACT(year FROM date_trunc(%(date_trunc_1)s,'
            ' facts_table.date)) AS time,'
            ' avg(facts_table.price) AS "Unit Price",'
            ' sum(facts_table.qty) AS "Quantity",'
            ' sum(facts_table.price * facts_table.qty) AS "Price"'
            ' \nFROM facts_table JOIN store ON store.store_id ='
            ' facts_table.store_id'
            ' JOIN country ON country.country_id = store.country_id'
            ' JOIN region ON region.region_id = country.region_id'
            ' GROUP BY region.region_name,'
            ' date_trunc(%(date_trunc_1)s, facts_table.date)')
        query = query.slice(self.cube['time']['year'])
        assert unicode(query._as_sql()) == expected
        second_query = query.slice(self.cube['product']['category'])
        expected = (u'SELECT'
            ' region.region_name AS store,'
            ' product_category.product_category_name AS product,'
            ' EXTRACT(year FROM date_trunc(%(date_trunc_1)s,'
            ' facts_table.date)) AS time,'
            ' avg(facts_table.price) AS "Unit Price",'
            ' sum(facts_table.qty) AS "Quantity",'
            ' sum(facts_table.price * facts_table.qty) AS "Price"'
            ' \nFROM facts_table'
            ' JOIN store ON store.store_id ='
            ' facts_table.store_id'
            ' JOIN country ON country.country_id = store.country_id'
            ' JOIN region ON region.region_id = country.region_id'
            ' JOIN product ON product.product_id ='
            ' facts_table.product_id JOIN product_category ON'
            ' product_category.product_category_id ='
            ' product.product_category_id'
            ' GROUP BY'
            ' region.region_name,'
            ' product_category.product_category_name,'
            ' date_trunc(%(date_trunc_1)s, facts_table.date)'
        )
        assert unicode(second_query._as_sql()) == expected
        query = query.slice(self.cube['product']['category']['product'])
        expected = (u'SELECT'
            ' region.region_name AS store,'
            ' product.product_name AS product,'
            ' EXTRACT(year FROM date_trunc(%(date_trunc_1)s,'
            ' facts_table.date)) AS time,'
            ' avg(facts_table.price) AS "Unit Price",'
            ' sum(facts_table.qty) AS "Quantity",'
            ' sum(facts_table.price * facts_table.qty) AS "Price"'
            ' \nFROM facts_table'
            ' JOIN store ON store.store_id ='
            ' facts_table.store_id'
            ' JOIN country ON country.country_id = store.country_id'
            ' JOIN region ON region.region_id = country.region_id'
            ' JOIN product ON product.product_id ='
            ' facts_table.product_id'
            ' GROUP BY'
            ' region.region_name,'
            ' product.product_name,'
            ' date_trunc(%(date_trunc_1)s, facts_table.date)'
        )
        assert unicode(query._as_sql()) == expected

    def test_result(self):
        results = self.cube.query.execute()
        assert results.dims.keys() == [self.store_dim, self.product_dim,
                self.time_dim]
        assert results.keys() == ['All']
        assert results['All'].keys() == ['All']
        assert results['All']['All'].keys() == ['All']
        assert results['All']['All']['All'].Price == 110000
        results = self.cube.query.slice(self.cube['time']['year']).execute()
        assert results['All']['All'].keys() == [2009, 2010, 2011]
        results = self.cube.query.axis(self.cube['time']['year']).execute()
        assert results.keys() == [2009, 2010, 2011]

    def test_measures(self):
        computed = self.cube.measures['Price']
        query = (self.cube.query.measure((computed /
                computed.over(self.cube['store']['region']) *
                100).label('CA_percent_by_region'))
            .axis(self.cube['store']['region']['country']['store']))
        query._as_sql()
        result = query.execute()
        assert result.keys() == [u'ACME.ca', u'ACME.de', u'ACME.fr', u'ACME.us',
                u'Food Mart.ca', u'Food Mart.de', u'Food Mart.fr',
                u'Food Mart.us']

        assert result['ACME.fr']['CA_percent_by_region'] == 15.1202749140893

        # Avg price * total quantity
        computed = (self.cube.measures['Unit Price'] *
                self.cube.measures['Quantity']).label('measure')
        result = self.cube.query.measure(computed).execute()
        assert result.keys() == ['All']
        assert result['All'].keys() == ['All']
        assert result['All']['All'].keys() == ['All']
        assert result['All']['All']['All'].measure == 110770


        # Test the same queries, using an aggregate
        self._append_aggregate_by_month()

        computed = self.cube.measures['Price']
        query = (self.cube.query.measure((computed /
                computed.over(self.cube['store']['region']) *
                100).label('CA_percent_by_region'))
            .axis(self.cube['store']['region']['country']['store']))
        assert 'agg_by_month' in unicode(query._as_sql())
        result = query.execute()
        assert result.keys() == [u'ACME.ca', u'ACME.de', u'ACME.fr', u'ACME.us',
                u'Food Mart.ca', u'Food Mart.de', u'Food Mart.fr',
                u'Food Mart.us']

        assert result['ACME.fr']['CA_percent_by_region'] == 15.1202749140893

        computed = (self.cube.measures['Unit Price'] *
                self.cube.measures['Quantity']).label('measure')
        query = self.cube.query.measure(computed)
        assert 'agg_by_month' in unicode(query._as_sql())
        result = query.execute()
        assert result.keys() == ['All']
        assert result['All'].keys() == ['All']
        assert result['All']['All'].keys() == ['All']
        assert result['All']['All']['All'].measure == 110770


    def _append_aggregate_by_month(self):
        aggregate = Aggregate(self.agg_by_month_table, {
                    self.cube['store']['region']['country']['store']:
                        self.agg_by_month_table.c.store_id,
                    self.cube['product']['category']['product']:
                        self.agg_by_month_table.c.product_id,
                    self.cube['time']['year']['month']:
                        self.agg_by_month_table.c.time_month},
                    {self.cube.measures['Unit Price']:
                        self.agg_by_month_table.c.price,
                     self.cube.measures['Quantity']:
                        self.agg_by_month_table.c.qty})
        self.cube.aggregates.append(aggregate)

    def test_agg(self):
        self._append_aggregate_by_month()
        query = self.cube.query.slice(self.cube['time']['year']['month'])
        expected = ('SELECT'
            ' %(param_1)s AS store,'
            ' %(param_2)s AS product,'
            ' EXTRACT(month FROM date_trunc(%(date_trunc_1)s,'
            ' agg_by_month.time_month)) AS time,'
            ' avg(agg_by_month.price) AS "Unit Price",'
            ' sum(agg_by_month.qty) AS "Quantity",'
            ' sum(agg_by_month.price * agg_by_month.qty) AS "Price"'
            ' \nFROM agg_by_month'
            ' GROUP BY date_trunc(%(date_trunc_1)s, agg_by_month.time_month)')
        assert unicode(query._as_sql()) == expected
        query = self.cube.query.slice(self.cube['time']['year'])
        expected = ('SELECT'
            ' %(param_1)s AS store,'
            ' %(param_2)s AS product,'
            ' EXTRACT(year FROM date_trunc(%(date_trunc_1)s,'
            ' agg_by_month.time_month)) AS time,'
            ' avg(agg_by_month.price) AS "Unit Price",'
            ' sum(agg_by_month.qty) AS "Quantity",'
            ' sum(agg_by_month.price * agg_by_month.qty) AS "Price"'
            ' \nFROM agg_by_month'
            ' GROUP BY date_trunc(%(date_trunc_1)s, agg_by_month.time_month)')
        assert unicode(query._as_sql()) == expected
        query = self.cube.query.slice(self.cube['time']
                ['year']['month']['day'])
        expected = (u'SELECT'
            ' %(param_1)s AS store,'
            ' %(param_2)s AS product,'
            ' EXTRACT(day FROM date_trunc(%(date_trunc_1)s,'
            ' facts_table.date)) AS time,'
            ' avg(facts_table.price) AS "Unit Price",'
            ' sum(facts_table.qty) AS "Quantity", sum(facts_table.price *'
            ' facts_table.qty)'
            ' AS "Price"'
            ' \nFROM facts_table'
            ' GROUP BY date_trunc(%(date_trunc_1)s, facts_table.date)')
        assert unicode(query._as_sql()) == expected

        agg_by_year_country = Aggregate(self.agg_by_year_country_table, {
            self.cube['store']['region']['country']:
                self.agg_by_year_country_table.c.store_country,
            self.cube['product']['category']['product']:
                self.agg_by_year_country_table.c.product_id,
                self.cube['time']['year']:
                self.agg_by_year_country_table.c.time_year},
             {self.cube.measures['Unit Price']:
                        self.agg_by_year_country_table.c.price,
                     self.cube.measures['Quantity']:
                        self.agg_by_year_country_table.c.qty,
                     self.cube.measures['Price']:
                        self.agg_by_year_country_table.c.price *
                        self.agg_by_year_country_table.c.qty})
        self.cube.aggregates.append(agg_by_year_country)
        query = self.cube.query.slice(self.cube['time']['year'])
        expected = ('SELECT'
            ' %(param_1)s AS store,'
            ' %(param_2)s AS product,'
            ' EXTRACT(year FROM date_trunc(%(date_trunc_1)s,'
            ' agg_by_year_country.time_year)) AS time,'
            ' avg(agg_by_year_country.price) AS "Unit Price",'
            ' sum(agg_by_year_country.qty) AS "Quantity",'
            ' sum(agg_by_year_country.price * agg_by_year_country.qty)'
            ' AS "Price"'
            ' \nFROM agg_by_year_country'
            ' GROUP BY'
            ' date_trunc(%(date_trunc_1)s, agg_by_year_country.time_year)')
        assert unicode(query._as_sql()) == expected
        query = self.cube.query.slice(self.cube['time']['2010'])
        expected = ('SELECT'
            ' %(param_1)s AS store,'
            ' %(param_2)s AS product,'
            ' %(param_3)s AS time,'
            ' avg(agg_by_year_country.price) AS "Unit Price",'
            ' sum(agg_by_year_country.qty) AS "Quantity",'
            ' sum(agg_by_year_country.price * agg_by_year_country.qty)'
            ' AS "Price"'
            ' \nFROM agg_by_year_country'
            ' \nWHERE'
            ' date_trunc(%(date_trunc_1)s, agg_by_year_country.time_year) ='
            ' %(date_trunc_2)s')
        assert unicode(query._as_sql()) == expected

        query = self.cube.query.slice(self.cube['store']['region'])
        expected = (u'SELECT'
            ' region.region_name AS store,'
            ' %(param_1)s AS product,'
            ' %(param_2)s AS time,'
            ' avg(agg_by_year_country.price) AS "Unit Price",'
            ' sum(agg_by_year_country.qty) AS "Quantity",'
            ' sum(agg_by_year_country.price * agg_by_year_country.qty)'
            ' AS "Price"'
            ' \nFROM agg_by_year_country'
            ' JOIN country ON country.country_id ='
            ' agg_by_year_country.store_country'
            ' JOIN region ON region.region_id = country.region_id'
            ' GROUP BY region.region_name')
        assert unicode(query._as_sql()) == expected

        query = self.cube.query.axis(self.cube['store']['region'],
                self.cube['product']['category']['product'])

        expected = (u'SELECT'
            ' region.region_name AS store,'
            ' product.product_name AS product,'
            ' avg(agg_by_year_country.price) AS "Unit Price",'
            ' sum(agg_by_year_country.qty) AS "Quantity",'
            ' sum(agg_by_year_country.price * agg_by_year_country.qty)'
            ' AS "Price"'
            ' \nFROM agg_by_year_country'
            ' JOIN country ON country.country_id ='
            ' agg_by_year_country.store_country'
            ' JOIN region ON region.region_id = country.region_id'
            ' JOIN product ON product.product_id ='
            ' agg_by_year_country.product_id'
            ' GROUP BY region.region_name, product.product_name')
        assert unicode(query._as_sql()) == expected


    def test_filters(self):
        query = self.cube.query.filter(self.cube['time'][date(year=2010,
            month=1, day=1)])
        assert query.execute()['All']['All']['All']['Price'] == 30000
        computed = self.cube.measures['Price']
        query = (self.cube.query.measure((computed /
                computed.over(self.cube['store']['region']) *
                100).label('CA_percent_by_region'))
            .axis(self.cube['store']['region']['country']['store'])
            .filter(self.cube['store']['Europe']))
        result = query.execute()
        assert result.keys() == [u'ACME.de', u'ACME.fr', u'Food Mart.de',
            u'Food Mart.fr']
        assert result['ACME.fr']['CA_percent_by_region'] == 15.1202749140893

    def test_query_equality(self):
        assert self.cube.query == self.cube.query
        assert (self.cube.query.filter(self.cube['store']['region']) ==
                self.cube.query.filter(self.cube['store']['region']))
        assert (self.cube.query.measure(self.cube.measures['Price']) ==
                self.cube.query.measure(self.cube.measures['Price']))

    def tearDown(self):
        self.metadata.drop_all()
