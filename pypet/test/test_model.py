from pypet import Cube, Dimension, Hierarchy, Level, Measure, Aggregate, Filter
from pypet.util import TimeDimension
from sqlalchemy.schema import MetaData, Table, Column, ForeignKey
from sqlalchemy.sql import func, operators
from sqlalchemy import create_engine, types
from itertools import cycle, izip
from datetime import date


class TestCase(object):

    def setUp(self):
        engine = create_engine('postgresql://pypet@localhost/pypet')
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
                Level('region', self.region_table.c.region_id,
                    self.region_table.c.region_name),
                Level('country', self.country_table.c.country_id,
                    self.country_table.c.country_name),
                Level('store', self.store_table.c.store_id,
                    self.store_table.c.store_name)])])

        self.product_dim = Dimension('product', [
            Hierarchy('default', [
                Level('category',
                    self.product_category_table.c.product_category_id,
                    self.product_category_table.c
                    .product_category_name),
                Level('product', self.product_table.c.product_id,
                    self.product_table.c.product_name)])])

        self.time_dim = TimeDimension('time', self.facts_table.c.date,
                ['year', 'month', 'day'])

        unit_price = Measure('Unit Price', self.facts_table.c.price, func.avg)
        quantity = Measure('Quantity', self.facts_table.c.qty, func.sum)
        price = ((unit_price.aggregate_with(None) *
                quantity.aggregate_with(None))
                .aggregate_with(func.sum).label('Price'))

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

    def test_sql(self):
        query = self.cube.query
        res = query.execute()
        scalar = res['All']['All']['All']
        assert scalar['Unit Price'] == 522.5
        assert scalar['Quantity'] == 212
        assert scalar['Price'] == 110000

        query = query.slice(self.cube.d['store'].l['region'])
        res = query.execute()
        query = query.slice(self.cube.d['time'].l['year'])
        query.execute()
        second_query = query.slice(self.cube.d['product'].l['category'])
        second_query.execute()
        query = query.slice(self.cube.d['product'].l['product'])
        query.execute()

    def test_result(self):
        results = self.cube.query.execute()
        assert results.keys() == ['All']
        assert results['All'].keys() == ['All']
        assert results['All']['All'].keys() == ['All']
        assert results['All']['All']['All'].Price == 110000
        results = (self.cube.query.slice(self.cube.d['time'].l['year'])
                    .execute())
        assert results['All']['All'].by_label().keys() == ['2009', '2010',
            '2011']
        results = self.cube.query.axis(self.cube.d['time'].l['year']).execute()
        assert results.by_label().keys() == ['2009', '2010', '2011']

    def test_measures(self):
        computed = self.cube.measures['Price']
        query = (self.cube.query.measure((computed /
                computed.over(self.cube.d['store'].l['region']) *
                100).label('CA_percent_by_region'))
            .axis(self.cube.d['store'].l['store']))
        query._as_sql()
        result = query.execute().by_label()
        self.compare_agg(query)
        assert set(result.keys()) == set([u'ACME.ca', u'ACME.de',
            u'ACME.fr', u'ACME.us', u'Food Mart.ca', u'Food Mart.de',
            u'Food Mart.fr', u'Food Mart.us'])

        assert result['ACME.fr']['CA_percent_by_region'] == 15.1202749140893

        # Avg price * total quantity
        computed = (self.cube.measures['Unit Price'] *
                self.cube.measures['Quantity']).label('measure')
        query = self.cube.query.measure(computed)
        self.compare_agg(query)
        result = query.execute()
        assert result.keys() == ['All']
        assert result['All'].keys() == ['All']
        assert result['All']['All'].keys() == ['All']
        assert result['All']['All']['All'].measure == 110770

        computed = ((computed / 1000).aggregate_with(func.sum)
                .label('measure'))
        query = self.cube.query.measure(computed)
        self.compare_agg(query)
        result = query.execute()
        assert result['All']['All']['All'].measure == 110770 / 1000.

    def _append_aggregate_by_month(self):
        aggregate = Aggregate(self.agg_by_month_table, {
                    self.cube.d['store'].l['store']:
                        self.agg_by_month_table.c.store_id,
                    self.cube.d['product'].l['product']:
                        self.agg_by_month_table.c.product_id,
                    self.cube.d['time'].l['month']:
                        self.agg_by_month_table.c.time_month},
                    {self.cube.measures['Unit Price']:
                        self.agg_by_month_table.c.price,
                     self.cube.measures['Quantity']:
                        self.agg_by_month_table.c.qty})
        self.cube.aggregates.append(aggregate)

    def compare_agg(self, query, used_agg=None):
        """Execute a query, with and without aggregation, and compare the
        results."""
        agg = self.cube.aggregates
        self.cube.aggregates = []
        res = query.execute()
        self._append_aggregate_by_month()
        aggres = query.execute()
        if used_agg is not None:
            assert used_agg in query._as_sql()._froms
        assert res == aggres
        self.cube.aggregates = agg


    def test_agg(self):
        query = self.cube.query.slice(self.cube.d['time'].l['month'])
        self.compare_agg(query, self.agg_by_month_table)
        query = self.cube.query.slice(self.cube.d['time'].l['year'])
        self.compare_agg(query, self.agg_by_month_table)
        query = self.cube.query.slice(self.cube.d['time'].l['day'])
        self.compare_agg(query, self.facts_table)
        agg_by_year_country = Aggregate(self.agg_by_year_country_table, {
            self.cube.d['store'].l['country']:
                self.agg_by_year_country_table.c.store_country,
                self.cube.d['product'].l['product']:
                self.agg_by_year_country_table.c.product_id,
                self.cube.d['time'].l['year']:
                self.agg_by_year_country_table.c.time_year},
             {self.cube.measures['Unit Price']:
                        self.agg_by_year_country_table.c.price,
                     self.cube.measures['Quantity']:
                        self.agg_by_year_country_table.c.qty,
                     self.cube.measures['Price']:
                        self.agg_by_year_country_table.c.price *
                        self.agg_by_year_country_table.c.qty})

        query = self.cube.query.slice(self.cube.d['time'].l['year'])
        res = query.execute()
        self.cube.aggregates.append(agg_by_year_country)
        newres = query.execute()
        assert res == newres
        assert self.agg_by_year_country_table in query._as_sql()._froms
        self.cube.aggregates = []
        query = self.cube.query.slice(self.cube.d['time'].l['year']
                .member_by_label('2010'))
        res = query.execute()
        self.cube.aggregates.append(agg_by_year_country)
        newres = query.execute()
        assert res == newres
        assert self.agg_by_year_country_table in query._as_sql()._froms

        self.cube.aggregates = []
        query = self.cube.query.slice(self.cube.d['store'].l['region'])
        res = query.execute()
        self.cube.aggregates.append(agg_by_year_country)
        newres = query.execute()
        assert res == newres
        assert self.agg_by_year_country_table in query._as_sql()._froms

        self.cube.aggregates = []
        query = self.cube.query.axis(self.cube.d['store'].l['region'],
                self.cube.d['product'].l['product'])
        res = query.execute()
        self.cube.aggregates.append(agg_by_year_country)
        newres = query.execute()
        assert res == newres
        assert self.agg_by_year_country_table in query._as_sql()._froms

    def test_filters(self):
        query1 = (self.cube.query
                .filter(self.cube.d['time'].l['year'].member_by_label('2010')))
        assert query1.execute()['All']['All']['All']['Price'] == 30000
        self.compare_agg(query1)
        computed = self.cube.measures['Price']
        query2 = (self.cube.query.measure((computed /
                computed.over(self.cube.d['store'].l['region']) *
                100).label('CA_percent_by_region'))
            .axis(self.cube.d['store'].l['store'])
            .filter(self.cube.d['store'].l['region']
                    .member_by_label('Europe')))
        result = query2.execute().by_label()
        self.compare_agg(query2)
        assert set(result.keys()) == set([u'ACME.de', u'ACME.fr',
            u'Food Mart.de', u'Food Mart.fr'])
        assert result['ACME.fr']['CA_percent_by_region'] == 15.1202749140893

    def test_top(self):
        query = (self.cube.query.axis(self.cube.d['time'].l['month'])
                .top(3, self.cube.measures['Price']))
        res = query.execute().by_label()
        self.compare_agg(query)
        assert res.keys() == [u'2010-11', u'2011-01', u'2011-05']
        mes = self.cube.measures['Price'].percent_over(
                    self.cube.d['time'].l['year'])
        query = (self.cube.query.axis(self.cube.d['time'].l['month'])
                .measure(mes)
                .top(3, self.cube.measures['Price']))
        res = query.execute().by_label()
        self.compare_agg(query)
        assert res.keys() == [u'2010-11', u'2011-01', u'2011-05']
        query = (self.cube.query.axis(self.cube.d['time'].l['month'])
                .measure(mes)
                .top(2, mes, partition_by=self.cube.d['time'].l['year']))
        res = query.execute().by_label()
        self.compare_agg(query)
        # Top 2 by year = 6 entries
        assert len(res) == 6
        # Top 3 (in percent by year) of all time
        query = (self.cube.query.axis(self.cube.d['time'].l['month'])
                .measure(mes)
                .top(3, mes))
        res = query.execute().by_label()
        self.compare_agg(query)
        assert res.keys() == [u'2009-08', u'2010-11', u'2011-01']

    def test_query_equality(self):
        assert self.cube.query == self.cube.query
        region = self.cube.d['store'].l['region']
        assert self.cube.query.filter(region) == self.cube.query.filter(region)
        assert (self.cube.query.measure(self.cube.measures['Price']) ==
                self.cube.query.measure(self.cube.measures['Price']))
        assert (self.cube.query.filter(region.member_by_label('Europe') ==
                self.cube.query.filter(region.member_by_label('Europe'))))

    def tearDown(self):
        self.metadata.drop_all()
