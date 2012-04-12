from pypet.test import BaseTestCase
from pypet import Aggregate
from pypet import aggregates
from sqlalchemy.sql import func


class TestModel(BaseTestCase):

    def test_dimensions(self):
        assert len(self.cube.dimensions) == 3

    def test_sql(self):
        query = self.cube.query
        res = query.execute()
        scalar = res['All']['All']['All']
        assert scalar['Unit Price'] == 518.867924528302
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
        computed = (self.cube.measures['Unit Price'].aggregate_with(None) *
                self.cube.measures['Quantity']).label('measure')
        query = self.cube.query.measure(computed)
        self.compare_agg(query)
        result = query.execute()
        assert result.keys() == ['All']
        assert result['All'].keys() == ['All']
        assert result['All']['All'].keys() == ['All']
        assert result['All']['All']['All'].measure == 110770

        computed = ((computed / 1000).aggregate_with(aggregates.sum)
                .label('measure'))
        query = self.cube.query.measure(computed)
        self.compare_agg(query)
        result = query.execute()
        assert result['All']['All']['All'].measure == 110770 / 1000.

    def _append_aggregate_by_month(self):
        aggregate = Aggregate(self.agg_by_month_table, {
                    self.cube.d['store'].l['store']:
                        self.agg_by_month_table.c.store_store,
                    self.cube.d['product'].l['product']:
                        self.agg_by_month_table.c.product_product,
                    self.cube.d['time'].l['month']:
                        self.agg_by_month_table.c.time_month},
                    {self.cube.measures['Unit Price']:
                        self.agg_by_month_table.c['Unit Price'],
                     self.cube.measures['Quantity']:
                        self.agg_by_month_table.c.Quantity},
                    fact_count_column=self.agg_by_month_table.c.Quantity)
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
                self.agg_by_year_country_table.c.product_product,
                self.cube.d['time'].l['year']:
                self.agg_by_year_country_table.c.time_year},
             {self.cube.measures['Unit Price']:
                        self.agg_by_year_country_table.c['Unit Price'],
                     self.cube.measures['Quantity']:
                        self.agg_by_year_country_table.c.Quantity},
                fact_count_column=self.agg_by_year_country_table.c.Quantity)

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
        query = (self.cube.query
                    .axis(self.cube.d['time'].l['year'])
                    .filter(self.cube.d['time'].l['year']['2010-01-01']))
        result = query.execute().by_label()
        assert set(result.keys()) == set(['2010'])

    def test_top(self):
        query = (self.cube.query.axis(self.cube.d['time'].l['month'])
                .top(3, self.cube.measures['Price']))
        res = query.execute().by_label()
        self.compare_agg(query)
        assert res.keys() == [u'2011-01', u'2011-05', u'2010-11']
        mes = self.cube.measures['Price'].percent_over(
                    self.cube.d['time'].l['year'])
        query = (self.cube.query.axis(self.cube.d['time'].l['month'])
                .measure(mes)
                .top(3, self.cube.measures['Price']))
        res = query.execute().by_label()
        self.compare_agg(query)
        assert res.keys() == [u'2011-01', u'2011-05', u'2010-11']
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
        assert res.keys() == [u'2010-11', u'2011-01', u'2009-08']

    def test_query_equality(self):
        assert self.cube.query == self.cube.query
        region = self.cube.d['store'].l['region']
        assert self.cube.query.filter(region) == self.cube.query.filter(region)
        assert (self.cube.query.measure(self.cube.measures['Price']) ==
                self.cube.query.measure(self.cube.measures['Price']))
        assert (self.cube.query.filter(region.member_by_label('Europe') ==
                self.cube.query.filter(region.member_by_label('Europe'))))

    def test_members(self):
        regions = self.cube.d['store'].l['region'].members
        assert set([r.label for r in regions]) == set(['Europe', 'America'])
        years = self.cube.d['time'].l['year'].members
        assert set([y.label for y in years]) == set(['2009', '2010', '2011'])
        american_countries = (self.cube.d['store'].l['region']
                .member_by_label('America').children)
        assert set([s.label for s in american_countries]) == set(['USA',
                    'Canada'])
