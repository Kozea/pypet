from pypet.test import BaseTestCase
from pypet.aggbuilder import AggBuilder, reflect_aggregates


class TestAggregateBuilder(BaseTestCase):

    def test_builder(self):
        return
        c = self.cube
        query = c.query.axis(c.d['time'].l['month'],
                c.d['store'].l['region'])
        facts_table_result = query.execute()
        other_query = c.query.axis(c.d['time'].l['year'])
        facts_table_other_result = other_query.execute()
        builder = AggBuilder(query)
        builder.build()
        agg_expected_name = ('agg_time_month_store_region_'
                            'Unit Price_Quantity_Price')
        assert ([agg.selectable.name for agg in c.aggregates] ==
                [agg_expected_name])
        sql_query = str(query._as_sql())
        assert agg_expected_name in sql_query
        assert c.table.name not in sql_query
        assert query.execute() == facts_table_result
        sql_query = str(other_query._as_sql())
        assert agg_expected_name in sql_query
        assert c.table.name not in sql_query
        assert other_query.execute() == facts_table_other_result

    def test_matching(self):
        return
        c = self.cube
        query = c.query.axis(c.d['time'].l['month'],
                c.d['store'].l['region'])
        facts_table_result = query.execute()
        other_query = c.query.axis(c.d['time'].l['year'])
        facts_table_other_result = other_query.execute()
        reflect_aggregates(self.cube)
        # We should have found two aggregate/hs
        assert len(self.cube.aggregates) == 2
        month, year = sorted(self.cube.aggregates,
                key=lambda x: x.selectable.name)
        assert set(month.levels.keys()) == set([
                self.cube.d['time'].l['month'],
                self.cube.d['product'].l['product'],
                self.cube.d['store'].l['store']])
        assert set(year.levels.keys()) == set([
                self.cube.d['time'].l['year'],
                self.cube.d['product'].l['product'],
                self.cube.d['store'].l['country']])
        assert set(month.measures.keys()) == set(['Unit Price', 'Quantity'])
        assert set(year.measures.keys()) == set(['Unit Price', 'Quantity'])
        sql_query = str(query._as_sql())
        assert self.agg_by_month_table.name in sql_query
        assert c.table.name not in sql_query
        assert query.execute() == facts_table_result
        sql_query = str(other_query._as_sql())
        assert self.agg_by_year_country_table.name in sql_query
        assert c.table.name not in sql_query
        assert other_query.execute() == facts_table_other_result

    def test_triggers(self):
        c = self.cube
        query = c.query.axis(c.d['time'].l['month'],
                c.d['store'].l['region'])
        builder = AggBuilder(query)
        builder.build(with_trigger=True)

