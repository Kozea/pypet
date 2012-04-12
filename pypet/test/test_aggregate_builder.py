from pypet.test import BaseTestCase
from pypet.aggbuilder import AggBuilder


class TestAggregateBuilder(BaseTestCase):

    def test_builder(self):
        c = self.cube
        query = c.query.axis(c.d['time'].l['month'],
                c.d['store'].l['region'])
        facts_table_result = query.execute()
        other_query = c.query.axis(c.d['time'].l['year'])
        facts_table_other_result = other_query.execute()
        builder = AggBuilder(self.cube.query.axis(
            c.d['time'].l['month'], c.d['store'].l['region']))
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
