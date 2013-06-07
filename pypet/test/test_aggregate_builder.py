from pypet.test import BaseTestCase
from pypet.aggbuilder import AggBuilder, reflect_aggregates


class TestAggregateBuilder(BaseTestCase):

    def test_builder(self):
        c = self.cube
        query = c.query.axis(c.d['time'].l['month'],
                c.d['store'].l['region'])
        facts_table_result = query.execute()
        other_query = c.query.axis(c.d['time'].l['year'])
        facts_table_other_result = other_query.execute()
        builder = AggBuilder(query)
        builder.build()
        agg_expected_name = 'agg_time_month_store_region'
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


class TestTriggers(BaseTestCase):

    def test_triggers(self, schema=None):
        c = self.cube
        query = c.query.axis(c.d['time'].l['month'],
                c.d['store'].l['region'],
                c.d['product'].l['All'])
        builder = AggBuilder(query)
        builder.build(with_trigger=True, schema=schema)
        old_total_qty = c.query.axis().execute()["Quantity"]
        # Test with a value already in the agg table
        c.table.insert({
            'store_id': 1,
            'product_id': 2,
            'date': '2009-01-12',
            'qty': 200,
            'price': 1000}).execute()
        oldaggs = c.aggregates
        c.aggregates = []
        result = c.query.axis().execute()
        c.aggregates = oldaggs
        newresult = c.query.axis().execute()

        assert 'facts_table' not in str(c.query._as_sql())
        assert newresult == result
        assert newresult["Quantity"] == old_total_qty + 200
        # Test with a new value (insert statement)
        c.table.insert({
            'store_id': 1,
            'product_id': 2,
            'date': '2020-01-12',
            'qty': 200,
            'price': 1000}).execute()
        oldaggs = c.aggregates
        c.aggregates = []
        result = c.query.axis().execute()
        c.aggregates = oldaggs
        newresult = c.query.axis().execute()
        assert 'facts_table' not in str(c.query._as_sql())
        assert newresult == result
        assert newresult["Quantity"] == old_total_qty + 400
        res = c.query.axis(c.d['time'].l['year']).filter(
                c.d['time'].l['year'].member_by_label('2020')).execute()
        assert len(res) == 1
        assert res.by_label()['2020'].Quantity == 200

    def test_in_schema(self):
        self.test_triggers(schema='aggregates')

    def setUp(self):
        self.schema = None
        super(TestTriggers, self).setUp()
        self.cube.table.bind.execute('CREATE SCHEMA aggregates')

    def tearDown(self):
        self.cube.table.bind.execute('DROP SCHEMA aggregates CASCADE');
        if self.schema is None:
            fn_name = 'trigger_function_agg_time_month_store_region_product_All'
            self.cube.table.bind.execute('DROP FUNCTION IF EXISTS "ins_%s"() CASCADE' % fn_name)
            self.cube.table.bind.execute('DROP FUNCTION IF EXISTS "upd_%s"() CASCADE' % fn_name)
        super(TestTriggers, self).tearDown()
