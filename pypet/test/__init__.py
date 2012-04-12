from sqlalchemy import create_engine, types
from sqlalchemy.schema import MetaData, Table, Column, ForeignKey
from pypet import Cube, Dimension, Hierarchy, Level, Measure
from pypet.util import TimeDimension
from datetime import date
from pypet import aggregates
from sqlalchemy.sql import func
from itertools import cycle, izip
import unittest


class BaseTestCase(unittest.TestCase):

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

        agg_name = ('agg_time_month_product_product_store_store'
                    '_Unit Price_Quantity')
        self.agg_by_month_table = (Table(agg_name,
                self.metadata,
                Column('store_store', types.Integer,
                    ForeignKey('store.store_id')),
                Column('time_month', types.Date),
                Column('product_product', types.Integer,
                    ForeignKey('product.product_id')),
                Column('Price', types.Float),
                Column('Quantity', types.Integer)))
        agg_name = ('agg_time_year_store_country_product_product'
                    '_Unit Price_Quantity')
        self.agg_by_year_country_table = Table(agg_name,
                self.metadata,
                Column('store_country', types.Integer,
                    ForeignKey('country.country_id')),
                Column('time_year', types.Date),
                Column('product_product', types.Integer,
                    ForeignKey('product.product_id')),
                Column('Price', types.Float),
                Column('Quantity', types.Integer))

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

        unit_price = Measure('Unit Price', self.facts_table.c.price,
                aggregates.avg)
        quantity = Measure('Quantity', self.facts_table.c.qty, func.sum)
        price = ((unit_price.aggregate_with(None) *
                quantity.aggregate_with(None))
                .aggregate_with(func.sum).label('Price'))

        self.cube = Cube(self.metadata, self.facts_table, [self.store_dim,
            self.product_dim, self.time_dim], [unit_price, quantity, price],
            fact_count_column=self.facts_table.c.qty)

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
                func.sum(self.facts_table.c.price).label('Price'),
                func.sum(self.facts_table.c.qty).label('Quantity'),
                self.facts_table.c.product_id.label('product_product'),
                self.facts_table.c.store_id.label('store_store'),
                func.date_trunc('month',
                    self.facts_table.c.date).label('time_month')])
            .group_by(func.date_trunc('month', self.facts_table.c.date),
                self.facts_table.c.product_id,
                self.facts_table.c.store_id)
            .execute())
        for res in results:
            self.agg_by_month_table.insert().execute(dict(res))
        second_agg = (self.facts_table.select().with_only_columns([
            func.sum(self.facts_table.c.price).label('Price'),
            func.sum(self.facts_table.c.qty).label('Quantity'),
            self.facts_table.c.product_id.label('product_product'),
            self.store_table.c.country_id.label('store_country'),
            func.date_trunc('year',
                self.facts_table.c.date).label('time_year')])
            .group_by(self.facts_table.c.product_id.label('product_product'),
            self.store_table.c.country_id.label('store_country'),
            func.date_trunc('year',
                self.facts_table.c.date).label('time_year'))
            .execute())
        for res in second_agg:
            self.agg_by_year_country_table.insert().execute(dict(res))

    def tearDown(self):
        self.metadata.drop_all()
