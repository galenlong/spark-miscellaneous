"""
Given an RDD of dictionaries and a column to decile,
add decile of column to each row's dictionary by
pre-computing decile thresholds to avoid out of memory errors
when sorting an RDD with too many columns.
"""

from pyspark import SparkContext, SQLContext#, HiveContext
from datetime import datetime
sc = SparkContext()
sqlContext = SQLContext(sc)
# hiveContext = HiveContext(sc)
sc.setLogLevel("FATAL")

#
# setup
#

import numpy as np

# create random data
n = 52
prices = [float(list(5 + abs(np.random.randn(1)) * 100)[0]) 
	for i in range(n)]
dates = [datetime(year=np.random.randint(2000, 2016), 
	month=np.random.randint(1, 12), 
	day=np.random.randint(1, 28)).date() for i in range(n)]
groups = [np.random.randint(1, 100) for i in range(n)]
data = [{"price": price, "date": _date, "group": group} 
	for price, _date, group in zip(prices, dates, groups)]
df = sqlContext.createDataFrame(data)

print('df initial')
df.show()

# convert to rdd of dicts
rdd = df.rdd
rdd = rdd.map(lambda x: x.asDict())

#
# get deciles
#

total_num_rows = rdd.count()
column_to_decile = 'price'


def add_to_dict(_dict, key, value):
	_dict[key] = value
	return _dict


# can use sql to get a single decile

# # only works in spark 1.6
# import pyspark.sql.functions as funcs
# funcs.percent_rank

# hiveContext = HiveContext(sc)
# hiveContext.registerDataFrameAsTable(df, "df")
# hiveContext.sql("SELECT percentile(price, 0.99999) FROM df")


# easy way - may run out of memory if there are many columns (50+)
# because sortBy sucks at shuffling RDDs with lots of columns

def decile(x, column_to_decile, total_num_rows):
    """Takes (dictionary, row number), name of column to decile, total number of rows. 
    Returns dict with decile added under key "decile"."""
    _dict, row_number = x
    value = _dict[column_to_decile]
    sorted_rank = row_number / float(total_num_rows)
    decile = int(sorted_rank * 10) + 1
    return add_to_dict(_dict, "decile", decile)


rdd_deciled = rdd.map(lambda d: (d[column_to_decile], d)) # make column_to_decile a key
rdd_deciled = rdd_deciled.sortByKey(
    ascending=False) # sort decreasing so 1st decile has largest values
rdd_deciled = rdd_deciled.map(lambda x: x[1]) # remove key
rdd_deciled = rdd_deciled.zipWithIndex() # append row number
rdd_deciled = rdd_deciled.map(
    lambda x: decile(x, column_to_decile, total_num_rows)) # add decile to dictionary

result_df = sqlContext.createDataFrame(rdd_deciled)
print('df deciled easy way')
result_df.show()


# harder way - avoid shuffling all columns by computing thresholds for each decile
# using only the column you want to decile
# then, calculate decile of full rows by seeing which decile's threshold they fall into

def get_decile_from_threshold(value, thresholds):
	"""
	Given value and list of decile thresholds, return decile.
	Thresholds should be sorted by decile like this: 
		[(1, minimum value in decile 1), ..., (10, minimum value in decile 10)]
	"""
	# this shouldn't happen if thresholds are computed correctly:
	if value < min([min_value for decile, min_value in thresholds]):
		raise ValueError("Value {} is smaller than smallest threshold {}.".format())
	inside_deciles = [decile for decile, min_value in thresholds if value >= min_value]
	return inside_deciles[0] # pick the first decile that value is inside threshold of


def get_decile_from_row_number(x, total_num_rows):
	"""
	Given (value, row number) in sorted RDD and total number of rows, return (decile, value). 
	Decile will be in integer interval [1, 10].
	Example: 
		row_number = 219, total_num_rows = 1000 ->
		219/1000 = 0.219 * 10 -> 
		int(2.19) + 1 = 3, 
		so 219 is in the 3rd decile.
	"""
	value, row_number = x
	sorted_rank = row_number / float(total_num_rows)
	decile = int(sorted_rank * 10) + 1
	return (decile, value)


def get_minimum_thresholds_per_decile(rdd):
	"""Takes RDD of single floats/integers, returns a list of (decile, minimum value in decile)."""
	total_rows = rdd.count()
	min_thresholds = rdd
	# sort decreasing so first decile has largest values, etc.
	min_thresholds = min_thresholds.sortBy(lambda x: x, ascending=False)
	# append row number
	min_thresholds = min_thresholds.zipWithIndex() 
	# get decile from row number
	min_thresholds = min_thresholds.map(lambda x: get_decile_from_row_number(x, total_rows))
	# get minimum value for each decile
	min_thresholds = min_thresholds.reduceByKey(lambda value1, value2: min(value1, value2))
	# sort by decile
	min_thresholds = sorted(min_thresholds.collect())
	return min_thresholds


def add_deciles_by_thresholding(rdd, column_to_decile):
	"""Takes RDD of dicts, column to decile, returns (RDD with decile added under key "decile", thresholds)."""
	rdd_values = rdd.map(lambda d: d[column_to_decile])
	min_thresholds = get_minimum_thresholds_per_decile(rdd_values)
	rdd = rdd.map(lambda d: add_to_dict(d, 'decile',
		get_decile_from_threshold(d[column_to_decile], min_thresholds))
	)
	return rdd, min_thresholds


rdd_deciled, min_thresholds = add_deciles_by_thresholding(rdd, column_to_decile)
print('minimum values per decile', min_thresholds)

count_per_decile = rdd_deciled.map(lambda d: (d['decile'], 1))
count_per_decile = count_per_decile.reduceByKey(lambda x, y: x + y)
count_per_decile = sorted(count_per_decile.collect())
print('number rows per decile', count_per_decile)


result_df = sqlContext.createDataFrame(rdd_deciled)
print('df deciled using thresholds')
result_df.show()
