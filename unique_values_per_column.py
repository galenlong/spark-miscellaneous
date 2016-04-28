"""
Get all unique values in a column.
"""

from pyspark import SparkContext
sc = SparkContext()
sc.setLogLevel("FATAL")

def set_plus_row(sets, row):
	"""Update each set in list with values in row."""
	for i in range(len(sets)):
		sets[i].add(row[i])
	return sets


rdd = sc.parallelize([
	["a", "one", "x"], 
	["b", "one", "y"], 
	["a", "two", "x"], 
	["c", "two", "x"]])

num_columns = len(rdd.first())

unique_values_per_column = rdd.aggregate(
	[set() for i in range(num_columns)], 
	set_plus_row, # in function b/c set().add doesn't return anything
	lambda x, y: [a.union(b) for a, b in zip(x, y)]
	)

for i, values in enumerate(unique_values_per_column):
	print 'column', i, sorted(values)

