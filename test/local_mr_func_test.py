#!/usr/bin/env python

"""
CREDENTIALS
  Module: local_mr_func_test.py
  Author: John Soper
  Date: Apr 28, 2014
  Rev: 3

SUMMARY
   Sets up a map-reduce operation that does everything locally instead of
   in the cloud.  This is actually a client program for the mincemeat.py
   server which runs a pure Python mapreduce with no Hadoop used.

   The input data is the same Titanic data the cloud uses, and the
   generated output files are identical

REQUIREMENTS
    The recommended use is to run the launch_mr_func_test.py module.
    If you want to run separately, the best way is to use two or more
    command line interfaces and start this module first:

        python local_mr_func_test.py
        python mincemeat.py -p changeme localhost #in one or more CLIs

    When processing a large volume of input data, using multiple servers
    does speed things up, but mincemeat has nowhere near the power of
    Hadoop.

    Some process time numbers for anyone interested, taken from a 2011
    Macbook Air which has two cores but four logical processors:
            2:55   1 mincemeat window
            1:58   2 windows
            1:51   3 windows
            1:49   4 windows
"""
# Pylint in Eclipse likes this block, but not "import mincemeat"
#import sys
#import os.path
#PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
#sys.path.append(PROJECT_DIR)
#import test.mincemeat as mincemeat

# Command line python likes this, but not above
import mincemeat

# Below is because pylint interprets all module-level variables as
#     being 'constants'
# pylint: disable-msg=C0103

# Fill list by reading input file
data = []
with open('../data/test.csv', 'r') as input_file:
    for line in input_file:
        # sanitize data
        line = line.replace('"', '').rstrip('\n').strip()
        if line[0].isdigit():
            data.append(line)

# The data source can be any dictionary-like object
#  enumerate - Iterate over indices and items of a list
#  dict - create dictionary with given mapping values
datasource = dict(enumerate(data))

def mapfn(_, v):  #  Replace _ with k when using.  Changed for pylint
    """ Mapper routine"""
    passenger_fields = v.split(',')
    fare = float(passenger_fields[9])
    if fare >= 30.0:
        fare_class = 3
    elif fare >= 20.0:
        fare_class = 2
    elif fare >= 10.0:
        fare_class = 1
    else:
        fare_class = 0
    passenger_class = int(passenger_fields[1]) -1
    male_survival_table = [[0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0]]

    # comment below in for model 1
    #female_survival_table = [[1, 1, 1, 1], [1, 1, 1, 1], [1, 1, 1, 1]]
    # comment below in for model 2
    female_survival_table = [[0, 0, 1, 1], [0, 1, 1, 1], [1, 1, 0, 0]]

    if passenger_fields[4] == 'male':
        val = male_survival_table[passenger_class][fare_class]
    else:
        val = female_survival_table[passenger_class][fare_class]
    yield int(passenger_fields[0]), val


def reducefn(_, vs):  # Replace _ with k when using.  Changed for pylint
    """ reduce routine, just a pass-through for Titanic data"""
    result = vs[0]
    return result

s = mincemeat.Server()
s.datasource = datasource
s.mapfn = mapfn
s.reducefn = reducefn

# results is a dictionary
results = s.run_server(password="changeme")
with open('../output/local_titanic_test_data.csv', 'w') as output_file:
    output_file.write("PassengerId,Survived\n")
    for key in results:
        output_file.write("%s,%s\n" % (key, results[key]))

