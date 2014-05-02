#!/usr/bin/env python

"""
CREDENTIALS
  Module: mapper.py
  Author: John Soper
  Date: Apr 4, 2014
  Rev: 2

SUMMARY
    Custom map file for running Titanic Prediction project in hadoop
    This map does not have a corresponding Reduce file
    The survival tables are meant to be dynamically updated by other python
    code, they're currently set so that everyone dies

    Prediction Score when entered in the Kaggle Titanic Competition
        Model 1: 76.555%
        Model 2: 77.990%


    This streaming mapper can be run on a Psuedo cluster instead of AWS EMR.
    The instructions below will need slight tweaking, and the female table
        will have to be hardcoded

    RUN ONCE:
        export HADOOP_CONF_DIR=/Users/john/hadoop/configurations/psuedo
        export STREAMJAR=/usr/local/Cellar/hadoop/1.2.1/libexec/contrib/ \
            streaming/hadoop-streaming-*.jar
        # rm -r /private/tmp/hadoop-john  # Create new HDFS if desired
        # hadoop namenode -format
        start-all.sh
    RUN ONCE (separate copy and paste from above):
        cd /Users/john/Desktop/school/cloud_computing/project_stream
        hadoop fs -put ./input/test.csv /user/wc/input/test.csv
    CAN RUN MULTIPLE TIMES:
        rm -f ./part*
        hadoop jar $STREAMJAR \
            -file ./mapper/mapper1.py  -mapper  ./mapper/mapper1.py  \
            -input /user/wc/input/* \
            -output /user/wc/output
        hadoop fs -get /user/wc/output/part* .
        head -20 part*
        hadoop fs -rmr /user/wc/output
    WHEN DONE:
        stop-all.sh
        rm -f ./part*

RESOURCES
    www.kaggle.com tutorial code in the Titanic Machine Learning Competition
"""

import sys

def main():
    """ Mapper module for Map-Reduce run on AWS EMR """
    female_survival_table = [[0, 0, 1, 1], [0, 1, 1, 1], [1, 1, 0, 0]]
    male_survival_table = [[0, 0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0]]

    for line in sys.stdin:
        # remove leading and trailing whitespace
        line = line.strip()

        # split line of csv data by comma
        words = line.split(',')
        if words[0] == 'PassengerId':
            print 'PassengerId,Survived'
        else:
            fare = float(words[9])
            if fare >= 30.0:
                fare_class = 3
            elif fare >= 20.0:
                fare_class = 2
            elif fare >= 10.0:
                fare_class = 1
            else:
                fare_class = 0

            passenger_class = int(words[1]) -1
            if words[4] == 'female':
                print '%s,%d' % (words[0], \
                    female_survival_table[passenger_class][fare_class])
            else:
                print '%s,%d' % (words[0], \
                    male_survival_table[passenger_class][fare_class])


if __name__ == "__main__":
    main()
