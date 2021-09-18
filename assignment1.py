from __future__ import print_function

import sys

from pyspark import SparkContext

if __name__ == "__main__":
    # load data by SparkContext
    sc = SparkContext()
    lines = sc.textFile(sys.argv[1])

    # filtered the bad values
    def isfloat(value):
        try:
            float(value)
            return True
        except:
            return False

    def correctRows(p):
        if (len(p) == 17):
            if (isfloat(p[5]) and isfloat(p[11])):
                if (float(p[5]) != 0 and float(p[11]) != 0 and float(p[4]) != 0):
                    return p

    linesCorrected = lines.map(lambda x: x.split(',')).filter(correctRows)

    # create a tuple to count the frequent of each key
    lineTuples = linesCorrected.map(lambda x: (x[0], 1))

    counts = lineTuples.reduceByKey(lambda x, y: x + y)

    # return top 10 values
    result = counts.top(10, lambda x: x[1])

    # write to file
    dataToASingleFile = sc.parallelize(result).coalesce(1)
    dataToASingleFile.saveAsTextFile(sys.argv[2])

    sc.stop()