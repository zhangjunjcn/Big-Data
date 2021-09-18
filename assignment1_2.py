from __future__ import print_function

import sys

from pyspark import SparkContext

if __name__ == "__main__":

    # load file by SparkContext
    sc = SparkContext()
    lines = sc.textFile(sys.argv[1])

    # drop the bad data
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

    # filter the right data
    linesCorrected = lines.map(lambda x: x.split(',')).filter(correctRows)

    # group data by time and earnings
    earnings_raw = linesCorrected.map(lambda x: (x[1], x[16]))
    earnings = earnings_raw.reduceByKey(lambda x, y: float(x) + float(y))
    time_earn_raw = linesCorrected.map(lambda x: (x[1], (float(x[4]), float(x[16]))))
    time_earn = time_earn_raw.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))

    # get the earning per minute, and filter top 10 values
    result = time_earn.map(lambda x: (x[0], x[1][1] / x[1][0] * 60)).top(10, lambda x: x[1])

    # write to file
    dataToASingleFile = sc.parallelize(result).coalesce(1)

    dataToASingleFile.saveAsTextFile(sys.argv[2])

    sc.stop()




