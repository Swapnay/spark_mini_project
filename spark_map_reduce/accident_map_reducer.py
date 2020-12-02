from os.path import isfile, join
from pyspark.sql import SparkSession
import logging
from pyspark.context import SparkContext


class AccidentMapReducer:
    make = None
    year = None
    loc = "hdfs://localhost/hadoop/"

    def get_logger(self,sc):
        logger = sc._jvm.org.apache.log4j
        return logger.LogManager.getLogger(__name__)

    def get_accidents_by_make_model(self):
        sc = SparkContext("local", "My Application")
        spark = SparkSession(sc)
        logger = self.get_logger(sc)
        logger.warn("Starting spark map reduce job")
        rdd = sc.textFile(join(self.loc, "data.csv"))
        pair_rdd = rdd.map(lambda line: self.extract_vin_key_value(line))
        enhance_make = pair_rdd.groupByKey().flatMap(lambda kv: self.populate_make(kv[1]))
        make_kv = enhance_make.map(lambda x: self.extract_make_key_value(x)).reduceByKey(lambda a, b: a + b)
        make_kv.coalesce(1).saveAsTextFile(join(self.loc,"result"))


    def extract_make_key_value(self, make_kv):
        return ("{}-{}".format(make_kv[1], make_kv[2]), 1)

    def populate_make(self, kv):
        list = []
        for row in kv:
            if row[1].strip():
                self.make = row[1]
            if row[2].strip():
                self.year = row[2]
        for k in kv:
            if k[0] == 'A':
                list.append([k[0], self.make, self.year])
        return list

    def extract_vin_key_value(self, line):
        row = line.split(",")
        return (row[2], [row[1], row[3], row[5]])


if __name__ == "__main__":
    accidentTotal = AccidentMapReducer()
    accidentTotal.get_accidents_by_make_model()
