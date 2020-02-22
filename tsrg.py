from pyspark.context import SparkContext
from pyspark.sql import SQLContext
import sys, getopt

def main(argv):
   input = argv[0]
   output = argv[1]
   sc = SparkContext.getOrCreate() #"SparkContext is the entry point to any Spark functionality" - tutorialspoint.com
   sqlContext = SQLContext(sc)
   dataframes = sqlContext.read.format("csv").option("header", "true").option("mode", "DROPMALFORMED").load(input)
   #previous line found from stackoverflow - it makes the format CSV, it makes the header not recognized as input,
   #drop malformed data, and then load the input file
   sorted_dataframes = dataframes.sort(['cca2', 'timestamp'], ascending=[True, True])
   #previous line sorts dataframe by cca2 (country) and timestamp
   sorted_dataframes.write.csv(output) #outputs to file

if __name__ == "__main__":
   main(sys.argv[1:])