from operator import add
from pyspark.sql import SparkSession

# Constants
PATH = "../data/data1.csv"
PATH_1B = "q1b.csv"

def pprint(ll):
    print()
    for i, l in enumerate(ll):
        print(f"{i}:", ", ".join([str(li) for li in l]))
    print()

def row2csv(row):
    crow = ", ".join([str(i) for i in row])
    return crow

def preprocess(row):
    prow = row.split(",")[1:] # remove first column
    prow = list(filter(len, prow)) # remove empty columns
    return prow

def cross(row):
    cp = [(i1, i2) for i1 in row for i2 in row if i1 != i2]
    return cp

if __name__ == "__main__":

    # Start
    spark = SparkSession.builder.appName("q1").getOrCreate()

    # Read and preprocess data
    g = spark.sparkContext.textFile(PATH)
    g = g.map(lambda row: preprocess(row))

    # Remove header
    header = g.first()
    g = g.filter(lambda row: row != header) 

    pprint(g.take(5))

    # Cross-product per row
    ga = g.map(lambda row: cross(row))
    pprint(ga.take(5))

    # Flatten
    gaf = ga.flatMap(lambda row: row)
    print(gaf.take(5))

    # Count
    gc = gaf.map(lambda item: (item, 1))
    gc = gc.reduceByKey(add)
    pprint(gc.take(5))

    # Save as csv
    with open(PATH_1B, "w+") as f:
        gc_list = gc.map(lambda row: row2csv(row)).collect()
        for row in gc_list:
            f.write(row)
            f.write("\n")

    # Stop
    spark.stop()