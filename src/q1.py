from pyspark.sql import SparkSession

# Constants
PATH = "../data/data1.csv"

def pprint(ll):
    print()
    for i, l in enumerate(ll):
        print(f"{i}:", ", ".join([str(li) for li in l]))
    print()

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
    pprint(gaf.take(5))

    # Stop
    spark.stop()