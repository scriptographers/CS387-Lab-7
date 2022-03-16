import re
from pyspark.sql import SparkSession

# Constants
PATH = "../data/access.log"

def pprint(ll):
    print()
    for i, l in enumerate(ll):
        print(f"{i}:", ", ".join([str(li) for li in l]))
    print()

def preprocess(row):
    # Match 5 groups
    m = re.match(r"(.+) - - \[(\d{2}\/\w{3}\/\d{4}:\d{2}:\d{2}:\d{2}) \+\d{4}\] \"(\w+).+\" (\d{3}) (\d+) .+", row)
    if m:
        return [m.group(i+1) for i in range(5)]
    else:
        return None

if __name__ == "__main__":

    # Start
    spark = SparkSession.builder.appName("q2").getOrCreate()

    # Read data
    g = spark.sparkContext.textFile(PATH)
    print(g.take(5))

    # Preprocess
    g = g.map(lambda row: preprocess(row))

    pprint(g.take(5))

    # Bad rows (takes time, uncomment before submitting)
    # n_bad = g.filter(lambda row: row is None).count()
    # print(f"Number of bad Rows : {n_bad}")



    # Stop
    spark.stop()