from pyspark.sql import SparkSession

# Constants
PATH = "../data/access.log"

def pprint(ll):
    print()
    for i, l in enumerate(ll):
        print(f"{i}:", ", ".join([str(li) for li in l]))
    print()

def preprocess(row):
    prow = row.replace("\"", "").replace("[", "").replace("]", "").split(" ")
    host = prow[0]
    timestamp = prow[3]
    req = prow[5]
    status = int(prow[8])
    length = int(prow[9])
    prow = [host, timestamp, req, status, length]
    return prow

if __name__ == "__main__":

    # Start
    spark = SparkSession.builder.appName("q2").getOrCreate()

    # Read data
    g = spark.sparkContext.textFile(PATH)
    print(g.take(5))

    # Preprocess
    g = g.map(lambda row: preprocess(row))

    pprint(g.take(5))

    # Stop
    spark.stop()