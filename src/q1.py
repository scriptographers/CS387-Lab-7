from operator import add
from pyspark.sql import SparkSession

# Constants
PATH = "groceries - groceries.csv"
PATH_1B = "count.csv"


def pprint(ll):
    print()
    for i, l in enumerate(ll):
        print(f"{i}:", ", ".join([str(li) for li in l]))
    print()


def row2csv(row):
    return f'{row[0][0]},{row[0][1]},{row[1]}'


def preprocess(row):
    prow = row.split(",")[1:]  # remove first column
    prow = list(filter(len, prow))  # remove empty columns
    return prow


def cross(row):
    cp = set()
    for i1 in row:
        for i2 in row:
            if i1 < i2:
                cp.add((i1, i2))
            elif i1 > i2:
                cp.add((i2, i1))
    cp = list(cp)
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
    # pprint(g.take(5))

    # Cross-product per row
    ga = g.map(lambda row: cross(row))
    # pprint(ga.take(5))

    # Flatten
    gaf = ga.flatMap(lambda row: row)
    # pprint(gaf.take(5))

    # Count
    gc = gaf.map(lambda item: (item, 1))
    gc = gc.reduceByKey(add)
    # pprint(gc.take(5))

    # Save as csv
    with open(PATH_1B, "w+") as f:
        gc_list = gc.map(row2csv).collect()
        for row in gc_list:
            f.write(row)
            f.write("\n")

    # Top 5
    gc_sorted = gc.sortBy(lambda row: row[1], ascending=False)
    top5 = gc_sorted.map(row2csv).take(5)
    for row in top5:
        print(row)

    # Stop
    spark.stop()
