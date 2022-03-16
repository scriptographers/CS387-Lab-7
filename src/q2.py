import re
from operator import add
from matplotlib import pyplot as plt
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

    # Remove bad rows (takes time, uncomment before submitting)
    g_clean = g.filter(lambda row: row is not None)
    n_clean = g_clean.count()
    n_total = g.count()
    n_bad = n_total - n_clean
    print(f"Number of bad Rows : {n_bad}")

    # Status distribution
    stat_distr = g_clean.map(lambda row: (row[3], 1)).reduceByKey(add).collect()
    stat_distr = sorted(stat_distr, key = lambda t: int(t[0]))
    print("HTTP status analysis:")
    print("status\tcount")
    for t in stat_distr:
        print(f"{t[0]}\t{t[1]}")

    # Pie chart
    labels = [t[0] for t in stat_distr]
    counts = [t[1] for t in stat_distr]
    fig = plt.figure()
    plt.pie(counts, labels = labels, autopct="%1.2f%%")
    plt.savefig("q2Db.png")
    plt.close(fig)

    # Hosts distribution
    host_distr = g_clean.map(lambda row: (row[0], 1)).reduceByKey(add).collect()
    host_distr = sorted(host_distr, key = lambda t: t[0])
    print("Frequent Hosts:")
    print("host\tcount")
    for t in host_distr:
        print(f"{t[0]}\t{t[1]}")

    # Unique hosts
    print("Unique hosts:")
    print(len(host_distr))

    # Stop
    spark.stop()