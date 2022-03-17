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
    print()
    print(f"Number of bad Rows : {n_bad}")

    # Status distribution
    stat_distr = \
        g_clean.map(lambda row: (row[3], 1)).\
        reduceByKey(add).\
        sortBy(lambda t: t[1], ascending = False).\
        collect()
    print()
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
    host_distr = \
        g_clean.\
        map(lambda row: (row[0], 1)).\
        reduceByKey(add).\
        sortBy(lambda t: t[1], ascending = False)
    n_hosts = host_distr.count()
    print()
    print("Frequent Hosts:")
    print("host\tcount")
    for t in host_distr.take(10):
        print(f"{t[0]}\t{t[1]}")

    # Unique hosts
    print()
    print("Unique hosts:")
    print(n_hosts)

    # Unique hosts per day
    host_and_day = g_clean.map(lambda row: (row[0], row[1][:11])).distinct()
    pprint(host_and_day.take(5))
    hosts_per_day = \
        host_and_day.\
        groupBy(lambda row: row[1]).\
        map(lambda row: (row[0], len(row[1]))).\
        sortBy(lambda row: row[0], ascending = True).\
        collect()
    print()
    print("Unique hosts per day:")
    print("day\thosts")
    for t in hosts_per_day:
        print(f"{t[0]}\t{t[1]}")

    # Line graph
    days = [t[0] for t in hosts_per_day]
    n_hosts = [t[1] for t in hosts_per_day]
    fig = plt.figure()
    plt.plot(days, n_hosts)
    plt.ylabel("Hosts count")
    plt.xlabel("Day") 
    plt.title("Number of Unique Hosts Daily")
    plt.savefig("q2Df.png")
    plt.close(fig)

    # Response lengths
    g_res = g_clean.map(lambda row: int(row[-1]))
    min_res = g_res.min()
    max_res = g_res.max()
    avg_res = g_res.mean()
    print()
    print("Response length statistics:")
    print(f"Minimum length: {min_res}")
    print(f"Maximum length: {max_res}")
    print(f"Average length: {avg_res}")

    # Stop
    spark.stop()