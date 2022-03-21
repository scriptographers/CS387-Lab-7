import re
from operator import add
from matplotlib import pyplot as plt
from pyspark.sql import SparkSession

# Constants
PATH = "access.log"

SUBPARTS = [
    "C",
    "Da",
    "Db",
    "Dc",
    "Dd",
    "De",
    "Df",
    "Dg",
    "Dh",
    "Di",
    "Dj",
]

def pprint(ll):
    print()
    for i, l in enumerate(ll):
        print(f"{i}:", ", ".join([str(li) for li in l]))
    print()

def preprocess(row):
    # Match 5 groups
    rgx = r"(\d+.\d+.\d+.\d+) .+ .+ \[(\d{2}\/\w{3}\/\d{4}:\d{2}:\d{2}:\d{2}) [\+|\-]\d{4}\] \"([A-Z]+) .+\" (\d{3}) (\d+) \".*\" \".*\" \".*\""
    m = re.match(rgx, row)
    if m:
        return [m.group(i+1) for i in range(5)]
    else:
        return None

if __name__ == "__main__":

    # Start
    spark = SparkSession.builder.appName("q2").getOrCreate()

    # Read data
    g = spark.sparkContext.textFile(PATH)

    # Preprocess
    g = g.map(lambda row: preprocess(row))

    # Remove bad rows (takes time, uncomment before submitting)
    g_clean = g.filter(lambda row: row is not None)

    if "C" in SUBPARTS:
        n_clean = g_clean.count()
        n_total = g.count()
        n_bad = n_total - n_clean
        print(f"Number of bad Rows : {n_bad}")

    # Status distribution
    if "Da" in SUBPARTS:

        stat_distr = \
            g_clean.map(lambda row: (row[3], 1)).\
            reduceByKey(add).\
            sortBy(lambda t: t[1], ascending = False).\
            collect()

        with open("a.txt", "w+") as f:
            f.write("HTTP status analysis:\n")
            f.write("status\tcount\n")
            for t in stat_distr:
                f.write(f"{t[0]}\t{t[1]}\n")

    # Pie chart
    if "Db" in SUBPARTS:
        labels = [t[0] for t in stat_distr[:5]]
        counts = [t[1] for t in stat_distr[:5]]
        fig = plt.figure()
        plt.pie(counts, labels = labels, autopct="%1.2f%%")
        fig.set_size_inches(6, 6)
        plt.savefig("b.jpg")
        plt.close(fig)

    # Hosts distribution
    if "Dc" in SUBPARTS:

        host_distr = \
            g_clean.\
            map(lambda row: (row[0], 1)).\
            reduceByKey(add).\
            sortBy(lambda t: t[1], ascending = False)

        with open("c.txt", "w+") as f:
            f.write("Frequent Hosts:\n")
            f.write("host\tcount\n")
            for t in host_distr.collect():
                f.write(f"{t[0]}\t{t[1]}\n")

    # Unique hosts
    if "Dd" in SUBPARTS:
        n_hosts = host_distr.count()
        with open("d.txt", "w+") as f:
            f.write("Unique hosts:\n")
            f.write(f"{n_hosts}\n")

    # Unique hosts per day
    if "De" in SUBPARTS:

        host_and_day = g_clean.map(lambda row: (row[0], row[1][:11])).distinct()
        # pprint(host_and_day.take(5))

        hosts_per_day = \
            host_and_day.\
            groupBy(lambda row: row[1]).\
            map(lambda row: (row[0], len(row[1]))).\
            sortBy(lambda row: row[0], ascending = True).\
            collect()

        with open("e.txt", "w+") as f:
            f.write("Unique hosts per day:\n")
            f.write("day\thosts\n")
            for t in hosts_per_day:
                f.write(f"{t[0]}\t{t[1]}\n")

    # Line graph
    if "Df" in SUBPARTS:
        days = [t[0] for t in hosts_per_day]
        n_hosts = [t[1] for t in hosts_per_day]
        fig = plt.figure()
        plt.plot(days, n_hosts)
        plt.ylabel("Hosts count")
        plt.xlabel("Day") 
        plt.title("Number of Unique Hosts Daily")
        plt.savefig("f.jpg")
        plt.close(fig)

    # Failure
    if "Dg" in SUBPARTS:
        g_st = \
            g_clean.filter(lambda row: (row[3][0] == "4" or row[3][0] == "5")).\
            map(lambda row: (row[0], row[3])).\
            groupBy(lambda row: row[0]).\
            map(lambda row: (row[0], len(row[1]))).\
            sortBy(lambda row: row[1], ascending = False).\
            take(5)
        
        with open("e.txt", "w+") as f:
            f.write("Failed HTTP Clients:\n")
            for t in g_st:
                f.write(f"{t[0]}\n") # \t{t[1]}

    # Requests per hour
    if "Dh" in SUBPARTS:

        hr_st = \
            g_clean.filter(lambda row: (row[1][:11] == "22/Jan/2019")).\
            map(lambda row: (int(row[1][-4:].split(":")[0]), row[3]))

        g_tot = hr_st.\
                map(lambda row: (row[0], 1)).\
                reduceByKey(add).\
                sortBy(lambda row: row[0], ascending = True).\
                collect()

        g_failed = hr_st.\
                filter(lambda row: (row[1][0] == "4" or row[1][0] == "5")).\
                map(lambda row: (row[0], 1)).\
                reduceByKey(add).\
                sortBy(lambda row: row[0], ascending = True).\
                collect()

        labels = [t[0] for t in g_tot]
        y_tot = [t[1] for t in g_tot]
        y_fl = [t[1] for t in g_failed]

        fig = plt.figure()
        plt.plot(labels, y_tot, label = "Total requests")
        plt.plot(labels, y_fl, label = "Failed requests")
        plt.ylabel("Number of requests")
        plt.xlabel("Hour") 
        plt.legend()
        plt.savefig("h.jpg")
        plt.close(fig)
    
    # Most active hour
    if "Di" in SUBPARTS:

        day_hr = g_clean.\
            map(lambda row: ((row[1][:11], int(row[1][-4:].split(":")[0])), 1)).\
            reduceByKey(add).\
            map(lambda row: (row[0][0], (row[0][1], row[1]))).\
            reduceByKey(lambda x1, x2: max(x1, x2, key = lambda x: x[1])).\
            sortBy(lambda row: row[0], ascending = True).\
            collect()

        with open("i.txt", "w+") as f:
            f.write("Active Hours:\n")
            f.write("day\thour\n")
            for t in day_hr:
                f.write(f"{t[0]}\t{t[1][0]}\n")

    # Response lengths
    if "Dj" in SUBPARTS:

        g_res = g_clean.map(lambda row: int(row[-1]))
        min_res = g_res.min()
        max_res = g_res.max()
        avg_res = g_res.mean()
        
        with open("j.txt", "w+") as f:
            f.write("Response length statistics:\n")
            f.write(f"Minimum length: {min_res}\n")
            f.write(f"Maximum length: {max_res}\n")
            f.write(f"Average length: {avg_res}\n")

    # Stop
    spark.stop()