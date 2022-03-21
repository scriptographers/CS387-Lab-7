import re

c = 0


def preprocess(row):
    # Match 5 groups
    rgx = r"(\d+.\d+.\d+.\d+) .+ .+ \[(\d{2}\/\w{3}\/\d{4}:\d{2}:\d{2}:\d{2}) [\+|\-]\d{4}\] \"([A-Z]+).*\" (\d{3}) (\d+) \".*\" \".*\" \".*\""
    m = re.match(rgx, row)
    if not m:
        global c
        c += 1
        print(c, ":", row)


if __name__ == "__main__":

    with open("access.log", "r") as f:
        while True:
            line = f.readline()
            if not line:
                break
            preprocess(line)
