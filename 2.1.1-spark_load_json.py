
import os, json, requests
from pyspark.sql import SparkSession, functions as F

def main():
    url = "https://anapioficeandfire.com/api/characters/"
    jsonList = []
    for i in range(1,10):
        r = requests.get(url + str(i))
        jsonList.append(r.json())

    jsonDumps = json.dumps(jsonList)
    print(jsonDumps)

    spark = SparkSession.builder \
            .appName("JSONLoad") \
            .getOrCreate()

    df = spark.read.json('stuff.json')
    df.show(10, truncate=False)

    return

if __name__ == '__main__':
    main()

