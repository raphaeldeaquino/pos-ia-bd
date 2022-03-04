import re
import findspark

findspark.init()
from pyspark.sql import SparkSession


# Count up how many of each word occurs in a book, using regular expressions.
if __name__ == "__main__":
    spark = (SparkSession
             .builder
             .appName("WordCount")
             .getOrCreate())

    # Load each line of my book into an RDD
    input = spark.sparkContext.textFile("data/Dom-Casmurro.txt")

    # Split using a regular expression that extracts words
    # Normalize everything to lowercase
    words = input.flatMap(lambda t: re.compile(r'\W+', re.UNICODE).split(t.lower()))

    # Count of the occurrences of each word
    wordCounts = words.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)

    # Flip (word, count) tuples to (count, word) and then sort by key (the counts)
    wordCountsSorted = wordCounts.map(lambda x: (x[1], x[0])).sortByKey()

    results = wordCountsSorted.collect()

    for result in results:
        count = str(result[0])
        word = result[1]
        print(word + "\t\t" + count)
