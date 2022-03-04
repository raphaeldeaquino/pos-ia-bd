import findspark

findspark.init()
from pyspark.sql import SparkSession

# Count up how many of each word appears in a book as simply as possible
if __name__ == "__main__":
    spark = (SparkSession
             .builder
             .appName("WordCount")
             .getOrCreate())

    # Read each line of my book into an RDD
    input = spark.sparkContext.textFile("data/Dom-Casmurro.txt")

    # Split into words separated by a space character
    words = input.flatMap(lambda x: x.split())

    # Count up the occurrences of each word
    wordCounts = words.countByValue()

    # Print the results.
    for word, count in wordCounts.items():
        print(word + " " + str(count))
