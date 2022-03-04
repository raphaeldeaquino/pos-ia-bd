from __future__ import print_function

import findspark

findspark.init()
from pyspark.sql import SparkSession


# A function that splits a line of input into (age, numFriends) tuples.
def parse_line(line):
    # Split by commas
    fields = line.split(",")

    # Extract the age and numFriends fields, and convert to integers
    age = int(fields[2])
    num_friends = int(fields[3])

    # Create a tuple that is our result.
    return age, num_friends


if __name__ == "__main__":
    spark = (SparkSession
             .builder
             .appName("FriendsByAge")
             .getOrCreate())

    # Load each line of the source data into an RDD
    lines = spark.sparkContext.textFile("data/fakefriends.csv")

    # Use our parseLines function to convert to (age, numFriends) tuples
    rdd = lines.map(parse_line)

    # Lots going on here...
    # We are starting with an RDD of form (age, numFriends) where age is
    # the KEY and numFriends is the VALUE
    # We use mapValues to convert each numFriends value to a tuple of (numFriends, 1)
    # Then we use reduceByKey to sum up the total numFriends and total instances
    # for each age, by adding together all the numFriends values and 1's respectively.
    totals_by_age = rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))

    # So now we have tuples of (age, (totalFriends, totalInstances))
    # To compute the average we divide totalFriends / totalInstances for each age.
    average_by_age = totals_by_age.mapValues(lambda x: x[0] / x[1]).sortByKey()

    # Collect the results from the RDD
    # This kicks off computing the DAG and actually executes the job
    results = average_by_age.collect()
    for r in results:
        print(r)

