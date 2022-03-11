import findspark

findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql import Row


def mapper(line):
    fields = line.split(',')
    return Row(ID=int(fields[0]), name=str(fields[1].encode("utf-8")), age=int(fields[2]), numFriends=int(fields[3]))


# Find the maximum temperature by weather station
if __name__ == "__main__":
    spark = (SparkSession
             .builder
             .appName("Teens")
             .getOrCreate())

    # Read each line of input data
    lines = spark.sparkContext.textFile("data/fakefriends.csv")
    people = lines.map(mapper)

    # Infer the schema, and register the DataFrame as a table.
    schemaPeople = spark.createDataFrame(people).cache()
    schemaPeople.createOrReplaceTempView("people")

    # SQL can be run over DataFrames that have been registered as a table.
    teenagers = spark.sql("SELECT * FROM people WHERE age >= 13 AND age <= 19")

    # The results of SQL queries are RDDs and support all the normal RDD operations.
    for teen in teenagers.collect():
        print(teen)

    # We can also use functions instead of SQL queries:
    schemaPeople.groupBy("age").count().orderBy("age").show()

    print("Here is our inferred schema:")
    schemaPeople.printSchema()

    print("Let's select the name column:")
    schemaPeople.select("name").show()

    print("Filter out anyone over 21:")
    schemaPeople.filter(schemaPeople.age < 21).show()

    print("Group by age:")
    schemaPeople.groupBy("age").count().show()

    print("Make everyone 10 years older:")
    schemaPeople.select(schemaPeople.name, schemaPeople.age + 10).show()

    spark.stop()