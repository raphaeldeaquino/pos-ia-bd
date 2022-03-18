import findspark

findspark.init()
from pyspark.sql import SparkSession


# Function to extract the hero ID and number of connections from each line
def count_co_occurences(line):
    elements = line.split()
    return int(elements[0]), len(elements) - 1


# Function to extract hero ID -> hero name tuples
def parse_names(line):
    fields = line.split('\"')
    return int(fields[0]), fields[1].encode("utf8")


# Find the movies with the most ratings
if __name__ == "__main__":
    spark = (SparkSession
             .builder
             .appName("PopularMovies")
             .getOrCreate())

    # Build up a hero ID -> name RDD
    names = spark.sparkContext.textFile("data/marvel-names.txt")
    names_rdd = names.flatMap(parse_names)

    # Load up the superhero co-apperarance data
    lines = spark.sparkContext.textFile("data/marvel-graph.txt")

    # Convert to (heroID, number of connections) RDD
    pairings = lines.map(count_co_occurences)

    # Combine entries that span more than one line
    total_friends_by_character = pairings.reduceByKey(lambda x, y: x + y)

    # Flip it to # of connections, hero ID
    flipped = total_friends_by_character.map(lambda x: (x[1], x[0]))

    popular_ranking = flipped.sortByKey(numPartitions=1)

    results = popular_ranking.collect()
    top = results[-10:]
    bottom = results[:9]
    print("Most popular:\n")
    for i in reversed(range(10)):
        print(top[i])
        most_popular_name = names_rdd.lookup(top[i][1])[0]
        print(most_popular_name + "\n")
    print("\nLeast popular:\n")
    for i in range(10):
        least_popular_name = names_rdd.lookup(bottom[i][1])[0]
        print(least_popular_name)