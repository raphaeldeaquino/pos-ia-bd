import findspark

findspark.init()
from pyspark.sql import SparkSession


# Load up a Map of movie IDs to movie names
def load_movie_names():
    movie_names = {}
    with open("data/ml-100k/u.ITEM", encoding="ISO-8859-1") as f:
        for line in f:
            fields = line.split('|')
            movie_names[int(fields[0])] = fields[1]
    return movie_names


# Find the movies with the most ratings
if __name__ == "__main__":
    spark = (SparkSession
             .builder
             .appName("PopularMovies")
             .getOrCreate())

    # Create a broadcast variable of our ID -> movie name map
    nameDict = spark.sparkContext.broadcast(load_movie_names())

    # Read in each rating line
    lines = spark.sparkContext.textFile("data/ml-100k/u.data")

    # Map to (movieID, 1) tuples
    movies = lines.map(lambda x: (int(x.split()[1]), 1))

    # Count up all the 1's for each movie
    movieCounts = movies.reduceByKey(lambda x, y: x + y)

    # Flip (movieID, count) to (count, movieID)
    flipped = movieCounts.map(lambda x: (x[1], x[0]))

    # Sort
    sortedMovies = flipped.sortByKey()

    # Fold in the movie names from the broadcast variable
    sortedMoviesWithNames = sortedMovies.map(lambda count_movie: (nameDict.value[count_movie[1]], count_movie[0]))

    # Collect and print results
    results = sortedMoviesWithNames.collect()

    for result in results:
        print(result)
