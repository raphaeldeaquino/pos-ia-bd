from math import sqrt
import findspark

findspark.init()
from pyspark.sql import SparkSession


def load_movie_names():
    movie_names = {}
    with open("data/ml-100k/u.ITEM", encoding='ascii', errors='ignore') as f:
        for line in f:
            fields = line.split('|')
            movie_names[int(fields[0])] = fields[1]
    return movie_names


#Python 3 doesn't let you pass around unpacked tuples,
#so we explicitly extract the ratings now.
def make_pairs(user_ratings):
    ratings = user_ratings[1]
    (movie1, rating1) = ratings[0]
    (movie2, rating2) = ratings[1]
    return (movie1, movie2), (rating1, rating2)


def filter_duplicates(user_ratings):
    ratings = user_ratings[1]
    (movie1, rating1) = ratings[0]
    (movie2, rating2) = ratings[1]
    return movie1 < movie2


def compute_cosine_similarity(rating_pairs):
    num_pairs = 0
    sum_xx = sum_yy = sum_xy = 0
    for ratingX, ratingY in rating_pairs:
        sum_xx += ratingX * ratingX
        sum_yy += ratingY * ratingY
        sum_xy += ratingX * ratingY
        num_pairs += 1

    numerator = sum_xy
    denominator = sqrt(sum_xx) * sqrt(sum_yy)

    score = 0
    if denominator:
        score = (numerator / (float(denominator)))

    return score, num_pairs


if __name__ == "__main__":
    spark = (SparkSession
             .builder
             .appName("MovieSimilarities")
             .getOrCreate())

    print("\nLoading movie names...")
    nameDict = load_movie_names()

    data = spark.sparkContext.textFile("data/ml-100k/u.data")

    # Map ratings to key / value pairs: user ID => movie ID, rating
    ratings = data.map(lambda l: l.split()).map(lambda l: (int(l[0]), (int(l[1]), float(l[2]))))

    # Emit every movie rated together by the same user.
    # Self-join to find every combination.
    joinedRatings = ratings.join(ratings)

    # At this point our RDD consists of userID => ((movieID, rating), (movieID, rating))

    # Filter out duplicate pairs
    uniqueJoinedRatings = joinedRatings.filter(filter_duplicates)

    # Now key by (movie1, movie2) pairs.
    moviePairs = uniqueJoinedRatings.map(make_pairs)

    # We now have (movie1, movie2) => (rating1, rating2)
    # Now collect all ratings for each movie pair and compute similarity
    moviePairRatings = moviePairs.groupByKey()

    # We now have (movie1, movie2) = > (rating1, rating2), (rating1, rating2) ...
    # Can now compute similarities.
    moviePairSimilarities = moviePairRatings.mapValues(compute_cosine_similarity).cache()

    # Save the results if desired
    #moviePairSimilarities.sortByKey()
    #moviePairSimilarities.saveAsTextFile("movie-sims")

    # Extract similarities for the movie we care about that are "good".
    movieID = 50

    scoreThreshold = 0.97
    coOccurenceThreshold = 50

    # Filter for movies with this sim that are "good" as defined by
    # our quality thresholds above
    filteredResults = moviePairSimilarities.filter(lambda pairSim: \
        (pairSim[0][0] == movieID or pairSim[0][1] == movieID) \
        and pairSim[1][0] > scoreThreshold and pairSim[1][1] > coOccurenceThreshold)

    # Sort by quality score.
    results = filteredResults.map(lambda pairSim: (pairSim[1], pairSim[0])).sortByKey(ascending = False).take(10)

    print("Top 10 similar movies for " + nameDict[movieID])
    for result in results:
        (sim, pair) = result
        # Display the similarity result that isn't the movie we're looking at
        similarMovieID = pair[0]
        if similarMovieID == movieID:
            similarMovieID = pair[1]
        print(nameDict[similarMovieID] + "\tscore: " + str(sim[0]) + "\tstrength: " + str(sim[1]))
