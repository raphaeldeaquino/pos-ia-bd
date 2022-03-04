import findspark

findspark.init()
from pyspark.sql import SparkSession


def parse_line(line):
    fields = line.split(',')
    station_id = fields[0]
    entry_type = fields[2]
    temperature = float(fields[3]) * 0.1 * (9.0 / 5.0) + 32.0
    return station_id, entry_type, temperature


# Find the minimum temperature by weather station
if __name__ == "__main__":
    spark = (SparkSession
             .builder
             .appName("MinTemperatures")
             .getOrCreate())

    # Read each line of input data
    lines = spark.sparkContext.textFile("data/1800.csv")

    # Convert to (stationID, entryType, temperature) tuples
    parsedLines = lines.map(parse_line)

    # Filter out all but TMIN entries
    minTemps = parsedLines.filter(lambda x: "TMIN" in x[1])

    # Convert to (stationID, temperature)
    stationTemps = minTemps.map(lambda x: (x[0], x[2]))

    # Reduce by stationID retaining the minimum temperature found
    minTemps = stationTemps.reduceByKey(lambda x, y: min(x, y))

    # Collect, format, and print the results
    results = minTemps.collect()

    for result in results:
        print(result[0] + "\t{:.2f}F".format(result[1]))
