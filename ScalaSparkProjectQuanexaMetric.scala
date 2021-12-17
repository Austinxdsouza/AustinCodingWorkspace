import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

object Metric {

    // Question 1 - Find the total number of flights for each month
    def computeQuestionOne(flightDataDF: DataFrame) = {
        // Extracting month from date in the format yyyy-MM-dd
        val parser : (String => Int) = (arg: String) => {arg.split('-')(1).toInt}
        val dateToMonth = udf(parser)

        // Filtering out duplicate flights ID
        val parsedDf = flightDataDF.withColumn("date", dateToMonth(flightDataDF("date")))
                                .drop("passengerId", "from", "to")
                                .distinct()

        // Counting flights by Month
        parsedDf.groupBy("date")
                .count
                .sort("date")
                .withColumnRenamed("date", "Month")
                .withColumnRenamed("count", "Number of Flights")
                .write.option("header",true)
                .csv("output/question1")
    }

    // Querstion 2 - Find the names of the 100 most frequent flyers
    def computeQuestionTwo(flightDataDF: DataFrame, passengersDF: DataFrame) = {
        // Assigning alias for join method
        val filteredFlightsDF = flightDataDF.drop("flightId", "from", "to", "date").as("filteredFlightsDF")
        val passengersInfoDF = passengersDF.as("passengersInfoDF")

        // Joining dataframe on passenger ID
        val joinedDF = filteredFlightsDF.join(passengersInfoDF,
                                              col("filteredFlightsDF.passengerId") === col("passengersInfoDF.passengerId"),
                                              "inner")
                                        .select("filteredFlightsDF.passengerId", "firstName", "lastName")

        // Counting flights by passenger, sorting and limiting the result
        joinedDF.groupBy("passengerId", "firstName", "lastName")
                .count
                .sort(desc("count"))
                .withColumnRenamed("count", "Number of Flights")
                .limit(100)
                .write.option("header",true)
                .csv("output/question2")
    }

    // Question 3 - Find the greatest number of countries a passenger has been in without being in the UK
    def computeQuestionThree(flightDataDF: DataFrame) = {
        // Merging date, from and to fields into a single one
        val filteredFlightsDF = flightDataDF.withColumn("flights", array("date", "from", "to"))
                                            .drop("flightId", "date", "from", "to")

        // Aggregating flights by passenger ID to have a list of flights given an ID
        val sortedFlightsDF = filteredFlightsDF.groupBy(col("passengerId"))
                                               .agg(collect_list(col("flights")) as "flightsList")
                                               .withColumn("flightsList", sort_array(col("flightsList")))

        // Function to compute the longest run given an array of flights in the format [date, from, to]
        def longestRun(flights: Array[Array[String]]) = {
            var countrySet : Set[String] = Set()
            var longest = 1

            for (flight <- flights) {
                if (flight(1) != "uk" && flight(2) != "uk") {countrySet += (flight(1), flight(2))}
                else if (flight(1) == "uk") {countrySet += flight(2)}
                else if (flight(2) == "uk") {countrySet += flight(1); longest = math.max(longest, countrySet.size)}
            }

            countrySet.size
        }
        val computeLongestRun = udf(longestRun _)

        // Computing the longest run for each passenger
        sortedFlightsDF.withColumn("Longest Run", computeLongestRun(col("flightsList")))
                       .drop("flightsList")
                       .write.option("header",true)
                       .csv("output/question3")

    }

    // Question 4 - Find the passengers who have been on more than 3 flights together
    def computeQuestionFour(flightDataDF: DataFrame) = {
        // Filtering out useless field and set an alias for self-join
        val filteredFlightsDF = flightDataDF.drop("from", "to")
                                            .withColumnRenamed("passengerId", "Passenger 1 ID")
                                            .as("filteredFlightsDF")

        // Self-join to have pairs of passengers. `Passenger 1 ID < Passenger 2 ID` creates pairs skipping duplicates and self-pair
        val joinedDF = filteredFlightsDF.as("df1").join(filteredFlightsDF.withColumnRenamed("Passenger 1 ID", "Passenger 2 ID").as("df2"),
                                                        col("Passenger 1 ID") < col("Passenger 2 ID") &&
                                                        col("df1.flightId") === col("df2.flightId") &&
                                                        col("df1.date") === col("df2.date"),
                                                        "inner")
                                                   .select("Passenger 1 ID", "Passenger 2 ID", "df1.flightId")

        // Couting flights together and filtering out results lower than 3
        joinedDF.groupBy("Passenger 1 ID", "Passenger 2 ID")
                .count
                .where(col("count") >= 3)
                .withColumnRenamed("count", "Number of Flights together")
                .write.option("header",true)
                .csv("output/question4")
    }

    // Bonus question - Find the passengers who have been on more than N flights together within the range (from,to)
    // Pretty similar to question 4, just changed `where` method with a more generic `agg`
    def computeBonusQuestion(flightDataDF: DataFrame, atLeastNTimes: Int, from: String, to: String) = {
        val filteredFlightsDF = flightDataDF.drop("from", "to")
                                            .withColumnRenamed("passengerId", "Passenger 1 ID")
                                            .as("filteredFlightsDF")

        val joinedDF = filteredFlightsDF.as("df1").join(filteredFlightsDF.withColumnRenamed("Passenger 1 ID", "Passenger 2 ID").as("df2"),
                                                        col("Passenger 1 ID") < col("Passenger 2 ID") &&
                                                        col("df1.flightId") === col("df2.flightId") &&
                                                        col("df1.date") === col("df2.date"),
                                                        "inner")
                                                   .select("Passenger 1 ID", "Passenger 2 ID", "df1.flightId", "df1.date")

        joinedDF.groupBy("Passenger 1 ID", "Passenger 2 ID")
                .agg(count("*").as("Number of Flights together"), min(col("df1.date")).as("From"), max(col("df1.date")).as("To"))
                .where(col("Number of Flights together") >= atLeastNTimes && col("From") >= from && col("To") <= to)
                .write.option("header",true)
                .csv("output/bonus_question")
    }
    
}
