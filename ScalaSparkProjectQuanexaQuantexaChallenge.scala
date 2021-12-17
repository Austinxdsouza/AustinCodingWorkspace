import org.apache.spark.sql.SparkSession
import scala.reflect.io.File

object QuantexaChallenge {
	def main(args: Array[String]) = {
		val (flightDataFilepath, passengersFilepath,
			atLeastNTimes, from, to) = ArgsParser.parser(args)

		val spark = SparkSession.builder.appName("Quantexa Challenge").getOrCreate()

		val flightDataDF = spark.read.option("header",true).csv(flightDataFilepath)
		val passengersDF = spark.read.option("header",true).csv(passengersFilepath)

		Metric.computeQuestionOne(flightDataDF)
		Metric.computeQuestionTwo(flightDataDF, passengersDF)
		Metric.computeQuestionThree(flightDataDF)
		Metric.computeQuestionFour(flightDataDF)
		Metric.computeBonusQuestion(flightDataDF, atLeastNTimes.toInt, from, to)

		spark.stop()
	}
}