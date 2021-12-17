import scala.reflect.io.File

object ArgsParser {

    def parser(args: Array[String]) = {
        if (args.length == 0) println("No args provided, default values will be used (if possible)")
        val arglist = args.toList
        type OptionMap = Map[Symbol, String]

        def optionMap(map: OptionMap, list: List[String]) : OptionMap = {
            def isSwitch(s: String) = (s(0) == '-')
            list match {
                case Nil => map
                case "--flightDataFilepath" :: value :: tail =>
                                        optionMap(map ++ Map('flightDataFilepath -> value), tail)
                case "--passengersFilepath" :: value :: tail =>
                                        optionMap(map ++ Map('passengersFilepath -> value), tail)
                case "--atLeastNTimes" :: value :: tail =>
                                        optionMap(map ++ Map('atLeastNTimes -> value), tail)
                case "--dateFrom" :: value :: tail =>
                                        optionMap(map ++ Map('from -> value), tail)
                case "--dateTo" :: value :: tail =>
                                        optionMap(map ++ Map('to -> value), tail)
                case option :: tail => println("Unknown option " + option) 
                                        sys.exit(1) 
            }
        }
        val options = optionMap(Map(), arglist)
        
        val flightDataDefaultFilepath = File(".").toAbsolute.parent / "data/flightData.csv"
        val passengersDefaultFilepath = File(".").toAbsolute.parent / "data/passengers.csv"

        (
            options.getOrElse('flightDataFilepath, flightDataDefaultFilepath.toString),
            options.getOrElse('passengersFilepath, passengersDefaultFilepath.toString),
            options.getOrElse('atLeastNTimes, "5"),
            options.getOrElse('from, "2017-02-01"),
            options.getOrElse('to, "2017-11-30"),
        )
    }
}

