package spark.odyssey.colloc

import org.apache.spark.{SparkConf, SparkContext}

import scala.language.existentials

object Driver {

  type OptionMap = Map[Symbol, String]
  val usage: String =
    """
      |Usage: colloc --algo [g_2|freq] -s <minSupportValue> -ml <minLLR> input output
    """.stripMargin

  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      println("Dunno: " + args.toList + "\n" + usage)
      sys.exit(-1)
    }
    val argsList = args.toList
    val optionMap = parseOptions(argsList)

    val conf = new SparkConf()
      .setAppName("colloc")
      .set("spark.hadoop.validateOutputSpecs", "false") //overwrite output directory

    val sc = new SparkContext(conf)

    //pass the containing jar to the spark context
    SparkContext.jarOfClass(this.getClass)
    
    val mappings = sc.textFile(optionMap('input))

    val analyzer = optionMap('algo) match {
      case "g_2" => new Analyzer().g_2(sc, mappings, optionMap)
      case "freq" => new Analyzer().rawFrequency(mappings)
      case _ => throw new RuntimeException("Unknown algorithm: " + optionMap('algo))
    }
    analyzer.saveAsTextFile(optionMap('output))
    sc.stop()
  }

  def parseOptions(argsList: List[String]): OptionMap = {
    def nextOption(map: OptionMap, list: List[String]): OptionMap = {
      list match {
        case Nil => map
        case "--algo" :: value :: tail => nextOption(map ++ Map('algo -> value.toString), tail)
        case "-s" :: value :: tail => nextOption(map ++ Map('minSupport -> value.toString), tail)
        case "-ml" :: value :: tail => nextOption(map ++ Map('minLLR -> value.toString), tail)
        case string :: opt2 :: tail => nextOption(map ++ Map('input -> string, 'output -> opt2), tail)
        case option :: tail => throw new RuntimeException("Unknown option " + option)
      }
    }

    nextOption(Map(), argsList)
  }
}
