package spark.odyssey.cooccur

import org.apache.spark.{SparkConf, SparkContext}

import scala.language.existentials

object Driver {

  type OptionMap = Map[Symbol, String]
  val usage: String =
    """
      |Usage: cooccur -n <neighbors> -m <minCount> input output
    """.stripMargin

  def main(args: Array[String]): Unit = {
    if (args.length < 4) {
      println("Dunno: " + args.toList + "\n" + usage)
      sys.exit(-1)
    }
    val argsList = args.toList
    val optionMap = parseOptions(argsList)

    val conf = new SparkConf()
      .setAppName("cooccur")
      .set("spark.hadoop.validateOutputSpecs", "false") //overwrite output directory

    val sc = new SparkContext(conf)

    //pass the containing jar to the spark context
    SparkContext.jarOfClass(this.getClass)
    
    val entries = sc.textFile(optionMap('input))

    val cooccur = new Cooccur().cooccur(sc, entries, optionMap)
    cooccur.saveAsTextFile(optionMap('output))
    sc.stop()
  }

  def parseOptions(argsList: List[String]): OptionMap = {
    def nextOption(map: OptionMap, list: List[String]): OptionMap = {
      list match {
        case Nil => map
        case "-n" :: value :: tail => nextOption(map ++ Map('neighbors -> value.toString), tail)
        case "-min" :: value :: tail => nextOption(map ++ Map('minCount -> value.toString), tail)
        case "-top" :: value :: tail => nextOption(map ++ Map('top -> value.toString), tail)
        case string :: opt2 :: tail => nextOption(map ++ Map('input -> string, 'output -> opt2), tail)
        case option :: tail => throw new RuntimeException("Unknown option " + option)
      }
    }

    nextOption(Map(), argsList)
  }
}
