package spark.odyssey.colloc

import org.apache.mahout.math.stats.LogLikelihood
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import spark.odyssey.colloc._

/*
  * Analyzer implements routines to rank pairs of consecutive mappings using Spark.
  * The approaches implemented here:
  * * raw frequency
  * * G_2 log likelihood metric
  */
class Analyzer extends Serializable {

  type BigramCount = (Bigram, Int)
  
  /*
    * Returns a RDD with a scored bigram based on frequency of mappings.
    *
    * @param commands an RDD containing Strings of mappings
    * @return An RDD of ScoredBigram sorted by score
    */
  def rawFrequency(mappings: RDD[String]): RDD[ScoredBigram] = {
    val mappingsAsBigrams = mappings.map(Bigram.apply).filter(_.isValidBigram)

    val bigramCounts = mappingsAsBigrams.map((bigram: Bigram) => (bigram, 1))
      .reduceByKey((x: Int, y: Int) => x + y)
    val totalNumBigrams = mappingsAsBigrams.count()

    bigramCounts.map((bigramCount: BigramCount) => (1.0 * bigramCount._2 / totalNumBigrams, bigramCount._1))
      .sortByKey(false) //sort the value (it's the first in the pair)
      .map((x: Tuple2[Double, Bigram]) => new ScoredBigram(x._2, x._1)) //now invert the order of the pair
  }

  /*
    * Returns a RDD with a scored bigram based on the G^2 statistical test as popularized in Dunning, Ted (1993).
    * Accurate Methods for the Statistics of Surprise and Coincidence, Computational Linguistics, Volume 19, issue 1 (March, 1993).
    *
    *
    * @param mappings an RDD containing Strings of mappings
    * @return An RDD of ScoredBigram sorted by score
    */
  def g_2(sc: SparkContext, mappings: RDD[String], optionMap: Map[Symbol, String]): RDD[ScoredBigram] = {

    /*
      * Returns the G^2 score given mapping counts
      *
      *
      * @param p_xy        The count of mappings x and y consecutively
      * @param p_x         The count of mapping x
      * @param p_y         The count of mapping y
      * @param numMappings The total number of mappings
      * @return The G^2 score
      **/
    def calculate(p_xy: Long, p_x: Long, p_y: Long, numMappings: Long): Double = {
      val k11 = p_xy // count of x and y together
      val k12 = (p_x - p_xy) //count of x without y
      val k21 = (p_y - p_xy) // count of y without x
      val k22 = numMappings - (p_x + p_y - p_xy) //count of neither x nor y

      val llr = LogLikelihood.logLikelihoodRatio(k11.asInstanceOf[Long], k12.asInstanceOf[Long], k21.asInstanceOf[Long], k22.asInstanceOf[Long])
      BigDecimal(llr).setScale(8, BigDecimal.RoundingMode.HALF_UP).toDouble
    }

    def computeFrequency(sc: SparkContext, mappings: RDD[Bigram], gramType: Symbol) : Map[String, Int] = {
      var flatMap: RDD[String] = sc.emptyRDD
      
      if('head equals(gramType)) {
        flatMap = mappings.flatMap((bigram: Bigram) => List(bigram.source))
      } else if('tail equals(gramType)) {
        flatMap = mappings.flatMap((bigram: Bigram) => List(bigram.target))
      }
      
      val mappingCounts = flatMap
      .map((mapping: String) => (mapping, 1))
      .reduceByKey((x: Int, y: Int) => x + y)

      // Collect the array of mapping to count pairs locally into an array
      val mappingCountsArr = mappingCounts.collect()

      // Create a Map from those pairs
      Map(mappingCountsArr: _*)
    }
    
    // Convert the mappings to lists of mapping pairs
    val mappingsAsBigrams = mappings.map(Bigram.apply).filter(_.isValidBigram)
    
    val totalNumBigrams = mappingsAsBigrams.count()

    // compute the head frequency
    val headCountsMap = computeFrequency(sc, mappingsAsBigrams, 'head)
    
    // compute the tail frequency
    val tailCountsMap = computeFrequency(sc, mappingsAsBigrams, 'tail)
    
    //This is a pre-filter to drop the mappings which are below the threshold value.
    val minSupport: Int = optionMap.getOrElse('minSupport, "2").toInt
    
    // Count the bigrams
    val bigramCounts = mappingsAsBigrams
    .map((bigram: Bigram) => (bigram, 1))
    .reduceByKey((x: Int, y: Int) => x + y)
    .filter((bigramCount: BigramCount) => bigramCount._2 >= minSupport)
    
    //This is a post-filter to drop the mappings which are below the LLR threshold value.
    val minLLR: Double = optionMap.getOrElse('minLLR, "1.0").toDouble
    
    // Score the bigrams
    bigramCounts
    .map( (bigramCount: BigramCount) => 
      (bigramCount._1, // the pair of mappings
      calculate (
        bigramCount._2, // bigram frequency
        headCountsMap(bigramCount._1.source), // head frequency
        tailCountsMap(bigramCount._1.target), // tail frequency
        totalNumBigrams // the total number of mappings
      ))
    )
    .map((x: Tuple2[Bigram, Double]) => new ScoredBigram(x._1, x._2))
    .filter((scoredBigram: ScoredBigram) => scoredBigram.score >= minLLR)
    .sortBy((scoredBigram: ScoredBigram) => scoredBigram.score, ascending=false)
  }

}
