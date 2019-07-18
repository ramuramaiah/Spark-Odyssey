package spark.odyssey.cooccur

import scala.collection.mutable.ListBuffer
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * An implementation of co-occurrence algorithm in Spark/Scala
 * A traditional Hadoop Map-Reduce algorithm for co-occurrence can be found below
 * https://dzone.com/articles/calculating-co-occurrence
 */
class Cooccur extends Serializable {
  def cooccur(sc: SparkContext, entries: RDD[String], optionMap: Map[Symbol, String]): RDD[ScoredWordPair] = {
    val serviceEntry = entries.map(Service.apply).filter(_.hasChildrens)
    
    val neighbors: Int = optionMap.getOrElse('neighbors, "1").toInt
    
    val wordPairs  = serviceEntry.flatMap((service: Service) => (convertToWordPair(service, neighbors).toList))
    
    val minCount: Int = optionMap.getOrElse('minCount, "1").toInt
    
    val top: Int = optionMap.getOrElse('top, "5").toInt
    
    val wordPairCount = wordPairs
      .map((wordPair: WordPair) => (wordPair, 1))
      .reduceByKey((x: Int, y: Int) => x + y)
      .filter((z: Tuple2[WordPair, Int]) => (z._2 > minCount))
    
    val cooccurGroupbyKey = wordPairCount
      .map((x: Tuple2[WordPair, Int]) => new ScoredWordPair(x._1, x._2))
      .groupBy((scoredWordPair: ScoredWordPair) => scoredWordPair.wordPair.word)
      
    val cooccur = cooccurGroupbyKey.mapValues((x: Iterable[ScoredWordPair]) => x.toList).values
    val cooccurSorted = cooccur
      .flatMap(list => list.sortBy(_.score)(Ordering[Int].reverse).take(top))
    cooccurSorted
  }
  
  def convertToWordPair(service: Service, neighbors:Int): ListBuffer[WordPair] = {
    val wordPairs:ListBuffer[WordPair] = new ListBuffer[WordPair]()
    
    val childrens = service.getChildrens
    
    for (i <- 0 until childrens.length) {
      var start = if (i - neighbors < 0) 0 else i - neighbors;
      
      var end = if (i + neighbors >= childrens.length) childrens.length - 1 else i + neighbors;
      
      for (j <- start to end) {
        if (j != i) {
          wordPairs.+=(new WordPair(childrens(i), childrens(j)))
        }
      }
    }
    
    wordPairs
  }
}