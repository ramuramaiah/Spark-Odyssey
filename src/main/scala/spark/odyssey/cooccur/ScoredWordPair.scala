package spark.odyssey.cooccur

/**
 * A scored word pair for sorting the neighbors based on the count
 */
case class ScoredWordPair(val wordPair: WordPair, val score: Int) {
  override def toString: String = wordPair.toString + "," + score
}

object ScoredWordPair {
  def apply(input: String): ScoredWordPair = {
    val splits = input.toString.split(",")
    new ScoredWordPair(new WordPair(splits.apply(0), splits.apply(1)), splits.apply(2).toInt)
  }    
}