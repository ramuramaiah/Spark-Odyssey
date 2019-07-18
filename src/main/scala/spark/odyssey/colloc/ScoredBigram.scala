package spark.odyssey.colloc

case class ScoredBigram(val bigram: Bigram, val score: Double) {
  override def toString: String = bigram.source + "," + bigram.target + "," + score
}

object ScoredBigram {
  def apply(input: String): ScoredBigram = {
    val splits = input.toString.split(",")
    new ScoredBigram(new Bigram(splits.apply(0), splits.apply(1)), splits.apply(2).toDouble)
  }    
}