package spark.odyssey.cooccur

/**
 * A word pair for co-occurrence algorithm
 * A word pair is a tuple of <word, neighbor>
 */
case class WordPair(val word: String, val neighbor: String) {
  override def toString: String = word + "," + neighbor
}

object WordPair {
  def apply(input: String): WordPair = {
    val splitWords = input.toString.split(" ")
    new WordPair(splitWords.apply(0), splitWords.apply(1))
  }
}