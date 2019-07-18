package spark.odyssey.colloc

case class Bigram(val source: String, val target: String) {
    def isValidBigram: Boolean = !source.equals(" ") && !target.equals(" ")
}

object Bigram {
  def apply(input: String): Bigram = {
    val splitWords = input.toString.split(" ")
    new Bigram(splitWords.apply(0), splitWords.apply(1))
  }
}