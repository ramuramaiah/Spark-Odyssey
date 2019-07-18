package spark.odyssey.cooccur

/**
 * A representation of a service. For e.g. see the below example
 * parentService child1 child2
 * The above is represented as follows
 * <parent service name> <children 1> <children 2> ...
 */
case class Service(val entry: String) {
  def hasChildrens: Boolean = {
    val tokens = entry.toString.split("\\s+")
    tokens.length > 1
  }
  
  def getChildrens: Array[String] = {
    val entries = entry.toString.split("\\s+")
    var childrens:Array[String] = null
    if(entries.length > 1) {
      childrens = new Array[String](entries.length-1)
      childrens = entries.splitAt(1)._2
    } else {
      childrens = new Array[String](0)
    }
    childrens
  }
}