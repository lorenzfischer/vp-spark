/**
 * @author lorenz.fischer@gmail.com
 */
object ZipWithIndexTester {

  def main(args: Array[String]): Unit = {
    val text = "hello how are you no you you funny hello you"

    text.split(" ").zipWithIndex.toMap.iterator.foreach( termIndex => println(s"$termIndex"))
  }

}
