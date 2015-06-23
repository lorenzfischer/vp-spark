package voxpopuli.util

/**
 * @author lorenz.fischer@gmail.com
 */
object Utils {

  /**
   * Returns the nth largest element of seq.
   * @param seq the sequence to find the element in.
   * @param n the nth element we're intested in.
   * @return nth largest element, if there are at least n elements in seq, the smallest element of the seq otherwise.
   */
  def getTopNthElement(seq: Seq[Double], n : Int): Double = {
    // todo: implement premature-quick-sort-version

    if (seq == null || seq.length == 0) {
      throw new IllegalArgumentException("The sequence must contain at least one element.")
    }

    if (n < 1) {
      throw new IllegalArgumentException(s"n must be at least 1. Current value = $n.")
    }

    val sorted = seq.sorted
    var result = sorted(0)
    if (sorted.length > n) {
      result = sorted(seq.length - n)
    }

    result
  }

  /**
   * Generates a uuid.
   * @return the generated uuid
   */
  def uuid = java.util.UUID.randomUUID.toString


}
