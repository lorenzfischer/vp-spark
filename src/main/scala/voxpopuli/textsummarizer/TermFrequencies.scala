package voxpopuli.textsummarizer

import voxpopuli.util.Utils

/**
 * The term frequencies of all terms in a document
 *
 * @tparam K the key with which the document should be identified with.
 */
class TermFrequencies[K](val docId: K, val termFrequencies: Seq[(String, Double)]) extends Serializable {

  /**
   * Drop all frequencies and only keep the terms with a score that is equal or greater to the nth highest score.
   * @return a new term frequency object that is based on this, having all the term frequencies with scores lower
   *         than the nth highest score removed.
   */
  def retainTop(n: Int): TermFrequencies[K] = {
    // step 1: find top n scores, todo: replace with premature-quick-sort
    val topNthScore = Utils.getTopNthElement(termFrequencies.map(tf => tf._2), n)

    // step 2: only put elements whose score is more than the smallest of the top 10
    val newSeq = this.termFrequencies.filter(tf => tf._2 >= topNthScore).sortBy(_._2).reverse

    new TermFrequencies(this.docId, newSeq)
  }

  /**
   * Only keep the terms that have scores that are higher than the top p percent.
   * @return a new term frequency object that is based on this, having all the term frequencies with scores lower
   *         than the top p percent removed.
   */
  def retainTopPercentage(p: Double): TermFrequencies[K] = {
    if (p < 0.0d || p > 1.0d) {
      throw new IllegalArgumentException(s"p must be set to a value between 0.0 and 1.0. Current value = $p.")
    }

    if (this.termFrequencies.size > 0) {
      // step 1: convert percentage into absolute value
      val n: Int = Math.max((p * this.termFrequencies.size).toInt, 1)

      retainTop(n)
    } else {
      this // don't do anything if there are no terms defined
    }
  }

  override def toString(): String = {
    docId.toString + " " + termFrequencies.toString()
  }

}
