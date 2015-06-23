package voxpopuli.textsummarizer

import org.apache.spark.mllib.linalg.{Vectors, Vector}
import voxpopuli.util.NlpUtils

import scala.collection.mutable

/**
 * This term frequency calculator maps a sequence of texts to a sequence of terms within these texts and computes
 * their respective term frequencies.
 *
 * @author "Lorenz Fischer" <lfischer@ifi.uzh.ch>
 */
class SingleWordTf extends AbstractTf with AugmentedTermFrequencies {
  /**
   * Transforms the input document into a sequence of terms.
   */
  override def createTerms(document: String): Seq[String] = {
    NlpUtils.computeTerms(document)
  }
}
