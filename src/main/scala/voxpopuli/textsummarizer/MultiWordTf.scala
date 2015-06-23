package voxpopuli.textsummarizer

import voxpopuli.util.NlpUtils

/**
 * This term frequency calculator maps a sequence of texts to a sequence of terms within these texts.
 *
 * @author "Lorenz Fischer" <lfischer@ifi.uzh.ch>
 */
class MultiWordTf(val maxTermLength: Int = 1) extends AbstractTf with AugmentedTermFrequencies {

  /**
   * Transforms the input document into a sequence of terms.
   */
  override def createTerms(document: String): Seq[String] = {
    NlpUtils.computeTerms(document, maxTermLength).map(NlpUtils.normalizeTerm)
  }
}
