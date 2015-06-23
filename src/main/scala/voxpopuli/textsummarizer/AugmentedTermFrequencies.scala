package voxpopuli.textsummarizer

/**
 * @author "Lorenz Fischer" <lfischer@ifi.uzh.ch>
 */
trait AugmentedTermFrequencies extends AbstractTf {

  override def computeTermFrequencies(document: Doc[_]): TermFrequencies[_] = {
    val tf = super.computeTermFrequencies(document)
    val maxFreq = tf.termFrequencies.foldLeft[Double](0.0D)((result, current) => Math.max(result, current._2) )
    new TermFrequencies( tf.docId, tf.termFrequencies.map( tuple => (tuple._1, tuple._2 / maxFreq) ) )
  }

}
