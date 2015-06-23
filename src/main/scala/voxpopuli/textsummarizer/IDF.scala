package voxpopuli.textsummarizer

import java.io.ObjectOutputStream

import breeze.linalg.DenseVector
import breeze.linalg.DenseVector
import com.clearspring.analytics.stream.frequency.CountMinSketch
import org.apache.spark.annotation.Experimental
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.mllib.feature.IDFModel
import org.apache.spark.mllib.linalg.{Vectors, DenseVector, SparseVector, Vector}
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.rdd.RDDFunctions._

import scala.collection.mutable

/**
 * Adapted from the Spark-MLLib class in package org.apache.spark.mllib.feature
 *
 * @author "Lorenz Fischer" <lfischer@ifi.uzh.ch>
 */
class Idf(val minDocFreq: Int = 0) {

  /**
   * Computes the inverse document frequency.
   * @param dataset an RDD of term frequency vectors
   */
  def fit(dataset: RDD[TermFrequencies[_]]): IdfMapModel = {
    val idf = dataset.treeAggregate(new Idf.DocumentFrequencyAggregator(
      minDocFreq = minDocFreq))(
        seqOp = (df, v) => df.add(v),
        combOp = (df1, df2) => df1.merge(df2)
      )

    // todo: implement something to drop the X% most uninformative words

    new IdfMapModel(idf)
  }
}

object Idf {

  /** Document frequency aggregator. */
  class DocumentFrequencyAggregator(var minDocFreq: Int = 0) extends Serializable {

    /** Merge constructor. */
    def this(first: DocumentFrequencyAggregator, second: DocumentFrequencyAggregator) = {
      this(math.min(first.minDocFreq, second.minDocFreq))
      this.numDocs = first.numDocs + second.numDocs

      if (first.dfCms == null && second.dfCms == null) {
        throw new IllegalStateException("At least one of the sketches should be non-null")
      }
      this.dfCms = if (first.dfCms != null && second.dfCms != null) {
        CountMinSketch.merge(first.dfCms, second.dfCms)
      } else if (first.dfCms != null) {
        first.dfCms
      } else {
        second.dfCms
      }

    }

    /*
       from: https://groups.google.com/forum/#!searchin/stream-lib-user/depth$20width/stream-lib-user/dhoMaeNTmL0/P0SUM8WS98wJ
       and:

       As I understood:
        T = total count (in my case termLength * 20k, so say 200k for a term length of 10)
        W = width of array for each hash function
        D = depth of structure = number of hash functions

       The error is computed as a proportion of (T) the sum of the counts inserted into the data structure.
       The error rate is T * e (2.71828) / width.
       The confidence of the estimation falling within the error is 1 - e ^ -depth, so for a depth of 10, I get
       a confidence of 99.995%.

       So let's say I want an error rate of no more than 1%
       -> err = T * 2.71828 / width
       -> width = T * 2.71828 / err
       -> width = 200k * 2.71828 / 0.01 = 54.365.600 =~ 55mio, when I try this, I run OOM, though..
      */
    private[textsummarizer] val depth = 10
    private[textsummarizer] val width = 500 * 1000

    /** number of documents processed in this IDF */
    var numDocs = 0L

    /** Document frequency countMinSketch. In this sketch, we count the number of documents, in which the terms occur. */
    private[textsummarizer] var dfCms = new CountMinSketch(depth, width, 13)

    /**
     * Computes the inverse document frequency for the given term.
     * @param term the idf for the term, iff the term was observed in more than the minimum required number of documents.
     * @return the idf for the term.
     */
    def of(term: String): Double = {
      if (minDocFreq == 0 || dfCms.estimateCount(term) >= minDocFreq) {
        val estimatedCount = dfCms.estimateCount(term)
        return calcIdf(numDocs, estimatedCount)
      } else {
        return 0.0D
      }
    }

    /**
     * Adds a new document.
     * @param doc term frequencies of one document object
     */
    def add(doc: TermFrequencies[_]): this.type = {
      // add all term frequencies of the doc into the df map
      for ((term, tf) <- doc.termFrequencies) {
        // the first element of the outer tuple is the document id
        dfCms.add(term, 1) // here we count documents
      }
      numDocs += 1L
      this
    }

    /** Merges another. */
    def merge(other: DocumentFrequencyAggregator): DocumentFrequencyAggregator = {
      new DocumentFrequencyAggregator(this, other)
    }

    @throws(classOf[java.io.IOException])
    private def writeObject(out: ObjectOutputStream): Unit = {
      out.writeInt(minDocFreq)
      out.writeLong(numDocs)

      val bytesDf = CountMinSketch.serialize(dfCms)
      out.writeInt(bytesDf.length)
      out.write(bytesDf)
    }

    @throws(classOf[java.io.IOException])
    @throws(classOf[ClassNotFoundException])
    private def readObject(in: java.io.ObjectInputStream): Unit = {
      this.minDocFreq = in.readInt()
      this.numDocs = in.readLong()

      val bytesDfLength = in.readInt()
      val bytesDf = new Array[Byte](bytesDfLength)
      in.readFully(bytesDf)
      this.dfCms = CountMinSketch.deserialize(bytesDf)
    }

  }

  /**
   * This method computes the document frequency of a term, given the total number of documents in the corpus and
   * the term frequency of the term itself.
   * @param numDocs the number of documents in the corpus.
   * @param docFreq number of documents in which the term appears.
   * @return the computed document frequency.
   */
  def calcIdf(numDocs: Long, docFreq: Long) = math.log((numDocs + 1.0) / (docFreq + 1.0))

}


/**
 * :: Experimental ::
 * Represents an IDF model that can transform term frequency maps.
 */
@Experimental
class IdfMapModel(val idf: Idf.DocumentFrequencyAggregator) extends Serializable {

  /**
   * Transforms term frequency (TF) tuples to TF-IDF tuples.
   *
   * @param dataset an RDD of term frequency tuple (id, term-tf-pairs)
   * @return an RDD of TF-IDF tuples (id, term-tfidf-pairs)
   */
  def transform(dataset: RDD[TermFrequencies[_]]): RDD[TermFrequencies[_]] = {
    val bcIdf = dataset.context.broadcast(idf)
    dataset.mapPartitions { iter =>
      val thisIdf = bcIdf.value
      iter.map { v =>
        val docId = v.docId
        val df = v.termFrequencies

        val transformed = df.map{ termTf =>
          val term = termTf._1
          val tf = termTf._2
          val tfIdf = tf * thisIdf.of(term)
          (termTf._1, tfIdf)
        }.filter(_._2 > 0)

        // return the transformed values along with the doc id
        new TermFrequencies(docId, transformed)
      }
    }
  }

  /**
   * Transforms term frequency (TF) vectors to TF-IDF vectors (Java version).
   * @param dataset a JavaRDD of term frequency vectors
   * @return a JavaRDD of TF-IDF vectors
   */
  def transform(dataset: JavaRDD[(_, Seq[(String, Double)])]): JavaRDD[(_, Seq[(String, Double)])] = {
    transform(dataset.rdd).toJavaRDD()
  }
}