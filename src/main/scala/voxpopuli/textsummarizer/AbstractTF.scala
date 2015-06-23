package voxpopuli.textsummarizer

import java.lang.{Iterable => JavaIterable}

import spire.math.poly.Term
import voxpopuli.util.{NlpUtils, Utils}

import scala.collection.JavaConverters._

import org.apache.spark.api.java.JavaRDD
import org.apache.spark.rdd.RDD

import scala.collection.mutable

/**
 * This is an abstract version of the term frequency calculator.
 *
 * The data structure it holds is a tuple2 consisting of an id (key) and a sequence of term-frequency pairs.
 *
 * @author "Lorenz Fischer" <lfischer@ifi.uzh.ch>
 */
abstract class AbstractTf extends Serializable {

  import org.apache.spark.SparkContext._

  /**
   * Transforms the input document into a sequence of terms.
   */
  protected def createTerms(document: String): Seq[String]

  /**
   * Transforms the input document into a sparse term frequency vector as as as a map of
   * @param document the document to parse.
   */
  def computeTermFrequencies(document: Doc[_]): TermFrequencies[_] = {
    val termFrequencies = mutable.HashMap.empty[String, Double]

    createTerms(document.text).foreach { term =>
      termFrequencies.put(term, termFrequencies.getOrElse(term, 0.0) + 1.0)
    }

    return new TermFrequencies(document.docId, termFrequencies.toSeq)
  }

  /**
   * @param dataset the dataset containing the doucments to parse.
   */
  def createTfFromDoc(dataset: RDD[Doc[_]]): (RDD[TermFrequencies[_]], collection.Map[String, String]) = {
    // we do this in two steps:
    // 1. we generate the regular term frequencies
    // 2. we compute the stemmed version over each term, count how many times each version occurs, and store
    //    the most common 'unstemmed' version in a dictionary

    val unstemmedFrequencies = dataset.map(this.computeTermFrequencies)

    // compute frequencies of terms across all documents
    val globalTermFreqs = unstemmedFrequencies.flatMap(_.termFrequencies).reduceByKey(_ + _)

    // now compute the stemmed forms (stemmedVersion, (originalVersion, count))
    val globalStemmedFreqs = globalTermFreqs.map { termFreq =>
      (NlpUtils.normalizeTerm(termFreq._1), (termFreq._1, termFreq._2))
    }

    // reduce by each stemmed form, take the top count 'original' version as the unstemmed form
    val stemmedToTermMap = globalStemmedFreqs.groupByKey().map { globalStemmedFreqs =>
      val stemmedVersion = globalStemmedFreqs._1
      val originalVersion = globalStemmedFreqs._2.maxBy(_._2)._1
      (stemmedVersion, originalVersion)
    }.collectAsMap()

    val stemmedFrequencies: RDD[TermFrequencies[_]] = unstemmedFrequencies.map { freqs =>
      val stemmedFreqsWithDups = freqs.termFrequencies.map { freq => (NlpUtils.normalizeTerm(freq._1), freq._2) }

      // now we could still have duplicates in the stemmed form
      val stemmedFreqsUnique = stemmedFreqsWithDups.groupBy(_._1).map { elem =>
        val stemmedTerm = elem._1
        val totalFreq = elem._2.map(_._2).sum

        (stemmedTerm, totalFreq)
      }.toSeq

      new TermFrequencies(freqs.docId, stemmedFreqsUnique)
    }

    // return the stemmed TermFrequencies objects
    (stemmedFrequencies, stemmedToTermMap)
  }
}

case class ArticleId(articleId: String)
case class CommentId(articleId: String, commentId: String)

/**
 * @tparam K the key with which the document should be identified (typically a url/string).
 */
class Doc[K](val docId: K, val text: String)




