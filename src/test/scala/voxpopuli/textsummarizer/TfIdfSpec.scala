package voxpopuli.textsummarizer

import java.util.Date

import org.apache.spark.rdd.RDD
import org.joda.time.{Years, MonthDay, DateTime, Days}
import org.scalatest._
import voxpopuli.DataMuncher
import voxpopuli.testutil.SparkUtils
import voxpopuli.testutil.SparkUtils
import voxpopuli.util.JsonUtils
import voxpopuli.util.JsonUtils.{Term, OpinionItem}

/**
 * @author lorenz.fischer@gmail.com
 */
class TfIdfSpec extends FlatSpec with Matchers with SparkUtils {

  "TfIdf" should "compute tfidf for one doc" in spark() {
    val line1 = """{"_id" : "doc1", "title" : "word1", "lead" : "word1 word2", "text" : "word1 word2 word3", "date_scraped" : "2014-12-23T14:00:11Z", "date_published" : "2014-12-23T14:09:00Z", "type" : "type-value", "type_version" : 1 }"""

    val paraData = sc.parallelize(List(line1))
    val documents = paraData.map(JsonUtils.parseArticle).flatMap(DataMuncher.createDocs(_))
    val (tf, stemmedToTermMap) = new MultiWordTf(1).createTfFromDoc(documents)
    tf.cache()

    /*
      doc1:
      word1
      word1 word2
      word1 word2 word3 word1

      augmented TF
      maxFreq: 4
      word1 = 4/4 = 1
      word2 = 2/4 = 0.5
      word3 = 1/4 = 0.25

      idf:
      numDocs = 1
      word1 = calcDocFreq(1, 1)
      word2 = calcDocFreq(1, 0.5)
      word3 = calcDocFreq(1, 0.25)
     */

    //org.apache.spark.mllib.feature.IDF
    val idf = new Idf(minDocFreq = 1).fit(tf)

    val tfidf = idf.transform(tf)

    //val topTfIdf = tfidf.map( tf => tf.retainTopPercentage(0.05))
    val tfIdfArray = tfidf.collect()
    val results = tfIdfArray.map(entry => (entry.docId, entry.termFrequencies)).toMap

    val tfInDoc1 = results.get(ArticleId("doc1")).get.toMap

    val word1Tf = 4.0 / 4
    val word1Idf = Idf.calcIdf(1, 1)
    val word1TfIdf = word1Tf * word1Idf
    tfInDoc1.get("word1").getOrElse(0.0D) shouldBe word1TfIdf // if it's not found, tfidf is 0

    val word2Tf = 2.0 / 4
    val word2Idf = Idf.calcIdf(1, 1)
    val word2TfIdf = word2Tf * word2Idf
    tfInDoc1.get("word2").getOrElse(0.0D) shouldBe word2TfIdf

    val word3Tf = 1.0 / 4
    val word3Idf = Idf.calcIdf(1, 1)
    val word3TfIdf = word3Tf * word3Idf
    tfInDoc1.get("word3").getOrElse(0.0D) shouldBe word3TfIdf
  }


  it should "compute tfidf for multiple docs" in spark() {
    val line1 = """{"_id" : "doc1", "title" : "word1", "lead" : "word1 word2", "text" : "word1 word2 word3 word1", "date_scraped" : "2014-12-23T14:00:11Z", "date_published" : "2014-12-23T14:09:00Z", "type" : "type-value", "type_version" : 1 }"""
    val line2 = """{"_id" : "doc2", "title" : "word4", "lead" : "word4 word5", "text" : "word4 word5 word6 word1", "date_scraped" : "2014-12-23T14:00:11Z", "date_published" : "2014-12-23T14:09:00Z", "type" : "type-value", "type_version" : 1 }"""

    val paraData = sc.parallelize(List(line1, line2))
    val documents = paraData.map(JsonUtils.parseArticle).flatMap(DataMuncher.createDocs(_))
    val (tf, stemmedToTermMap) = new MultiWordTf(1).createTfFromDoc(documents)
    tf.cache()

    /*
      doc1:
      word1
      word1 word2
      word1 word2 word3 word1

      augmented TF doc1
      maxFreq: 4
      word1 = 4/4 = 1
      word2 = 2/4 = 0.5
      word3 = 1/4 = 0.25


      doc2
      word4
      word4 word5
      word5 word6 word1

      augmented TF doc2
      maxFreq: 2
      word4: 2/2
      word5: 2/2
      word6: 1/2
      word1: 1/2

      idf:
      numDocs = 2
      word1 = calcDocFreq(2, 2)
      word2 = calcDocFreq(2, 1)
      word3 = calcDocFreq(2, 1)
      word4 = calcDocFreq(2, 1)
      word5 = calcDocFreq(2, 1)
      word6 = calcDocFreq(2, 1)
     */

    //org.apache.spark.mllib.feature.IDF
    val idf = new Idf(minDocFreq = 1).fit(tf)

    val tfidf = idf.transform(tf)
    //val topTfIdf = tfidf.map( tf => tf.retainTopPercentage(0.05))
    val tfIdfArray = tfidf.collect()
    val results = tfIdfArray.map(entry => (entry.docId, entry.termFrequencies)).toMap

    val tfInDoc1 = results.get(ArticleId("doc1")).get.toMap
    val tfInDoc2 = results.get(ArticleId("doc2")).get.toMap

    val word1Tf = 4.0 / 4
    val word1Idf = Idf.calcIdf(2, 2)
    val word1TfIdf = word1Tf * word1Idf
    tfInDoc1.get("word1").getOrElse(0.0D) shouldBe word1TfIdf

    val word2Tf = 2.0 / 4
    val word2Idf = Idf.calcIdf(2, 1)
    val word2TfIdf = word2Tf * word2Idf
    tfInDoc1.get("word2").getOrElse(0.0D) shouldBe word2TfIdf

    val word3Tf = 1.0 / 4
    val word3Idf = Idf.calcIdf(2, 1)
    val word3TfIdf = word3Tf * word3Idf
    tfInDoc1.get("word3").getOrElse(0.0D) shouldBe word3TfIdf


    val word4Tf = 3.0 / 3
    val word4Idf = Idf.calcIdf(2, 1)
    val word4TfIdf = word4Tf * word4Idf
    tfInDoc2.get("word4").getOrElse(0.0D) shouldBe word4TfIdf

    val word5Tf = 2.0 / 3
    val word5Idf = Idf.calcIdf(2, 1)
    val word5TfIdf = word5Tf * word5Idf
    tfInDoc2.get("word5").getOrElse(0.0D) shouldBe word5TfIdf

    val word6Tf = 1.0 / 3
    val word6Idf = Idf.calcIdf(2, 1)
    val word6TfIdf = word6Tf * word6Idf
    tfInDoc2.get("word6").getOrElse(0.0D) shouldBe word6TfIdf

    val word12Tf = 1.0 / 3
    val word12Idf = Idf.calcIdf(2, 2)
    val word12TfIdf = word12Tf * word12Idf
    tfInDoc2.get("word1").getOrElse(0.0D) shouldBe word12TfIdf
  }


  it should "remove all tfidf with low docFreqs" in spark() {
    val line1 = """{"_id" : "doc1", "title" : "word1", "lead" : "word1 word2", "text" : "word1 word2 word3 word1", "date_scraped" : "2014-12-23T14:00:11Z", "date_published" : "2014-12-23T14:09:00Z", "type" : "type-value", "type_version" : 1 }"""
    val line2 = """{"_id" : "doc2", "title" : "word4", "lead" : "word4 word5", "text" : "word4 word5 word6 word1", "date_scraped" : "2014-12-23T14:00:11Z", "date_published" : "2014-12-23T14:09:00Z", "type" : "type-value", "type_version" : 1 }"""

    val paraData = sc.parallelize(List(line1, line2))
    val documents = paraData.map(JsonUtils.parseArticle).flatMap(DataMuncher.createDocs(_))
    val (tf, stemmedToTermMap) = new MultiWordTf(1).createTfFromDoc(documents)
    tf.cache()

    /*
      doc1:
      word1
      word1 word2
      word1 word2 word3 word1

      augmented TF doc1
      maxFreq: 4
      word1 = 4/4 = 1
      word2 = 2/4 = 0.5
      word3 = 1/4 = 0.25


      doc2
      word4
      word4 word5
      word5 word6 word1

      augmented TF doc2
      maxFreq: 2
      word4: 2/2
      word5: 2/2
      word6: 1/2
      word1: 1/2

      idf:
      numDocs = 2
      word1 = calcDocFreq(2, 2)
      word2 = calcDocFreq(2, 1)
      word3 = calcDocFreq(2, 1)
      word4 = calcDocFreq(2, 1)
      word5 = calcDocFreq(2, 1)
      word6 = calcDocFreq(2, 1)
     */

    //org.apache.spark.mllib.feature.IDF
    val idf = new Idf(minDocFreq = 2).fit(tf)

    val tfidf = idf.transform(tf)
    //val topTfIdf = tfidf.map( tf => tf.retainTopPercentage(0.05))
    val tfIdfArray = tfidf.collect()
    val results = tfIdfArray.map(entry => (entry.docId, entry.termFrequencies)).toMap

    val tfInDoc1 = results.get(ArticleId("doc1")).get.toMap
    val tfInDoc2 = results.get(ArticleId("doc2")).get.toMap

    tfInDoc1.get("word1") shouldBe None // occurs in all docs
    tfInDoc1.get("word2") shouldBe None // occurs in no docs
    tfInDoc1.get("word3") shouldBe None // occurs in no docs
    tfInDoc2.get("word4") shouldBe None // occurs in no docs

    tfInDoc2.get("word5") shouldBe None // occurs in no docs
    tfInDoc2.get("word6") shouldBe None // occurs in no docs
    tfInDoc2.get("word1") shouldBe None // occurs in all docs
  }

}
