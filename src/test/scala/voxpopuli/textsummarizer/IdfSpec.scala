package voxpopuli.textsummarizer

import java.io._

import com.clearspring.analytics.stream.frequency.CountMinSketch
import org.apache.spark.rdd.RDD
import org.scalatest.Matchers._
import org.scalatest._
import voxpopuli.testutil.SparkUtils
import voxpopuli.textsummarizer.Idf.DocumentFrequencyAggregator

/**
 *  Code copied from: https://gist.github.com/agemooij/15a0eaebc2c1ddd5ddf4
 */
class IdfSpec extends FlatSpec  with SparkUtils {

  "Idf.DocumentFrequencyAggregator" should "correctly aggregate term frequencies" in {
    val aggregator = new Idf.DocumentFrequencyAggregator(2)
    val doc1 = new TermFrequencies("doc1", List(("hello", 0.1D), ("world", 0.1D)))
    val doc2 = new TermFrequencies("doc2", List(("hello", 0.2D), ("lorenz", 0.2D)))

    aggregator.add(doc1)
    aggregator.add(doc2)
    val idf = aggregator

    val numDocs = 2
    val sumTf = 0.1D + 0.2D
    idf.of("hello") - math.log((numDocs + 1.0) / (sumTf + 1.0)) should be < 0.000001D
  }

  it should "correctly count document frequencies" in {
    val aggregator = new Idf.DocumentFrequencyAggregator(2)
    val doc1 = new TermFrequencies("doc1", List(("hello", 0.1D), ("world", 0.1D)))
    val doc2 = new TermFrequencies("doc2", List(("hello", 0.2D), ("lorenz", 0.2D)))

    aggregator.add(doc1)
    aggregator.add(doc2)

    aggregator.dfCms.estimateCount("hello") shouldBe 2
  }

  it should "correctly aggregate term frequencies in merged aggregators" in {
    val agg1 = new Idf.DocumentFrequencyAggregator(2)
    val agg2 = new Idf.DocumentFrequencyAggregator(2)
    val doc1 = new TermFrequencies("doc1", List(("hello", 0.1D), ("world", 0.1D)))
    val doc2 = new TermFrequencies("doc2", List(("hello", 0.2D), ("lorenz", 0.2D)))

    agg1.add(doc1)
    agg2.add(doc2)
    val idf = agg1.merge(agg2)

    val numDocs = 2
    val sumTf = 0.1D + 0.2D
    idf.of("hello") - math.log((numDocs + 1.0) / (sumTf + 1.0)) should be < 0.000001D
  }

  it should "correctly count document frequencies in merged aggregators" in {
    val agg1 = new Idf.DocumentFrequencyAggregator(2)
    val agg2 = new Idf.DocumentFrequencyAggregator(2)
    val doc1 = new TermFrequencies("doc1", List(("hello", 0.1D), ("world", 0.1D)))
    val doc2 = new TermFrequencies("doc2", List(("hello", 0.2D), ("lorenz", 0.2D)))

    agg1.add(doc1)
    agg2.add(doc2)

    agg1.merge(agg2).dfCms.estimateCount("hello") shouldBe 2
  }

  "Idf.DocumentFrequencyAggregator" should "be serializable" in {
    val baos = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(baos)
    oos.writeObject(new DocumentFrequencyAggregator(10))
    oos.flush()
    val data = baos.toByteArray
    oos.close


    baos.size() should be > 0

    val in = new ObjectInputStream(new ByteArrayInputStream(data))
    val deserializedObj = in.readObject().asInstanceOf[DocumentFrequencyAggregator]
    in.close
    deserializedObj.minDocFreq shouldBe 10
  }

  "Idf.calcIdf" should "should compute floating point values" in {
    // math.log((numDocs + 1.0) / (docFreq + 1.0))
    // math.log(5 / 3) =
    math.abs(0.5108256 - Idf.calcIdf(4, 2)) should be < 0.0001
  }

  "IDF" should "correctly process termfrequencies objects in a spark context" in spark() {

    val doc1 = new TermFrequencies("doc1", List(("word1", 1.0D), ("word2", 2.0D)))
    val doc2 = new TermFrequencies("doc2", List(("word1", 1.0D), ("word3", 3.0D)))
    val numDocs = 2 // no kiddin' eh?
    val docFreqWord1 = 2 // word1 appears in 2 documents

    val tf: RDD[TermFrequencies[_]] = sc.parallelize(List(doc1, doc2))
    val idf = new Idf(minDocFreq = 1).fit(tf)

    // this is the formula we use to compute IDF scores
    val expectedIdfValue = scala.math.log((numDocs + 1.0) / (docFreqWord1 + 1.0))

    idf.idf.of("word1") shouldBe expectedIdfValue

  }


}
