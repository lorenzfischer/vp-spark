package voxpopuli.textsummarizer

import org.scalatest.Matchers._
import org.scalatest._

/**
 * @author lorenz.fischer@gmail.com
 */
class TermFrequenciesSpec extends FlatSpec {

  "TermFrequencies.retainTop" should "only keep the top 1 term if requested" in {

    val doc = new TermFrequencies("doc1", List(
      ("hello", 0.1D),
      ("world", 0.3D),
      ("you", 0.35D),
      ("lorenz", 0.2D)))

    val topDocs = doc.retainTop(1)
    topDocs.termFrequencies.length shouldBe 1

    val topWord = topDocs.termFrequencies.iterator.next()
    topWord._1 shouldBe "you"
    math.abs(topWord._2 - 0.35D) should be < 0.000001D
  }

  it should "only keep the top 2 terms if requested" in {

    val doc = new TermFrequencies("doc1", List(
      ("hello", 0.1D),
      ("world", 0.3D),
      ("you", 0.35D),
      ("lorenz", 0.2D)))

    val topDocs = doc.retainTop(2)
    topDocs.termFrequencies.length shouldBe 2

    val iter = topDocs.termFrequencies.sortBy(-_._2).iterator

    val topWord = iter.next()
    topWord._1 shouldBe "you"
    math.abs(topWord._2 - 0.35D) should be < 0.000001D

    val alsoGood = iter.next()
    alsoGood._1 shouldBe "world"
    math.abs(alsoGood._2 - 0.3D) should be < 0.000001D
  }

  it should "only keep the top 10% if requested" in {

    val doc = new TermFrequencies("doc1", List(
      ("hello", 0.1D),
      ("world", 0.3D),
      ("you", 0.35D),
      ("lorenz", 0.2D)))

    val topDocs = doc.retainTopPercentage(0.1D) // this should only return the best of the four
    topDocs.termFrequencies.length shouldBe 1

    val topWord = topDocs.termFrequencies.iterator.next()
    topWord._1 shouldBe "you"
    math.abs(topWord._2 - 0.35D) should be < 0.000001D
  }

  it should "only keep the top 30% if requested" in {

    val doc = new TermFrequencies("doc1", List(
      ("hello", 0.1D),
      ("world", 0.3D),
      ("you", 0.35D),
      ("lorenz", 0.2D)))

    val topDocs = doc.retainTopPercentage(0.3D) // this should only return the best of the four
    topDocs.termFrequencies.length shouldBe 1

    val topWord = topDocs.termFrequencies.iterator.next()
    topWord._1 shouldBe "you"
    math.abs(topWord._2 - 0.35D) should be < 0.000001D
  }

  it should "only keep the top 60% if requested" in {

    val doc = new TermFrequencies("doc1", List(
      ("hello", 0.1D),
      ("world", 0.3D),
      ("you", 0.35D),
      ("lorenz", 0.2D)))

    val topDocs = doc.retainTopPercentage(0.6)
    topDocs.termFrequencies.length shouldBe 2

    val iter = topDocs.termFrequencies.sortBy(-_._2).iterator

    val topWord = iter.next()
    topWord._1 shouldBe "you"
    math.abs(topWord._2 - 0.35D) should be < 0.000001D

    val alsoGood = iter.next()
    alsoGood._1 shouldBe "world"
    math.abs(alsoGood._2 - 0.3D) should be < 0.000001D
  }

  it should "keep everything if retaining 100%" in {

    val doc = new TermFrequencies("doc1", List(
      ("hello", 0.1D),
      ("world", 0.3D),
      ("you", 0.35D),
      ("lorenz", 0.2D)))

    val topDocs = doc.retainTopPercentage(1.0)
    topDocs.termFrequencies.length shouldBe 4
  }

}
