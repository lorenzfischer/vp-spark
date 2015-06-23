package voxpopuli


import java.util.Date

import org.apache.spark.rdd.RDD
import org.joda.time.{Years, MonthDay, DateTime, Days}
import org.scalatest._
import voxpopuli.testutil.SparkUtils
import voxpopuli.textsummarizer._
import voxpopuli.util.JsonUtils
import voxpopuli.util.JsonUtils._

class DataMuncherTestSpec extends FlatSpec with Matchers with SparkUtils {

  "The DataMuncher" should "be able to create documents for a a json line with comments" in {
    val json = """{"_id" : "_id-value", "date_scraped" : "2014-12-23T14:00:11Z", "date_published" : "2014-12-23T14:09:00Z", "type" : "type-value", "type_version" : 1, "title" : "title-value", "lead" : "lead-value", "text" : "text-value", "comments" : [ { "id" : "id-value1", "text" : "text-value1" },{ "id" : "id-value2", "text" : "text-value2" },{ "id" : "id-value3", "text" : "text-value3" }] }"""
    val docs = DataMuncher.createDocs(JsonUtils.parseArticle(json))

    docs.size shouldBe 4 // 1 main article and 3 comments

    docs(0).docId shouldBe ArticleId("_id-value")
    docs(0).text shouldBe "title-value lead-value text-value"

    docs(1).docId shouldBe CommentId("_id-value", "id-value1")
    docs(1).text shouldBe "text-value1"

    docs(2).docId shouldBe CommentId("_id-value", "id-value2")
    docs(2).text shouldBe "text-value2"

    docs(3).docId shouldBe CommentId("_id-value", "id-value3")
    docs(3).text shouldBe "text-value3"

  }

  it should "be able to create documents for a a json line with comments that have no text" in {
    val json = """{"_id" : "_id-value", "date_scraped" : "2014-12-23T14:00:11Z", "date_published" : "2014-12-23T14:09:00Z", "type" : "type-value", "type_version" : 1, "title" : "title-value", "lead" : "lead-value", "text" : "text-value", "comments" : [ { "id" : "id-value1", "text" : null } ] }"""
    val docs = DataMuncher.createDocs(JsonUtils.parseArticle(json))

    docs.size shouldBe 1 // 1 main article and 3 comments

    docs(0).docId shouldBe ArticleId("_id-value")
    docs(0).text shouldBe "title-value lead-value text-value"

  }

  it should "be able to create documents for a a json line with no comments" in {
    val json = """{"_id" : "_id-value", "date_scraped" : "2014-12-23T14:00:11Z", "date_published" : "2014-12-23T14:09:00Z", "type" : "type-value", "type_version" : 1, "title" : "title-value", "lead" : "lead-value", "text" : "text-value", "comments" : null }"""
    val docs = DataMuncher.createDocs(JsonUtils.parseArticle(json))

    docs.size shouldBe 1 // only 1 article

    docs(0).docId shouldBe ArticleId("_id-value")
    docs(0).text shouldBe "title-value lead-value text-value"

  }

  // todo: reactivate this test
  //  ignore should "convert normalized terms and their tfidf scores back into their original form" in {
  //    //    NlpUtils.normalizeTerm("Lustigen") should be("lustig")
  //    //    NlpUtils.normalizeTerm("äffchen") should be("affch")
  //    val terms = DataMuncher.createTerms("Lustige Äffchen!", List(("lustig", 1.00)))
  //    terms.length shouldBe 1
  //    terms.last.originalForms.last shouldBe "Lustige"
  //  }

  it should "compute buzz values for opinion items" in {
    val oneDayAgo = DateTime.now.minus(Days.days(1)).toDate
    val terms = List(Term("Hal", List("Hallo"), 0.44))

    var bv = DataMuncher.computeBuzzValue(OpinionItem(id = "someComment", oneDayAgo, "author", terms, Some(1), None))
    bv should be > 4.0D
    bv should be < 5.0D

    bv = DataMuncher.computeBuzzValue(OpinionItem(id = "someComment", oneDayAgo, "author", terms, None, None))
    bv should be > 3.0D
    bv should be < 4.0D


    bv = DataMuncher.computeBuzzValue(OpinionItem(id = "someComment", oneDayAgo, "author", terms, Some(1981298120), None))
    bv should be > 3.0D
  }

  it should "compute buzz scores for lists of opinion items" in {
    val oneDayAgo = DateTime.now.minus(Days.days(1)).toDate
    val oneMonth = DateTime.now.minus(Days.days(31)).toDate
    val oneYearAgo = DateTime.now.minus(Years.years(1)).toDate
    val terms = List(Term("Hal", List("Hallo"), 0.44))
    val opinions = List(OpinionItem(id = "someComment1", oneDayAgo, "author1", terms, Some(4000), Some(100)),
      OpinionItem(id = "someComment2", oneMonth, "author2", terms, None, None),
      OpinionItem(id = "someComment3", oneYearAgo, "author3", terms, Some(30), Some(130)),
      OpinionItem(id = "evilComment", null, "author3", terms, None, None))
    var bv = DataMuncher.computeBuzzMeta(opinions)
    bv.buzzScore.getOrElse(30.0) should be < 20.0D
  }

  "DataMuncher.countOpinionsAndIteractions" should "compute the opinion counts and interaction counts" in spark() {

    val topicItem1 = TopicItem(_id = "ti1",
      url = None,
      date_published = Some(new Date()),
      authors = None,
      tags = None,
      topics = None,
      terms = None,
      title = None,
      lead = None,
      opinions = Some(List(
        OpinionItem(id = "ti1Op1", date_published = new Date(), author = "auth1", terms = List[Term](), votes_up = Some(1), votes_down = Some(1)),
        OpinionItem(id = "ti1Op2", date_published = new Date(), author = "auth2", terms = List[Term](), votes_up = Some(1), votes_down = Some(1))
      )),
      buzzMeta = None
    )

    val inputData: RDD[(Int, TopicItem)] = sc.parallelize(List((0, topicItem1)))
    val counts = DataMuncher.countOpinionsAndIteractions(inputData)

    val collectedCounts = counts.collectAsMap()

    collectedCounts.size shouldBe 1
    collectedCounts.get(0).get._2 shouldBe 2 // we have two opinions
    collectedCounts.get(0).get._3 shouldBe 4 // we have four votes
  }

  it should "ignore items without date values for the computation of opinion and interaction counts" in spark() {

    val topicItem1 = TopicItem(_id = "ti1",
      url = None,
      date_published = None,
      authors = None,
      tags = None,
      topics = None,
      terms = None,
      title = None,
      lead = None,
      opinions = Some(List(
        OpinionItem(id = "ti1Op1", date_published = new Date(), author = "auth1", terms = List[Term](), votes_up = Some(1), votes_down = Some(1)),
        OpinionItem(id = "ti1Op2", date_published = new Date(), author = "auth2", terms = List[Term](), votes_up = Some(1), votes_down = Some(1))
      )),
      buzzMeta = None
    )

    val inputData: RDD[(Int, TopicItem)] = sc.parallelize(List((0, topicItem1)))
    val counts = DataMuncher.countOpinionsAndIteractions(inputData)

    val collectedCounts = counts.collectAsMap()

    collectedCounts.size shouldBe 0
  }

}