package voxpopuli.util

import java.util.Date

import org.joda.time.DateTime
import org.json4s.DefaultFormats
import org.scalatest.Matchers._
import org.scalatest._
import voxpopuli.DataMuncher
import voxpopuli.textsummarizer.{CommentId, ArticleId}
import voxpopuli.util.JsonUtils._


/**
 * @author "Lorenz Fischer" <lfischer@ifi.uzh.ch>
 */
class JsonUtilsSpec extends FlatSpec {

  /** Helper function to convert Strings to date objects. */
  def date(s: String) = DefaultFormats.dateFormat.parse(s).get

  //    val jsonObj = parse(jsonLine)
  //    val test = (jsonObj \ "_id").extract[String]
  //    test should be ("_id-value")

  "JsonUtils" should "be able to read json lines" in {
    val json = """{"_id" : "_id-value", "date_scraped" : "2014-12-23T14:00:11Z", "date_published" : "2014-12-23T14:09:00Z", "type" : "type-value", "type_version" : 1, "title" : "title-value", "lead" : "lead-value", "text" : "text-value", "comments" : [ { "id" : "id-value1", "text" : "text-value1" },{ "id" : "id-value2", "text" : "text-value2" },{ "id" : "id-value3", "text" : "text-value3" }] }"""
    JsonUtils.parseArticle(json) should be(JsonUtils.Article("_id-value",
      Some(date("2014-12-23T14:00:11Z")),
      Some(date("2014-12-23T14:09:00Z")),
      Some("type-value"),
      Some(1),
      None,
      None,
      Some("title-value"),
      Some("lead-value"),
      Some("text-value"),
      Some(List(JsonUtils.Comment("id-value1", None, None, Some("text-value1"), None, None),
        JsonUtils.Comment("id-value2", None, None, Some("text-value2"), None, None),
        JsonUtils.Comment("id-value3", None, None, Some("text-value3"), None, None)))))
  }

  it should "be able to deal with lines without comments" in {
    val json = """{"_id" : "_id-value", "date_scraped" : "2014-12-23T14:00:11Z", "date_published" : "2014-12-23T14:09:00Z", "type" : "type-value", "type_version" : 1, "title" : "title-value", "lead" : "lead-value", "text" : "text-value", "comments" : null, "author" : null }"""
    JsonUtils.parseArticle(json) should be(JsonUtils.Article("_id-value",
      Some(date("2014-12-23T14:00:11Z")),
      Some(date("2014-12-23T14:09:00Z")),
      Some("type-value"),
      Some(1),
      None,
      None,
      Some("title-value"),
      Some("lead-value"),
      Some("text-value"),
      None))
  }

  it should "be able to deal with lines with no 'published date'" in {
    val json = """{"_id" : "_id-value", "date_scraped" : "2014-12-23T14:00:11Z", "date_published" : null, "type" : "type-value", "type_version" : 1, "title" : "title-value", "lead" : "lead-value", "text" : "text-value", "comments" : null, "author" : null }"""
    JsonUtils.parseArticle(json) should be(JsonUtils.Article("_id-value",
      Some(date("2014-12-23T14:00:11Z")),
      None,
      Some("type-value"),
      Some(1),
      None,
      None,
      Some("title-value"),
      Some("lead-value"),
      Some("text-value"),
      None))
  }

  it should "be able to deal with lines with no 'lead text'" in {
    val json = """{"_id" : "_id-value", "date_scraped" : "2014-12-23T14:00:11Z", "date_published" : null, "type" : "type-value", "type_version" : 1, "title" : "title-value", "lead" : null, "text" : "text-value", "comments" : null, "author" : null }"""
    JsonUtils.parseArticle(json) should be(JsonUtils.Article("_id-value",
      Some(date("2014-12-23T14:00:11Z")),
      None,
      Some("type-value"),
      Some(1),
      None,
      None,
      Some("title-value"),
      None,
      Some("text-value"),
      None))
  }

  it should "be able to deal with missing attributes" in {
    val json = """{"_id" : "_id-value" }"""
    JsonUtils.parseArticle(json) should be(JsonUtils.Article("_id-value",
      None,
      None,
      None,
      None,
      None,
      None,
      None,
      None,
      None,
      None))
  }

  it should "generate json rows from topicItem case classes" in {
    val topicItem = TopicItem(_id = "someUrl",
                              url = Some("someUrl"),
                              date_published = Some(new Date()),
                              authors = Some(List("Lorenz")),
                              tags = Some(List("tag1")),
                              topics = Some(List(TopicAssignment(1, 0.45), TopicAssignment(0, 0.33), TopicAssignment(77, 0.00001))),
                              terms = Some(List(Term(normalizedForm = "Loren", originalForms = List("Lorenz", "Lorenzz"), score=0.32))),
                              title=None,
                              lead=None,
                              opinions = Some(List(
                                            OpinionItem(id="somecomment",
                                                        date_published=null,
                                                        author="lorenz",
                                                        terms=List(Term("Hal", List("Hallo"), 0.44)),
                                                        votes_up = Some(2),
                                                        votes_down = Some(0)))),
                              None)

    val jsonTxt = toJsonString(topicItem)
    jsonTxt.size should be > 1
  }

  it should "format dates in iso format" in {
    val topicItem = TopicItem(_id = "someUrl",
      url = None,
      date_published = Some(new DateTime("2015-06-15T10:14:07Z").toDate),
      authors = None,
      tags = None,
      topics = None,
      terms = None,
      title=None,
      lead=None,
      opinions = None,
      buzzMeta = None)


    val jsonTxt = toJsonString(topicItem)
    //todo: there's something wonky with the utc offset
    """{"_id":"someUrl","date_published":"2015-06-15T10:14:07Z"}""" shouldBe jsonTxt
    //todo: test for ISODate() value .. this is what we actually need!
  }



}
