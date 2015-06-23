package voxpopuli.util

import java.text.SimpleDateFormat
import java.util.Date

import com.fasterxml.jackson.core.JsonParseException
import org.json4s.JsonAST.{JString, JField, JObject}
import org.json4s._
import org.json4s.jackson.JsonMethods._

/**
 * Utilities and case classes to serialize and deserialize json objects processed by the muncher pipeline.
 *
 * @author lorenz.fischer@gmail.com
 */
object JsonUtils {

  // todo: move this into common scala package, webapp is using these too
  case class Article(_id: String,
                     date_scraped: Option[Date],
                     date_published: Option[Date],
                     `type`: Option[String],
                     type_version: Option[Int],
                     authors: Option[Seq[String]],
                     tags: Option[Seq[String]],
                     title: Option[String],
                     lead: Option[String],
                     text: Option[String],
                     comments: Option[List[Comment]])

  case class Comment(id: String,
                     date_created: Option[Date],
                     author: Option[String],
                     text: Option[String],
                     votes_up: Option[Int],
                     votes_down: Option[Int])

  /** one topic. */
  case class Topic(id: Int,
                   label: Option[String],
                   topicTerms: Option[Seq[String]],
                   buzzMeta: Option[BuzzMetaInfo],
                   buzzMetaPerTag : Option[Map[String, BuzzMetaInfo]],
                   timeseries : Option[List[MetaInfo]])

  case class BuzzMetaInfo(buzzScore: Option[Double],
                           opinionCount: Option[Int],
                           opinionVotes: Option[Int],
                           opinionTerms: Option[Seq[String]])

  case class MetaInfo(date: Date,
                      opinionCount: Option[Int],
                      interactionCount: Option[Int])

  /** one 'article'. */
  case class TopicItem(_id: String,
                       url: Option[String],
                       date_published: Option[Date],
                       authors: Option[Seq[String]],
                       tags: Option[Seq[String]],
                       topics: Option[Seq[TopicAssignment]], // topicId, score
                       terms: Option[Seq[Term]],
                       title: Option[String],
                       lead: Option[String],
                       opinions: Option[Seq[OpinionItem]],
                       buzzMeta: Option[BuzzMetaInfo]
                        )

  /** one 'article'. */
  case class Tag(_id: String, score: Option[Int])

  /** Objects of this type are used to express that some doc (article or comment) has been assigned to a topic.
    * @param topicId the id of the topic the assignment is for.
    * @param score the score or weight expresses how strong the connection is.
    */
  case class TopicAssignment(topicId: Int, score: Double)

  /**
   * For each term, we store its normalized form and all the original forms it takes within the topic or opinion
   * item.
   * @param normalizedForm the normalized for of the word (eg. 'frank')
   * @param originalForms a list containing all original forms of the word in the text (eg. 'franken')
   * @param score the tfidf score, the higher the more important
   */
  case class Term(normalizedForm: String, originalForms: Seq[String], score: Double)

  /**
   * These are the processed and augmented comments. What TopicItems are to Articles, OpinionItems are to Comments.
   * @param id the id of the original comment, so we can link back.
   * @param date_published the date when the opinion was posted.
   * @param author the name of the person who posted the opinion.
   * @param terms the terms that represent the comment.
   * @param votes_up the number of up-votes the opinion received.
   * @param votes_down the number of down-votes the opinion received.
   */
  case class OpinionItem(id: String,
                         date_published: Date,
                         author: String,
                         terms: Seq[Term],
                         votes_up: Option[Int],
                         votes_down: Option[Int])



//  /**
//   * Custom date serializer, so mongodb can recognize our date values automatically.
//   *
//   * We create date strings that look like:
//   *
//   * ISODate("2012-07-14T01:00:00.000+02:00Z")
//   */
//  class IsoDateSerializer extends Serializer[Date] {
//    private val DateClass = classOf[Date]
//    val df = new SimpleDateFormat("'ISODate(\"'yyyy-MM-dd'T'HH:mm:ss.SSSZ'\")'");
//
//    def deserialize(implicit format: Formats) = {
//      case (TypeInfo(DateClass, _), json) => json match {
//        case JObject(List(JField("$dt", JString(s)))) =>
//          format.dateFormat.parse(s).getOrElse(throw new RuntimeException("Can't parse "+ s + " to Date"))
//        case x => throw new RuntimeException("Can't convert " + x + " to Date")
//      }
//    }
//
//    def serialize(implicit format: Formats) = {
//      case x: Date => JString(df.format(x))
//    }
//  }
// magical formats string...
//   implicit val formats = org.json4s.DefaultFormats + new IsoDateSerializer()

//  implicit val formats = new org.json4s.DefaultFormats {
//    override def dateFormatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
//  }

  implicit val formats = org.json4s.DefaultFormats

  /** Parses one line of json text that represents an article. */
  def parseArticle(jsonLine: String): Article = {
    try {
      parse(jsonLine).extract[Article]
    } catch {
      case jpe: JsonParseException => throw new RuntimeException(s"Error parsing line '$jsonLine'. ", jpe)
    }
  }

  /** Renders a case class as a compact (one line) json string. */
  def toJsonString[T](jsonObj: T): String = compact(Extraction.decompose(jsonObj))

}
