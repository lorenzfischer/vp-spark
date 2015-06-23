package voxpopuli

import org.apache.hadoop.io.compress.BZip2Codec
import org.apache.hadoop.streaming.StreamXmlRecordReader
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.{SparkConf, SparkContext}
import org.clapper.argot.ArgotParser
import scala.xml.XML
import org.clapper.argot.ArgotConverters._

/**
 * @author "Lorenz Fischer" <lfischer@ifi.uzh.ch>
 */
object ConvertWikiXml2Txt {

  def main(args: Array[String]) {

    // this is from here: http://software.clapper.org/argot/
    val parser = new ArgotParser("ConvertWikiXml2Txt")
    val inputParam = parser.parameter[String]("inputfile", "File to read the xml from.", true)
    val outputParam = parser.parameter[String]("outputfile", "Output file to which to write.", true)
    parser.parse(args)

    val input = inputParam.value.getOrElse(
      "/user/lfischer/voxpopuli/dewiki-latest-pages-articles.xml")
    val output = outputParam.value.getOrElse(
      "/user/lfischer/voxpopuli/dewiki-latest-pages-articles-txt")


    val conf = new SparkConf().setAppName("ConvertWikiXml2Txt")
    //conf.set("spark.executor.extraJavaOptions", "-Dfile.encoding=UTF-8")
    val sc = new SparkContext(conf)

    // read the xml using a hadoop job
    // this is from: http://apache-spark-user-list.1001560.n3.nabble.com/Parsing-a-large-XML-file-using-Spark-td19239.html
    val jobConf = new JobConf()
    jobConf.set("stream.recordreader.class", "org.apache.hadoop.streaming.StreamXmlRecordReader")
    jobConf.set("stream.recordreader.begin", "<page")
    jobConf.set("stream.recordreader.end", "</page>")
    org.apache.hadoop.mapred.FileInputFormat.addInputPaths(jobConf, input)

    // load documents (one per line).
    val documents = sc.hadoopRDD(jobConf, classOf[org.apache.hadoop.streaming.StreamInputFormat],
      classOf[org.apache.hadoop.io.Text],
      classOf[org.apache.hadoop.io.Text])

    // parse the contents of the xml code
    val texts = documents.map(_._1.toString)
      .map{ s =>
      val xml = XML.loadString(s)
      val id = (xml \ "id").text.toDouble
      val title = (xml \ "title").text
      var text = (xml \ "revision" \ "text").text

      // omit all redirects
      if (text.startsWith("#WEITERLEITUNG")
       || text.startsWith("#REDIRECT")) {
        text = ""
      }

      // omit all infoboxes
      if (text.startsWith("{{Infobox")
        || text.startsWith("{{Information")) {
        text = ""
      }


      // todo tokenize terms
      text = text.replaceAll("\\[\\[([^\\|]*?)\\]\\]", "$1") // [[term]] -> term
      text = text.replaceAll("\\[\\[[^\\|]*\\|(.*?)\\]\\]", "$1") // [[link|term]] -> term
      text = text.replaceAll("\\[[^\\|]*\\s(.*?)\\]", "$1") // [url term] -> term
      text = text.replaceAll("<\\!--.*?-->", "") // remove thinks like "<!--- Periodensystem --->"

      // todo: remove tables, references, tokenize names, {{Navigationsleiste Periodensystem}}, replace doppel-S (ß)

      text = text.replaceAll("[\\s]+", " ") // remove double spaces


      //val tknzed = text.split(" ").filter(_.size >= 2).toList  // we only return words of length >= 2
      //(id, title, tknzed )
      text
    }.filter(s => s.length > 0)

    texts.saveAsTextFile(output, classOf[BZip2Codec])

  }

}
