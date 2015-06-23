package voxpopuli

/**
 * @author "Lorenz Fischer" <lfischer@ifi.uzh.ch>
 */

import org.apache.hadoop.io.compress.BZip2Codec
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.clapper.argot.ArgotParser
import org.clapper.argot.ArgotConverters._

object DataWordCount {
  def main(args: Array[String]) {

    // this is from here: http://software.clapper.org/argot/
    val parser = new ArgotParser("DataWordCount")
    val inputParam = parser.parameter[String]("inputfile", "Files to read the xml from.", true)
    val outputParam = parser.parameter[String]("outputfile", "Output file to which to write.", true)
    parser.parse(args)

    val input = inputParam.value.getOrElse(
      "/user/lfischer/voxpopuli/german-books/*")
    val output = outputParam.value.getOrElse(
      "/user/lfischer/voxpopuli/german-books-counts")


    // Setup HDFS, you can manipulate the config used by your application to override the defaults
    val hadoopConf = new org.apache.hadoop.conf.Configuration()
    val hdfs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(input), hadoopConf)


    val conf = new SparkConf().setAppName("DataWordCount")
    val sc = new SparkContext(conf)

    val inputFile = sc.textFile(input)
    val counts = inputFile.flatMap{line =>
      var text = line

      text = text.replaceAll("\\p{Punct}+", " ") // remove all punctuation
      /*  Replace all kinds of newlines:
          A newline (line feed) character ('\n'),
          A carriage-return character followed immediately by a newline character ("\r\n"),
          A standalone carriage-return character ('\r'),
          A next-line character ('\u0085'),
          A line-separator character ('\u2028'), or
          A paragraph-separator character ('\u2029).
       */
      text = text.replaceAll("[\\r\\n\\u0085\\u2028\\u2029]+", " ") // remove all punctuation
      text = text.replaceAll("\\s+", " ") // replace double spaces with single spaces
      text.split(" ")
    }.map(word => (word, 1)).reduceByKey(_ + _)

    // Delete the existing path, ignore any exceptions thrown if the path doesn't exist
    try { hdfs.delete(new org.apache.hadoop.fs.Path(output), true) } catch { case _ : Throwable => { } }

    counts.saveAsTextFile(output, classOf[BZip2Codec])
  }
}
