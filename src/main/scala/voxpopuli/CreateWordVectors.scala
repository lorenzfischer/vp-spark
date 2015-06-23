package voxpopuli

/**
 * @author "Lorenz Fischer" <lfischer@ifi.uzh.ch>
 */

import java.io.{OutputStreamWriter, BufferedWriter, ObjectOutputStream, FileOutputStream}

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.compress.BZip2Codec
import org.apache.spark.mllib.feature.Word2Vec
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.clapper.argot.ArgotConverters._
import org.clapper.argot.ArgotParser

object CreateWordVectors {
  def main(args: Array[String]) {

    // this is from here: http://software.clapper.org/argot/
    val parser = new ArgotParser(this.getClass.getName)
    val inputParam = parser.parameter[String]("inputfile", "Files to read the text from.", true)
    val outputParam = parser.parameter[String]("outputfile", "Output file to which to.", true)
    parser.parse(args)

    val input = inputParam.value.getOrElse(
      "/user/lfischer/voxpopuli/german-books/*")
    val output = outputParam.value.getOrElse(
      "/user/lfischer/voxpopuli/german-books-counts")


    // Setup HDFS, you can manipulate the config used by your application to override the defaults
    val hadoopConf = new org.apache.hadoop.conf.Configuration()
    val hdfs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(input), hadoopConf)


    val conf = new SparkConf().setAppName(this.getClass.getName)
    val sc = new SparkContext(conf)

//    val inputFile = sc.textFile(input)
//    val words = inputFile.map{line =>
//      var text = line
//
//      text = text.replaceAll("\\p{Punct}+", " ") // remove all punctuation
//      /*  Replace all kinds of newlines:
//          A newline (line feed) character ('\n'),
//          A carriage-return character followed immediately by a newline character ("\r\n"),
//          A standalone carriage-return character ('\r'),
//          A next-line character ('\u0085'),
//          A line-separator character ('\u2028'), or
//          A paragraph-separator character ('\u2029).
//       */
//      text = text.replaceAll("[\\r\\n\\u0085\\u2028\\u2029]+", " ") // remove all punctuation
//      text = text.replaceAll("\\s+", " ") // replace double spaces with single spaces
//      text.split(" ").toSeq
//    }

    val words = sc.textFile("text8").map(line => line.split(" ").toSeq)

    val word2vec = new Word2Vec()

    val model = word2vec.fit(words)

    val fos = new FileOutputStream("vectors.serialized")
    val oos = new ObjectOutputStream(fos)

    oos.writeObject(model)
    oos.close

    // and now write the vectors into a file
    val writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("vectors.serialized")))
    writer.write(model.getVectors.toString())
    writer.write("\n")
    writer.flush()
    writer.close()


//    // Delete the existing path, ignore any exceptions thrown if the path doesn't exist
//    try { hdfs.delete(new org.apache.hadoop.fs.Path(output), true) } catch { case _ : Throwable => { } }
//
//    counts.saveAsTextFile(output, classOf[BZip2Codec])
  }
}
