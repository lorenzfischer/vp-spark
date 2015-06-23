package voxpopuli.testutil

import org.apache.log4j.{Logger, Level}
import org.apache.spark.SparkContext

/**
 * Copied and adapted from: http://blog.quantifind.com/posts/spark-unit-test/
 *
 * @author lorenz.fischer@gmail.com
 */
trait SparkUtils {

  var sc: SparkContext = _

  /**
   * convenience method for tests that use spark.  Creates a local spark context, and cleans
   * it up even if your test fails.  Also marks the test with the tag SparkTest, so you can
   * turn it off
   *
   * By default, it turn off spark logging, b/c it just clutters up the test output.  However,
   * when you are actively debugging one test, you may want to turn the logs on
   *
   * @param silenceSpark true to turn off spark logging
   */
  def spark(silenceSpark: Boolean = true)(body: => Unit) {
    val origLogLevels = if (silenceSpark) doSilenceSpark() else null
    sc = new SparkContext("local[4]", "sparkTest")
    try {
      body
    }
    finally {
      sc.stop
      sc = null
      // To avoid Akka rebinding to the same port, since it doesn't unbind immediately on shutdown
      System.clearProperty("spark.master.port")
      // the hostPost is a suggestion from here:
      // http://spark-summit.org/wp-content/uploads/2014/06/Testing-Spark-Best-Practices-Anupama-Shetty-Neil-Marshall.pdf
      System.clearProperty("spark.hostPort")
      //if (silenceSpark) Logging.setLogLevels(origLogLevels)
    }
  }

  def doSilenceSpark() = {
    setLogLevels(Level.WARN, Seq("spark", "org.eclipse.jetty", "akka"))
  }

  def setLogLevels(level: org.apache.log4j.Level, loggers: TraversableOnce[String]) = {
    loggers.map {
      loggerName =>
        val logger = Logger.getLogger(loggerName)
        val prevLevel = logger.getLevel()
        logger.setLevel(level)
        loggerName -> prevLevel
    }.toMap
  }
}
