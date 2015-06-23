package voxpopuli

/**
 * @author "Lorenz Fischer" <lfischer@ifi.uzh.ch>
 */


import java.util.{Calendar, Date}

import com.fasterxml.jackson.core.JsonParseException
import org.apache.commons.lang.time.DateUtils
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.io.compress.BZip2Codec
import org.apache.spark.{Partitioner, SparkContext, SparkConf}
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.clustering.{DistributedLDAModel, LDA}
import org.apache.spark.mllib.linalg.{Vectors, Vector, SparseVector}
import org.apache.spark.rdd.RDD
import org.clapper.argot.ArgotParser
import org.clapper.argot.ArgotConverters._
import org.joda.time.{Hours, DateTime}
import org.json4s.Extraction
import org.json4s.jackson.JsonMethods._
import voxpopuli.textsummarizer._
import voxpopuli.util.NlpUtils
import voxpopuli.util.JsonUtils._

import scala.collection

/**
 * This class does the 'data munching' by computing the TFIDF over all articles and their comments. It expects the input
 * file to contain one json-record per line.
 *
 * We generate these records using the query at 'mongo-queries/get-newest-records.js'
 *
 * Example record:
 *
 * {  "_id" : "http://www.nzz.ch/inter ... gen-pegida-werden-lauter-1.18450333",
 * "date_scraped" : ISODate("2014-12-26T20:03:04.336Z"),
 * "date_published" : ISODate("2014-12-23T14:09:00Z"),
 * "type" : "nzz.ch",
 * "type_version" : 1,
 * "title" : "Die Stimmen gegen «Pegida» werden lauter",
 * "lead" : "In Dresden hat die «Pegida»-Bewegung am ... sie Hass gegenüber Fremden.",
 * "text" : "Sie singen Weihnachtslieder. Gute alte de ... Ihr seid deren Schande», so heisst es.",
 * "comments" : [ { "id" : "1757575361",
 * "text" : "Immer weniger \"innerer\" ... Beruhigungsmittel." },
 * { "id" : "1758813826",
 * "text" : "Sie stellen sich damit ... Schwachsinn verlinken." },
 * { "id" : "1756225279",
 * "text" : "Die satirische Darst ... PIVrxPfZ_s?t=36m2s" },
 * { "id" : "1757478991",
 * "text" : "Korrektur: Eine Bewegung wie ... t sein Gesicht zu verlieren...." },
 * { "id" : "1756190724",
 * "text" : "\"Hass gegenüber  ... Quo vadis NZZ?" }
 * ] }
 */
object DataMuncher {

  // todo: read and write directly from and to mongo? http://codeforhire.com/2014/02/18/using-spark-with-mongodb/


  def main(args: Array[String]) {

    /* *********************** */
    /*  APPLICATION PARAMETERS */
    /* *********************** */

    // this is from here: http://software.clapper.org/argot/
    val parser = new ArgotParser("DataWordCount")
    val retainTop = parser.option[Float](List("retainTop"), "retainTop", "Percentage of terms to retain in output.")
    val oLdaIterations = parser.option[Int](List("li", "ldaIterations"), "ldaIterations", "Number of iterations we do in LDA.")
    val oNumTopics = parser.option[Int](List("lt", "ldaTopics"), "numTopics", "Number of topics to search using LDA.")
    val termWords = parser.option[Int](List("w", "termWords"), "termWords", "Maximum number of words per term.")
    val minDocFreqOpt = parser.option[Int](List("m", "minDocFreq"), "minDocFreq", "Minimum document frequency of a term.")
    val tasksOption = parser.option[Int](List("t", "tasks"), "tasks", "Number of tasks to run use.")
    val inputParam = parser.parameter[String]("inputfile", "Files to read the xml from.", true)
    val outputParam = parser.parameter[String]("outputfile", "Output directory to which to write.", true)
    parser.parse(args)

    val input = inputParam.value.getOrElse(
      "/user/lfischer/voxpopuli/db-dump.json")
    val output = outputParam.value.getOrElse(
      "/user/lfischer/voxpopuli/munch-output")



    /* ******* */
    /*  SETUP  */
    /* ******* */

    // Setup HDFS, you can manipulate the config used by your application to override the defaults
    val hadoopConf = new org.apache.hadoop.conf.Configuration()
    val hdfs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(input), hadoopConf)

    val conf = new SparkConf().setAppName("DataMuncher")
    conf.set("spark.driver.maxResultSize", "0")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    // trying to fight the  (No space left on device) mystery
    conf.set("spark.shuffle.consolidateFiles", "true")
    // conf.set("spark.cleaner.ttl", "1200") // keep old stages for at most 20 minutes before starting to delete them
    // Io could try this too: spark.rdd.compress true
    val sc = new SparkContext(conf)



    /* ********************* */
    /*  PROCESSING PIPELINE  */
    /* ********************* */

    val inputFile = sc.textFile(input)
    val numTasks = tasksOption.value.getOrElse(1)
    val numWordsInTerm = termWords.value.getOrElse(1)
    val minDocFreq = minDocFreqOpt.value.getOrElse(2)
    val retainTopPercentage = retainTop.value.getOrElse(0.05F)

    val ldaIterations = oLdaIterations.value.getOrElse(70)
    val ldaTopics = oNumTopics.value.getOrElse(50)

    val articles: RDD[Article] = inputFile.repartition(numTasks).map(parseArticle)
    articles.cache() // we will need these articles again at the end of the pipeline, so we cache them

    // create general purpose 'documents' to compute the tfidf scores for all articles, as well as the associated
    // comments within them. As each article hence, generates multiple 'documents' we use flatMap here
    val documents: RDD[Doc[_]] = articles.flatMap(createDocs)


    val (tf, stemmedToTermMap) = new SingleWordTf().createTfFromDoc(documents)
    tf.cache() // tf is cached in memory, because we access it frequently in the next three steps

    //org.apache.spark.mllib.feature.IDF
    val idf = new Idf(minDocFreq = minDocFreq).fit(tf)
    val tfidf: RDD[TermFrequencies[_]] = idf.transform(tf)
    val topNTfIdf: RDD[TermFrequencies[_]] = tfidf.map(tf => tf.retainTopPercentage(retainTopPercentage))

    val (articleCorpus, commentCorpus, docIdMap, commentIdMap, termIdMap) = createVocabulary(topNTfIdf)
    val corpus: RDD[(Long, Vector)] = articleCorpus
    corpus.cache() // cache since LDA is iterative


    /* *************************** */
    /* LDA Processing of documents */
    /* *************************** */
    // todo: optimize LDA using bayesian optimization
    val ldaModel = runLda(
      new LDA().setK(ldaTopics).setMaxIterations(ldaIterations).setTopicConcentration(-1).setDocConcentration(-1),
      corpus,
      termIdMap)

    //    /*
    //        **************************
    //        LDA Processing of comments
    //        **************************
    //
    //        The LDA processing of the comments needs to be done on a per article-topic basis, so we first need to find
    //        all topics a comment belongs to (over its parent article). then we can run the LDA.
    //     */
    //
    //    /* get the topics for each document: RDD[(documentId, topicDistribution)]
    //       topicDistribution vector_index = topicID [0..k] and value=score  */
    //    val topicDist: RDD[(Long, Vector)] = ldaModel.topicDistributions
    //    /* .. and convert this into an RDD containing all docIds for each topic */
    //    val topicIdByDocId: RDD[(Long, Int)] = topicDist.flatMap{ case (documentId, topicScores) =>
    //      // index = topicId, we only take topics with a score of more than 5%
    //      val scoresTopicIds = topicScores.toArray.zipWithIndex.filter(_._1 > 0.05)
    //      scoresTopicIds.map{ scoreTopicId => (documentId, scoreTopicId._2) }
    //    }
    //    /* now, join the docIdTopicId-RDD with the comments corpus by documentID before stipping of the docId. */
    //    val commentCorpiByTopicId = topicIdByDocId.join(commentCorpus).map(_._2)
    //
    //    /*
    //       spark doesn't support rdds of rdds and I also cannot split an rdd into multiple sub-rdds, so I do it manually
    //       here: We loop over all topic ids, filter the respective corpus and run LDA on it.
    //     */
    //    val ldaModelsPerTopic = (0 until numTopics).toSeq.map{ topicID =>
    //      val corpusForTopic = commentCorpiByTopicId.filter(_._1 == topicID).map(_._2)
    //      if (corpusForTopic.count() == 0) {
    //        None
    //      } else {
    //        Some(runLda(
    //          new LDA().setK(20).setMaxIterations(80).setTopicConcentration(-1).setDocConcentration(-1),
    //          corpusForTopic,
    //          termIdMap))
    //      }
    //    }

    postProcessing(sc, articles, tfidf, ldaModel, termIdMap, docIdMap, hdfs, output, stemmedToTermMap)

  }

  /**
   * Convenience method to run LDA over a corpus
   * @param lda the LDA configuration defining the number of topics, iterations, etc.
   * @param corpus the corpus over which the LDA should be run.
   * @param vocabMap a map that allows us to map back from the term ids to a string representation.
   * @param debug if true, time measurement and other debug info will be printed to the console
   */
  def runLda(lda: LDA,
             corpus: RDD[(Long, Vector)],
             vocabMap: scala.collection.Map[Long, String],
             debug: Boolean = true): DistributedLDAModel = {
    println("starting LDA process")

    val startTime = System.nanoTime()
    val ldaModel = lda.run(corpus) // run LDA
    val elapsed = (System.nanoTime() - startTime) / 1e9

    // also debug info, but only one line
    println(s"LDA run: k = ${lda.getK}, " +
      s"log likelihood: ${ldaModel.logLikelihood / corpus.count()}, " +
      s"time: $elapsed sec, " +
      s"training set:  ${corpus.count()} docs, " +
      s"Vocab-size: ${vocabMap.size} terms")

    if (debug) {

      //      println(s"Finished training LDA model.  Summary:")
      //      println(s"\t Training time: $elapsed sec")
      //      println(s"\t Training set size: ${corpus.count()} documents")
      //      println(s"\t Vocabulary size: ${vocabMap.size} terms")
      //      println(s"\t Training data average log likelihood: ${ldaModel.logLikelihood / corpus.count()}")

      // Print the topics, showing the top-weighted terms for each topic.
      val topicIndices: Array[(Array[Int], Array[Double])] = ldaModel.describeTopics(maxTermsPerTopic = 10)
      val tpcs = topicIndices.map { case (terms, termWeights) =>
        terms.zip(termWeights).map { case (term, weight) => (vocabMap(term), weight) }
      }
      println(s"topics:")
      tpcs.zipWithIndex.foreach { case (topic, i) =>
        println(s"TOPIC $i")
        topic.foreach { case (weight, term) =>
          println(s"$term\t$weight")
        }
        println()
      }

    }

    ldaModel
  }

  def storeInHdfs(path: String, data: RDD[_], hdfs: FileSystem) = {
    try {
      hdfs.delete(new org.apache.hadoop.fs.Path(path), true)
    } catch {
      case _: Throwable => {}
    }
    data.saveAsTextFile(path, classOf[BZip2Codec])
  }

  def createDocs(article: Article): Seq[Doc[_]] = {
    val text = List(article.title, article.lead, article.text).map(_.getOrElse("")).mkString(" ")
    var result = Vector[Doc[_]]()

    result = result :+ new Doc(ArticleId(article._id), text)
    result = result ++ article.comments.getOrElse(List.empty[Comment]).collect {
      case Comment(cId, _, _, Some(cText), _, _) => new Doc(CommentId(article._id, cId), cText)
    }
    result
  }


  /**
   * This method creates a list of terms based on the a map and the normalized term frequencies. The
   * goal of this process is to convert the list of normalized (stemmed) terms back into a list of words as they
   * appeared in the original text, preserving the stemmed form as well as the score.
   * @param stemmedToTermMap a map that maps from the stemmed form of a word back to its original form.
   * @param scoredTerms a sequence of normalizedTerm/score tuples
   * @return
   */
  def createTerms(stemmedToTermMap: collection.Map[String, String], scoredTerms: Seq[(String, Double)]): Seq[Term] = {
    scoredTerms.map { case Tuple2(normalizedTerm, score) =>
      val termList = scala.collection.mutable.MutableList[String]()
      termList += NlpUtils.deStemTerm(normalizedTerm, stemmedToTermMap)
      Term(normalizedTerm, termList, score)
    }
  }


  /**
   * This method creates the document (article) and comment corpi for the LDA, as well as a maps that allow to
   * map from the generated numeric document and comment ids back to textual id's as well as map to map back from
   * the dictionary encoded terms into regular string terms. This information can then be processed by the
   * Latent Dirichlet Allocation (LDA) algorithm.
   *
   * The comment corpus is nested by documentId, so it can be partitioned based on documentId. This is necessary
   * so that after I have the articleTopics through the first LDA iteration, I can run an additional LDA for each
   * topic to find the main types of comments (topics within the comments).
   *
   * @param tfidfScores all documents and comments and their tfidf scores
   * @return the document corpus RDD[(docId: Long, Vector[termId: Int, Count: Double])],
   *         the comment corpus RDD[(docId: Long, (commentId: Long, Vector[termId: Int, Count: Double]))],
   *         a map that maps from a (lda-) document id (Int) to the article id (String),
   *         a map that maps from a (lda-) comment id (Int) to the (original commment id)
   *         article_url-comment_author-comment_date (String),
   *         and a m that maps from a term id (Int) to the terms (String) in the vocabulary.
   */
  def createVocabulary(tfidfScores: RDD[TermFrequencies[_]]): (
    RDD[(Long, Vector)],
      RDD[(Long, (Long, Vector))],
      scala.collection.Map[Long, String],
      scala.collection.Map[Long, CommentId],
      scala.collection.Map[Long, String]) = {

    // we have to do this in three steps: first we build the vocabulary (distributed over multiple workers
    // we convert the values into a set and back into an array, to get rid of all duplicates
    var vocabMap =
      tfidfScores.flatMap(tf => tf.termFrequencies.map(termScoreTuple => termScoreTuple._1))
        .filter(NlpUtils.stopWordList.contains(_) == false)
        .zipWithUniqueId().collectAsMap()

    /* ****************************************
        Create Doc Corpus and the DocumentIdMap
        **************************************** */
    val tfidfArticles = tfidfScores.filter { tf => tf.docId match {
      case ArticleId(_) => true
      case _ => false
    }
    }
    // second: the lda algorithm works with docIds, so we need to be able to map back to these ids
    val docMap = tfidfArticles.map(_.docId match { case ArticleId(articleId) => articleId }).
      zipWithUniqueId().collectAsMap()

    // todo: for 40k docs with 80 bytes for the url, this is about 3MB, so no problem for now...
    val bcDocMap = tfidfArticles.context.broadcast(docMap) // broadcast docMap, so we can access it from on all machines
    // todo: check how big the vocabMap is
    val bcVocabMap = tfidfArticles.context.broadcast(vocabMap)

    val docCorpus = tfidfArticles.map(tf => tf.docId match {
      case ArticleId(articleId) =>
        val thisDocMap = bcDocMap.value // get broadcasted docMap
      val thisVocabMap = bcVocabMap.value
        // Filter tokens by vocabulary, and create word count vector representation of document.
        // Map[termIndex, count]
        val wordCounts = new scala.collection.mutable.HashMap[Int, Int]()
        tf.termFrequencies.foreach { termScore =>
          if (thisVocabMap.contains(termScore._1)) {
            val termIndex = thisVocabMap(termScore._1).toInt
            wordCounts(termIndex) = wordCounts.getOrElse(termIndex, 0) + 1
          }
        }
        val indices = wordCounts.keys.toArray.sorted
        val values = indices.map(i => wordCounts(i).toDouble)

        val sb = Vectors.sparse(thisVocabMap.size, indices, values)
        (thisDocMap(articleId), sb)
    })


    /* ******************************************
       Create Comment Corpus and the CommentIdMap
       ****************************************** */
    val tfidfComments = tfidfScores.filter { tf => tf.docId match {
      case CommentId(_, _) => true
      case _ => false
    }
    }
    val commentMap: collection.Map[CommentId, Long] =
      tfidfComments.map(_.docId match { case commentId: CommentId => commentId }).
        zipWithUniqueId().collectAsMap()

    val bcCommentMap = tfidfComments.context.broadcast(commentMap) // todo: how big is this?
    val bcDocMap2 = tfidfComments.context.broadcast(docMap)
    val commentCorpus = tfidfComments.map(tf => tf.docId match {
      case commentId: CommentId =>
        val bcDocMap2 = bcDocMap.value // get broadcasted docMap
      val thisCommentMap = bcCommentMap.value // get broadcasted docMap
      // Filter tokens by vocabulary, and create word count vector representation of the comment.
      // Map[termIndex, count]
      val wordCounts = new scala.collection.mutable.HashMap[Int, Int]()
        tf.termFrequencies.foreach { termScore =>
          if (vocabMap.contains(termScore._1)) {
            val termIndex = vocabMap(termScore._1).toInt
            wordCounts(termIndex) = wordCounts.getOrElse(termIndex, 0) + 1
          }
        }
        val indices = wordCounts.keys.toArray.sorted
        val values = indices.map(i => wordCounts(i).toDouble)

        val sb = Vectors.sparse(vocabMap.size, indices, values)
        (bcDocMap2(commentId.articleId), (thisCommentMap(commentId), sb))
    })


    (docCorpus, commentCorpus, docMap.map {
      _.swap
    }, commentMap.map {
      _.swap
    }, vocabMap.map {
      _.swap
    })
  }


  /**
   * In this part of the pipeline we merge all the components and generate topics and topic items in hdfs
   *
   * @param sc the spark context we are in.
   * @param articles the articles we used to compute the tfidf and lda models.
   * @param tfidf the tfidf values for the articles.
   * @param ldaModel the lda model containing the topics and the document-topic assignments.
   * @param vocabMap a map to convert from termIds (used in LDA) back to regular strings.
   * @param docMap a map to convert from docIds (used in LDA) back to regular strings.
   * @param hdfs a reference to the HDFS file system to store the files.
   * @param outputPath the path we store the result of the computation in hdfs.
   * @param stemmedToTermMap a map to map from the stemmed version of a word back to its original representation
   */
  def postProcessing(sc: SparkContext,
                     articles: RDD[Article],
                     tfidf: RDD[TermFrequencies[_]],
                     ldaModel: DistributedLDAModel,
                     vocabMap: scala.collection.Map[Long, String],
                     docMap: scala.collection.Map[Long, String],
                     hdfs: org.apache.hadoop.fs.FileSystem,
                     outputPath: String,
                     stemmedToTermMap: collection.Map[String, String]) = {

    // Print the topics, showing the top-weighted terms for each topic.
    val topicIndices = ldaModel.describeTopics(maxTermsPerTopic = 10)
    val tpcs = topicIndices.map { case (terms, termWeights) =>
      terms.zip(termWeights).map { case (term, weight) => (vocabMap(term), weight) }
    }

    //    println(s"100 topics:")
    //    tpcs.zipWithIndex.foreach { case (topic, i) =>
    //      println(s"TOPIC $i")
    //      topic.foreach { case (weight, term) =>
    //        println(s"$term\t$weight")
    //      }
    //      println()
    //    }

    /*
       get the topics for each document: RDD[(documentId, topicDistribution)]
       topicDistribution vector_index = topicID [0..k] and value=score
      */
    val topicDist: RDD[(Long, Vector)] = ldaModel.topicDistributions
    val bcDocMap = topicDist.context.broadcast(docMap)
    val topicDistByUrl: RDD[(String, Vector)] = topicDist.map { ldaDocId_topicDist =>
      val thisDocMap = bcDocMap.value
      (thisDocMap(ldaDocId_topicDist._1), ldaDocId_topicDist._2)
    }

    /* ************* */
    /* Merge Results */
    /* ************* */

    /*
          todo: should I move the retaining somewhere else?

          I had this line here:

            val top10PercentTfidf = tfidf.map( tf => tf.retainTopPercentage(0.1))

          what was the reasoning. I will use the tfidf as it is passed in from the processing part, and decrease
          the number of words there.
      */
    //val top10PercentTfidf = tfidf
    val top10PercentTfidf = tfidf.map(tf => tf.retainTopPercentage(0.1))

    // join the articles with their comments by ...
    // ... first grouping all comments by articleId
    val commentsByArticleId = top10PercentTfidf.filter(tf => tf.docId match {
      case CommentId(_, _) => true
      case _ => false
    }).map(tf => tf.docId match {
      case CommentId(articleId, commentId) => (articleId, (commentId, tf.termFrequencies))
    }).groupByKey

    // ... then unwrapping the articleId from all articles
    val articlesByArticleId = top10PercentTfidf.filter { tf => tf.docId match {
      case ArticleId(_) => true
      case _ => false
    }
    }.map(tf => tf.docId match {
      case ArticleId(articleId) => (articleId, tf.termFrequencies)
    })

    // ... and by then joining the articles and their comments (articleId = url)
    val summarizedArticlesByUrl = articlesByArticleId.join(commentsByArticleId)


    // join the original articles with the generated summaries
    val articlesByUrl = articles.map(article => (article._id, article))
    val articlesAndSummaryByUrl = articlesByUrl.join(summarizedArticlesByUrl)

    // join the articles with the topic distribution obtained from the LDA
    val articlesAndSummaryAndTdistByUrl = articlesAndSummaryByUrl.join(topicDistByUrl)

    // generate topic items
    val topicItems: RDD[TopicItem] = articlesAndSummaryAndTdistByUrl.map { urlArticleAndSummaryAndTdist =>

      /*
        Here we combine the summarization of the article and its comments back into a structured representation.
        This happens in 3 steps:
         1. build a map with all comments for easy lookup
         2. build a list of 'opinionItems' using the constructed map
         3. build the topicItem using the opinionItems
       */
      val articleAndSummaryAndTdist = urlArticleAndSummaryAndTdist._2
      val articleAndSummary = articleAndSummaryAndTdist._1
      val topicDist = articleAndSummaryAndTdist._2
      val article = articleAndSummary._1
      val summary = articleAndSummary._2
      val articleTerms = summary._1
      val comments = summary._2 //  (articleId, Seq[(commentId, tf.termFrequencies))]

      // build a lookup table for the comments
      val commentId2CommentTuples = article.comments.map { commentList => commentList.map(c => (c.id, c)) }
      val commentId2CommentMap: Map[String, Comment] = commentId2CommentTuples.getOrElse(Map.empty[String, Comment]).toMap

      // build a commentIdCommentMap of all comments
      val commentId2commentMap = article.comments.getOrElse(List()).map { comment => (comment.id, comment) }.toMap

      val opinionItems: Seq[OpinionItem] = comments.flatMap { case Tuple2(commentId, terms) =>
        commentId2commentMap(commentId).text match {
          case Some(commentText) => {
            val comment = commentId2CommentMap(commentId)
            Some(OpinionItem(commentId,
              comment.date_created.getOrElse(null),
              comment.author.getOrElse(null),
              createTerms(stemmedToTermMap, terms),
              comment.votes_up,
              comment.votes_down))
          }
          case _ => None
        }
      }.toSeq
      val buzzMeta = computeBuzzMeta(opinionItems)

      val topicArray = topicDist.toArray
      // Each document has a score for each topic. For debugging purposes we keep the top 10 topics.
      // todo: do I need to take all of them here?
      val topics = (0 until topicArray.size zip topicArray)
        .map(ts => TopicAssignment(topicId = ts._1, score = ts._2)).sortBy(-_.score).take(10).toSeq
      TopicItem(_id = article._id, // id
        url = Some(article._id), // url
        date_published = article.date_published,
        authors = article.authors,
        tags = article.tags,
        topics = Some(topics),
        terms = Some(createTerms(stemmedToTermMap, articleTerms)),
        title = article.title,
        lead = article.lead,
        opinions = Some(opinionItems),
        buzzMeta = Some(buzzMeta))
    }

    /*
      I did some manual evaluations [1] and it seemed to me that topics with a score of less than 0.05 did not
      result in good topic assignments. Scores of 33 and more resulted in perfect topic matches. For this reason,
      and until I have implemented a better way of doing the clustering (hierarchical LDA?), I'm just gonna
      ignore all topic assignments that have a score of less than 5% when computing the buzzScores and opinion counts.

      [1] https://docs.google.com/document/d/1kbMBW6WdnoA-WgUNe9TomZ5S-KC7tZqmXSJ2TVxE5GQ/edit
    */
    // todo: we could send the topic score along with the OpinionItem, maybe this could help us inform the importance of the opinion?
    val topicItemsByTopicId: RDD[(Int, TopicItem)] = topicItems.flatMap { topicItem =>
      topicItem.topics.getOrElse(List()).filter(_.score > 0.05).map { topicAssignment =>
        (topicAssignment.topicId, topicItem)
      }
    }



    /* ****************************************************************************************** */
    /* Here, we count the number of opinions and interactions (up/down votes) a topic had per day */
    /* ****************************************************************************************** */
    val topicTimeseriesByTopicId: RDD[(Int, (Date, Int, Int))] = countOpinionsAndIteractions(topicItemsByTopicId)


    /* ************ */
    /* Extract tags */
    /* ************ */

    val tagsSorted = topicItems.flatMap { topicItem =>
      topicItem.tags.getOrElse(List()).map { tag =>
        (NlpUtils.normalize(tag), 1)
      }
    }.reduceByKey(_ + _).sortBy(_._2, ascending = false).take(100)

    val topicsTermsByTopicId: RDD[(Int, (String, Double))] = sc.parallelize(tpcs.zipWithIndex.map(_.swap)).flatMap {
      case (topicId, termsAndScores) =>
        termsAndScores.map {
          (topicId, _)
        }
    }



    /* *************************************************************************************************** */
    /* Group all data (terms, topicItems, and timeseries information by topicID, and produce Topic objects */
    /* *************************************************************************************************** */

    val termsItemsTimeSeriesByTopicId = topicsTermsByTopicId.groupWith(topicItemsByTopicId, topicTimeseriesByTopicId)
    val bcTagsSorted = termsItemsTimeSeriesByTopicId.context.broadcast(tagsSorted)
    val topics = termsItemsTimeSeriesByTopicId.map { case (topicId, (termsWithScore, items, timeseries)) =>
      val thisTagsSorted = bcTagsSorted.value
      val top10TopicTerms = termsWithScore.toList.sortBy(_._1).map { case (term, weight) => term }.take(10)

      // todo: don't re-compute the buzz score, but use the precomputed one in the topicItem. For this I need
      // to preserve all the opinion terms, though. At the moment I lose them because the computeBuzz method
      // already drops the lower 95% terms.

      val opinions: Iterable[OpinionItem] = items.flatMap(_.opinions.getOrElse(List()))
      val metaInfo = computeBuzzMeta(opinions)

      val buzzMetaPerTag = thisTagsSorted.map { tagScore =>
        val tag = tagScore._1
        val opinionsWithTag: Iterable[OpinionItem] = items.filter(_.tags.getOrElse(List()).contains(tag))
          .flatMap(_.opinions.getOrElse(List()))
        (tag, computeBuzzMeta(opinionsWithTag))
      }.toMap


      val timeseriesMetaInfo = timeseries.map( i => MetaInfo(i._1, Some(i._2), Some(i._3))).toList

      Topic(id = topicId,
        label = None,
        topicTerms = Some(top10TopicTerms.map(NlpUtils.deStemTerm(_, stemmedToTermMap))),
        buzzMeta = Some(metaInfo),
        buzzMetaPerTag = Some(buzzMetaPerTag),
        timeseries = Some(timeseriesMetaInfo)
      )
    }

    val tagsAsObjects = tagsSorted.map { tagScore =>
      val tag = tagScore._1
      val score = tagScore._2
      Tag(tag, Some(score))
    }

    storeInHdfs(s"$outputPath/topicItems", topicItems.map(toJsonString), hdfs)
    storeInHdfs(s"$outputPath/topics", topics.map(toJsonString), hdfs)
    storeInHdfs(s"$outputPath/tags", sc.parallelize(tagsAsObjects.map(toJsonString)), hdfs)

  }

  /**
   * The steps to compute the buzz are:
   * 1. for each document, read out the topics with a score of > 5%
   * 2. group all documents by all topics (each document can belong to n topics)
   * 3. for each topic, count the number of comments, number of votes, and compute a buzzScore which takes
   * the temporal recency of comments into account.
   *
   * BUZZ-SCORE
   *
   * We try to capture an answer to the following question in our buzz-score:
   *
   * How many comments did people write for this topic and how recent are these comments.
   *
   * This number needs to be computed using all of the above values (comments, dates, votes)
   *
   * For now we use buzzScore = sum(ln(upVotes+e) / ln(downVotes+e) * max(0, log(30 / age)))
   * where age is the number of days (floating point value) that have passed since the comment has been
   * published. This discounts the importance of comments over the course of 30 days logarithmically.
   *
   * @param opinions all opinion items assigned to a topic
   * @return the buzzScore, the opinionCount, and the count of all votes given to opinions for the topic,
   *         given all the associated opinions.
   */
  def computeBuzzMeta(opinions: Iterable[OpinionItem]): BuzzMetaInfo = {
    // todo: count opinions by the same author only once
    val opinionCount = opinions.toSeq.length // waste of resources.. is there no other way?

    // todo: do tfidf to compute top 100 terms, not just simple document count
    val top10OpinionTerms = opinions.flatMap {
      _.terms
    }.groupBy(_.normalizedForm).toSeq.map { terms =>
      val occurrenceCount = terms._2.toSeq.length // wasteful! counts in how many comments this term appeared
    val mostPopularFormUsed = terms._2.flatMap {
        _.originalForms
      }.foldLeft(Map.empty[String, Int]) {
        (count, word) => count + (word -> (count.getOrElse(word, 0) + 1))
      }.toSeq.sortBy(-_._2).head._1
      (mostPopularFormUsed, occurrenceCount)
    }.sortBy(-_._2).take(10).map(_._1)

    // todo: don't loop over the same stuff 3 times.. maybe use tuple3s?

    val voteCount = opinions.map { opinion =>
      opinion.votes_up.getOrElse(0) + opinion.votes_down.getOrElse(0)
    }.foldLeft(0) { (a, b) => a + b }

    // todo: tired note: probably, I should compute the buzz for each topic Item so that i only have to sum up here
    val buzzScore = opinions.filter {
      _.date_published != null
    }.map(computeBuzzValue).foldLeft(0.0D) { (a, b) => a + b }

    BuzzMetaInfo(Some(buzzScore),
      Some(opinionCount),
      Some(voteCount),
      Some(top10OpinionTerms))
  }

  /** Computes the buzz value for one single opinion item.
    *
    * @param opinionItem the item we should compute the buzz value for.
    * @return the buzz value the opinion item.
    */
  def computeBuzzValue(opinionItem: OpinionItem): Double = {
    // we want days as floating point values
    val age = 1.0D * Hours.hoursBetween(new DateTime(opinionItem.date_published), DateTime.now).getHours / 24

    // we multiply each opinion by it's up-vote-count and divide by its down-vote-count. In order to prevent too
    // much impact of votes, we take the logarithm over twice the counts, adding 1,
    val votesScore = Math.log(Math.E + opinionItem.votes_up.getOrElse(0)) / Math.log(Math.E + opinionItem.votes_down.getOrElse(0))
    val ageScore = Math.max(0.0D, Math.log(30 / age)) // + 0.1D // 0 for ages of more than 30 days

    votesScore * ageScore
  }

  /**
   * This method counts all opinions (comments) and interactions (up/down votes on opinions) and returns an RDD
   * having the topicId as the key and triples of (Date, OpinionCount, InteractionCount) as values.
   *
   * @param topicItemsByTopicId a list of TopicItems (documents/webpages/articles) that have OpinionItems attached
   *                            to them.
   * @return an RDD containig the timeseries.
   */
  def countOpinionsAndIteractions(topicItemsByTopicId: RDD[(Int, TopicItem)]): RDD[(Int, (Date, Int, Int))] = {
    val topicItemsByTopicIdAndDate: RDD[((Int, Date), TopicItem)] = topicItemsByTopicId.map{ topicAndItems =>
      val topicId = topicAndItems._1
      val topicItem = topicAndItems._2

      // create new key, consisting of the topicId and the date (without time) value of the topicItem
      val newKey = topicItem.date_published match {
        case Some(date_published) => ( topicId, DateUtils.truncate(date_published, Calendar.DATE) )
        case None => null
      }

      (newKey, topicItem)
    }.filter( keyItem => keyItem._1 != null ) // todo: I should do this with some, none and map

    // a function that reads the number of opinion and the number of interactions (up/down votes) for a topicItem
    // and adds these values to the counts passed in as the first part of the tuple
    // todo: consider using longs instead of ints. for now I should be safe, as it's only swiss media
    val seqOp: ( (Int, Int), TopicItem ) => (Int, Int) = (counts, topicItem) => {
      val (opinionCount, interactionCount) = counts
      val iteractions = topicItem.opinions match {
        case Some(opinionSeq) => opinionSeq.map{ opinionItem =>
          opinionItem.votes_up.getOrElse(0) + opinionItem.votes_down.getOrElse(0) }
        case _ => scala.List[Int]()
      }

      val opinions = topicItem.opinions match {
        case Some(seq) => seq.size
        case None => 0
      }

      (opinionCount + opinions, interactionCount + iteractions.sum)
    }
    // a function that combines opinion and interactions counts (oic)
    val combOp: ( (Int, Int), (Int, Int) ) => (Int, Int) = (oic1, oic2) => {
      (oic1._1 + oic2._1, oic1._2 + oic2._2)
    }
    val topicTimeseriesByTopicId: RDD[(Int, (Date, Int, Int))] =
      topicItemsByTopicIdAndDate.aggregateByKey( (0,0) )(seqOp, combOp).map{ i =>
        // move the date value over to the counts, and use the topicId as the sole key
        val (topicId, day) = i._1
        val (opinionCount, interactionCount) = i._2
        (topicId, (day, opinionCount, interactionCount))
      }

    topicTimeseriesByTopicId
  }



}

//    val terms = documents.flatMap{ document =>
//       NlpUtils.computeTerms(document)
//    }
//    val tf = terms.map( term => (term, 1.0)).reduceByKey( (_,_) => )

//    val tf: RDD[(_, Double)] = new SingleWordTF().transform(documents)


//    val documents: RDD[Seq[String]] = inputFile.map { line =>
//      val article = parseArticle(line)
//      List(article.title.getOrElse(""), article.lead.getOrElse(""), article.text.getOrElse("")).mkString(" ").split(" ")
//    }
//    // compute term frequencies (TF)
//    val hashingTF = new HashingTF()
//    val tf: RDD[Vector] = hashingTF.transform(documents)
//    // comput idf and tfidf
//    tf.cache()
//    val idf = new IDF(minDocFreq = 2).fit(tf)
//    val tfidf: RDD[Vector] = idf.transform(tf)