package voxpopuli.util

import java.io.{BufferedReader, FileInputStream, BufferedInputStream}
import java.util.zip.GZIPInputStream

import maui.stemmers.GermanStemmer
import scala.io.Source
import scala.util.control.Breaks._
import scala.util.parsing.input.StreamReader

/**
 * @author "Lorenz Fischer" <lfischer@ifi.uzh.ch>
 */
object NlpUtils extends NormalizeSupport {

  /**
   * This method converts the text it is passed into a sequence of terms. One term can
   * contain 1 to n words.
   *
   * For n = 2, the text "Hi, my name is Lorenz."
   * will be converted into the sequence:
   * "Hi my", "my name", "name is", "is Lorenz"
   *
   * @param inputText the text to compute the terms from.
   * @param maxLength the maximum number of words that one term can consist of.
   * @return a sequence of terms contained in the text
   */
  def computeTerms(inputText: String, maxLength: Int): Seq[String] = {
    var text = inputText

    text = text.replaceAll("\\p{Punct}+", " ") // remove all punctuation
    text = removeLinebreaks(text)

    /*
      todo: don't just return a sequence of strings, but put some weighting information along with the strings. For
      example, a term in double quotes "The President" could be given a greater weight because the author obviously
      intended to emphasize the combination of these two words.
    */
    val words = text.split(" ")
    var result = words
    for (wordsInTerm <- 2 to maxLength) {
      result ++= words.sliding(wordsInTerm).map(termSeq => termSeq.mkString(" "))
    }

    result
  }

  /**
   * This method tries to find multi-digit numbers in the text and formats them so they will be recognized as a full
   * number instead of separate digits by later stages of the processing.
   *
   * @param text
   * @return
   */
  def processNumbers(text: String): String = {
    var result = text

    // replace duplication of dollar sign and 'dollar' string
    result = result.replaceAll( """\$ Dollar""", " Dollar")

    // replace .00 with ,00 in numbers
    result = result.replaceAll( """([0-9]+)\.([0-9][0-9])\s""", """$1,$2 """)

    // replace dots and apostrophes in the numbers
    result = result.replaceAll( """([0-9])[`'\.]([0-9])""", """$1$2""")

    // replace white space between numbers
    result = result.replaceAll( """([0-9]+)\s([0-9]+)""", """$1$2""")
    // this loop was causing infinite loops
    //    while (result.matches(""".*[0-9]+\s[0-9]+.*""")) {
    //      result = result.replaceAll("""([0-9]+)\s([0-9]+)""","""$1$2""")
    //    }

    //  8000000000 Fr
    val numFr = """(.*?)([0-9]+\,?[0-9]+)\s?Fr\s(.*)""".r
    //  8000000000 CHF
    val numCHF = """(.*?)([0-9]+\,?[0-9]+)\s?CHF(.*)""".r
    //  8000000000 Chf
    val numChf = """(.*?)([0-9]+\,?[0-9]+)\s?Chf(.*)""".r
    //  8000000000 chf
    val numchf = """(.*?)([0-9]+\,?[0-9]+)\s?chf(.*)""".r
    // CHF 300000.-
    val chfNumPointDash = """(.*?)CHF ([0-9]+\,?[0-9]+)\s?\.-+(.*)""".r
    // CHF 300000
    val chfNum = """(.*?)CHF ([0-9]+\,?[0-9]+)(.*)""".r
    // EUR 300000
    val eurNum = """(.*?)EUR ([0-9]+\,?[0-9]+)(.*)""".r
    // Fr. 250000
    val frNum = """(.*?)Fr\. ([0-9]+\,?[0-9]+)\s?(.*)""".r
    // Fr. 250000.-
    val frNumPointDash = """(.*?)Fr\. ([0-9]+\,?[0-9]+)\s?\.-+(.*)""".r
    // 12000.-- Franken
    val numPointDashFranken = """(.*?)([0-9]+\,?[0-9]+)\s?\.-+ Franken(.*)""".r
    // 10000.--
    val numPointDash = """(.*?)([0-9]+\,?[0-9]+)\s?\.-+(.*)""".r
    //  8000000000 Fr.
    val numFrDot = """(.*?)([0-9]+\,?[0-9]+)\s?Fr\.(.*)""".r


    result = result match {
      case numFr(before, value, after) => s"$before$value Franken$after "
      case numCHF(before, value, after) => s"$before$value Franken$after"
      case numChf(before, value, after) => s"$before$value Franken$after"
      case numchf(before, value, after) => s"$before$value Franken$after"
      case chfNumPointDash(before, value, after) => s"$before$value Franken$after"
      case chfNum(before, value, after) => s"$before$value Franken$after"
      case eurNum(before, value, after) => s"$before$value Euro$after"
      case frNumPointDash(before, value, after) => s"$before$value Franken$after"
      case frNum(before, value, after) => s"$before$value Franken$after"
      case numPointDashFranken(before, value, after) => s"$before$value Franken$after"
      case numPointDash(before, value, after) => s"$before$value Franken$after"
      case numFrDot(before, value, after) => s"$before$value Franken$after"
      case _ => result
    }

    // replace commas
    result = result.replaceAll( """([0-9])[\,]([0-9])""", """$1%2C$2""")

    //val f = new DecimalFormat("CHF #.###")

    return result
  }

  /**
   * This method converts the text it is passed into a sequence of terms that we can further process.
   *
   * "Hi, my name is Lorenz. How are you?" turns into "Hi","my","name","is", "Lorenz", "How", "are", "you"
   *
   * @param inputText the text to normalize.
   * @return a sequence of terms contained in the text
   */
  def computeTerms(inputText: String): Seq[String] = {
    var text = processNumbers(inputText)
    text = text.replaceAll("\\p{Punct}+", " ") // remove all punctuation
    removeLinebreaks(text).split(" ")
  }


  /**
   * Remove all sorts of line breaks from a given string.
   * @param inputText the string to remove the linebreaks from.
   * @return the input string with all linebreaks removed.
   */
  def removeLinebreaks(inputText: String): String = {
    var text = inputText

    /*  Replace all kinds of newlines:
        A newline (line feed) character ('\n'),
        A carriage-return character followed immediately by a newline character ("\r\n"),
        A standalone carriage-return character ('\r'),
        A next-line character ('\u0085'),
        A line-separator character ('\u2028'), or
        A paragraph-separator character ('\u2029).
     */
    text = text.replaceAll("[\\r\\n\\u0085\\u2028\\u2029]+", " ") // replace line breaks with white space
    text = text.replaceAll("\\s+", " ") // replace double spaces with single spaces

    text
  }


  /**
   * This method converts the text it is passed into a form that is more likely to find matches when computing
   * the various NLP metrics.
   *
   * Transformations include the replacement of german umlauts and special characters such as
   * the german "sharp s" (ß) with a standardized representation.
   *
   * All
   *
   * @param text the text to normalize.
   * @return a normalized version of text.
   */
  def normalizeTerm(text: String): String = {
    // delegate to the pros..
    normalize(text).split("-").map(stemTerm(_)).mkString(" ")
    //normalize(text).split("-").mkString(" ")
  }

  /**
   * This object is used by the stemming function. As the stemmer itself is not thread-safe, we create one
   * instance per thread. The ThreadLocal class is helping in achieving this goal.
   *
   * In theory, this could still go wrong in the case that the same thread starts to stem one word, gets interrupted
   * in the process, and continues with stemming another word. However, as there are currently no Thread.wait()
   * or Thread.yield() calls in the GermanStemmer class, we should be safe in that regard.
   *
   * @see stemTerm()
   */
  private val threadLocalStemmer = new ThreadLocal[GermanStemmer] {
    override def initialValue() = {
      new GermanStemmer()
    }
  }

  /**
   * Returns a stemmed version of the provided term.
   * @param term the term to be stemmed.
   * @return the stemmed version.
   */
  def stemTerm(term: String): String = {
    threadLocalStemmer.get().stem(term)
  }

  /**
   * Searches for normalizedTerm within text and returns the denormalized form of it.
   * @param stemmedTerm
   * @return the de-stemmed version of the term.
   */
  def deStemTerm(stemmedTerm: String, stemmedToTermMap: collection.Map[String, String]): String = {
    stemmedTerm.split("_").map { st =>
      stemmedToTermMap.getOrElse(st, st)
    }.mkString(" ")
  }


  /**
   * This map maps from the stemmed form of a named entity, to the unstemmed form.
   *
   * Example: sepp_blatt -> "Sepp Blatter"
   */
  val namedEntities: Map[String, String] =
    Source.fromURL(getClass.getResource("/named_entities.txt")).getLines().map { name =>
      val original = name.replaceAll("_", " ")
      val stemmed = name.replaceAll("-","_").split("_").map(_.trim()).filter(_.length > 0).map(NlpUtils.normalizeTerm(_)).mkString("_")
      (stemmed, original)
    }.toMap
  // todo: process the wikipedia names, for now I will rely on a manual list
  //    Source.fromInputStream(new GZIPInputStream(getClass.getResourceAsStream("/wikipedia_names.gz"))).getLines().map { name =>
  //      val original = name.replaceAll("_", " ")
  //      val stemmed = name.split("_").map(_.trim()).filter(_.length > 0).mkString(" ")
  //      (stemmed, original)
  //    }.toMap


  /**
   * This set contains the first words of all stemmed named entities.
   *
   * Example:
   * "st_gallen" -> "st"
   * "sepp blatter" -> "sepp"
   */
  val namedEntityFirstPartIndex: Set[String] = namedEntities.keys.map(_.split("_")(0)).toSet


  /**
   * This method searches for multi-word named entities in the provided string and marks them by joining their
   * components with an underscore.
   *
   * Example: "hello mr. sepp blatter" will be replaced with "Hello Mr. Sepp_Blatter"
   *
   * Note: the input text is assumed to be normalized!
   *
   * @param text the text to process.
   * @return the same text, wich all named entities marked.
   *
   * @see #normalizeText
   */
  def markNamedEntities(text: String): String = {
    val wordArray = text.split(" ")

    var jumper = 0;
    // dirty hack
    val result = for (i <- 0 until wordArray.length) yield {
      var currentWord = wordArray(i)

      jumper -= 1
      if (jumper <= 0) {
        if (namedEntityFirstPartIndex.contains(currentWord)) {
          for (l <- 1 until 3 if i + l < wordArray.length) yield {
            // try words containing of i+1 until i+3. this is wasteful, but scala doesn't let me break! ffs
            val keyToLookFor = wordArray.slice(i, i + l + 1).mkString("_")
            if (namedEntities.contains(keyToLookFor)) {
              // return the current key as the word and shift i
              jumper = l + 1
              currentWord = keyToLookFor
            }
          }
        }
      } else {
        currentWord = ""
      }

      currentWord
    }

    result.filter(_.length > 0).mkString(" ")
  }


  val stopWordList: Set[String] =
    Source.fromURL(getClass.getResource("/stopwordlist_de.txt")).getLines().map(NlpUtils.normalizeTerm(_)).toSet
  //  List(
  //    "du",
  //    "aber","alle","allen","alles","als","also","am","an","andere","anderem","anderer","anderes","anders","auch","auf","aus","ausser","ausserdem","bei","beide","beiden","beides","beim","bereits","bestehen","besteht","bevor","bin","bis","bloss","brauchen","braucht","dabei","dadurch","dagegen","daher","damit","danach","dann","darf","darueber","darum","darunter","darüber","das","dass","davon","dazu","dem","den","denn","der","des","deshalb","dessen","die","dies","diese","diesem","diesen","dieser","dieses","doch","dort","duerfen","durch","durfte","durften","dürfen","ebenfalls","ebenso","ein","eine","einem","einen","einer","eines","einige","einiges","einzig","entweder","er","erst","erste","ersten","es","etwa","etwas","falls","fast","ferner","folgender","folglich","fuer","fur","für","ganz","geben","gegen","gehabt","gekonnt","gemaess","gemäss","getan","gewesen","gewollt","geworden","gibt","habe","haben","haette","haetten","hallo","hat","hatte","hatten","heraus","herein","hier","hin","hinein","hinter","hätte","hätten","ich","ihm","ihn","ihnen","ihr","ihre","ihrem","ihren","ihres","im","immer","in","indem","infolge","innen","innerhalb","ins","inzwischen","irgend","irgendwas","irgendwen","irgendwer","irgendwie","irgendwo","ist","jede","jedem","jeden","jeder","jedes","jedoch","jene","jenem","jenen","jener","jenes","kann","kein","keine","keinem","keinen","keiner","keines","koennen","koennte","koennten","konnte","konnten","kuenftig","können","könnte","könnten","künftig","leer","machen","macht","machte","machten","man","mehr","mein","meine","meinem","meinen","meiner","meist","meiste","meisten","mich","mit","moechte","moechten","muessen","muessten","muss","musste","mussten","möchte","möchten","müssen","müssten","nach","nachdem","nacher","naemlich","neben","nein","nicht","nichts","noch","nuetzt","nun","nur","nutzt","nämlich","nützt","oben","obgleich","obwohl","oder","ohne","per","pro","rund","sagt","sagte","sagten","schon","sehr","sei","seid","sein","seine","seinem","seinen","seiner","seit","seitdem","seither","selber","sich","sie","siehe","sind","so","sobald","solange","solch","solche","solchem","solchen","solcher","solches","soll","sollen","sollte","sollten","somit","sondern","soweit","sowie","spaeter","später","stets","such","uber","uber","ueber","um","ums","und","uns","unser","unsere","unserem","unseren","unten","unter","viel","viele","vollstaendig","vollständig","vom","von","vor","vorbei","vorher","vorueber","vorüber","waehrend","waere","waeren","wann","war","waren","warum","was","wegen","weil","weiter","weitere","weiterem","weiteren","weiterer","weiteres","weiterhin","welche","welchem","welchen","welcher","welches","wem","wen","wenigstens","wenn","wenngleich","wer","werde","werden","weshalb","wessen","wie","wieder","will","wir","wird","wodurch","wohin","wollen","wollte","wollten","worin","wuerde","wuerden","wurde","wurden","während","wäre","wären","würde","würden","zu","zufolge","zum","zur","zusammen","zwar","zwischen","über"
  //  ).map(NlpUtils.normalizeTerm(_)).toSet

}
