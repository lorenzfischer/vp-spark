package voxpopuli.textsummarizer

import org.scalatest.Matchers._
import org.scalatest._

import scala.collection.immutable.HashMap

/**
 *  Code copied from: https://gist.github.com/agemooij/15a0eaebc2c1ddd5ddf4
 */
class SingleWordTfSpec extends FlatSpec {

  "SingleWordTf" should "correctly convert a document into a term frequency object" in {
    val tfMap = new SingleWordTf().computeTermFrequencies(new Doc("doc1", "hallo hallo welt")).termFrequencies.toMap
    tfMap.get("hallo").get shouldBe 1
  }

}
