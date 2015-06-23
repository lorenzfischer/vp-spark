package voxpopuli.textsummarizer

import org.scalatest.Matchers._
import org.scalatest._

/**
 *  Code copied from: https://gist.github.com/agemooij/15a0eaebc2c1ddd5ddf4
 */
class MultiWordTfSpec extends FlatSpec {

  //"MultiWordTf"
  ignore should "create single word term frequencies" in {
    val tfMap = new MultiWordTf(1).computeTermFrequencies(new Doc("doc1", "hallo hallo welt")).termFrequencies.toMap
    tfMap.get("hallo").get shouldBe (2.0 / 2)
    tfMap.get("wel").get shouldBe (1.0 / 2)
  }


  ignore should "create multi word term frequencies" in {
    val tfMap = new MultiWordTf(2).computeTermFrequencies(new Doc("doc1", "hallo hallo welt")).termFrequencies.toMap

    /*
      hallo
      hallo hallo
      hallo wel
      wel
     */

    tfMap.get("hallo").get shouldBe (2.0 / 2)
    tfMap.get("wel").get shouldBe (1.0 / 2)
    tfMap.get("hallo hallo").get shouldBe (1.0 / 2)
    tfMap.get("hallo wel").get shouldBe (1.0 / 2)
  }

}
