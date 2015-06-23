package voxpopuli.util

import org.scalatest.Matchers._
import org.scalatest._

import scala.collection.mutable


/**
 * @author "Lorenz Fischer" <lfischer@ifi.uzh.ch>
 */
class UtilsSpec extends FlatSpec {

    "getTopNthElement" should "find the top nth element" in {
      Utils.getTopNthElement(mutable.ArraySeq(2, 4, 3, 1), 2) should be (3)
    }

    it should "be able to deal with small seqs" in {
      Utils.getTopNthElement(mutable.ArraySeq(4, 3), 2) should be (3)
      Utils.getTopNthElement(mutable.ArraySeq(4), 2) should be (4)
    }

}
