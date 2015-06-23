package voxpopuli.util

import org.scalatest.Matchers._
import org.scalatest._

/**
 *  Code copied from: https://gist.github.com/agemooij/15a0eaebc2c1ddd5ddf4
 */
class NormalizeSupportSpec extends FlatSpec with NormalizeSupport {

  "NormalizeSupport" should "correctly normalize non -ASCII characters" in {
    normalize("ÀÁÂÃĀĂȦÄẢÅǍȀȂĄẠḀẦẤàáâä") shouldBe "aaaaaaaaaaaaaaaaaaaaaa"
    normalize("ÉÊẼĒĔËȆȄȨĖèéêẽēȅë") shouldBe "eeeeeeeeeeeeeeeee"
    normalize("ÌÍÏïØøÒÖÔöÜüŇñÇçß") shouldBe "iiiioooooouunnccss"
  }

  it should "normalize 's to nothing" in {
    normalize("aa'sbba") shouldBe "aabba"
  }

  it should "normalize & for -" in {
    normalize("aa & bb") shouldBe "aa-bb"
    normalize("aa&& & &&& bb") shouldBe "aa-bb"
  }

  it should "normalize brackets to -" in {
    normalize("aa(bb)cc") shouldBe "aa-bb-cc"
    normalize("aa((((bb)))cc") shouldBe "aa-bb-cc"
  }

  it should "normalize multiples of '-' to a single '-'" in {
    normalize("a----a--b-b-------a") shouldBe "a-a-b-b-a"
  }

  it should "normalize to lowercase" in {
    normalize("AAbAbbB") shouldBe "aababbb"
  }

  it should "normalize a string with several diacritical marks" in {
    normalize("a'sa((%%$ & b___--BB a") shouldBe "aa-b-bb-a"
  }

}
