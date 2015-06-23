package voxpopuli.util

import org.scalatest.Matchers._
import org.scalatest._


/**
 * @author "Lorenz Fischer" <lfischer@ifi.uzh.ch>
 */
class NlpUtilsSpec extends FlatSpec {

  //"NlpUtils.normalizeTerm"
  ignore should "normalize a sequence of terms" in {
    NlpUtils.normalizeTerm("Lustigen") should be("lustig")
    NlpUtils.normalizeTerm("äffchen") should be("affch")
  }

  ignore should "not stumble over empty strgings" in {
    NlpUtils.normalizeTerm("") shouldBe ("")
  }

  "NlpUtils.processNumbers" should "parse money values" in {

    //"Erstellt: 06.11.2014, 00:11 Uhr"


    val testValues = Map(
      "Fr. 100000" -> "100000 Franken"
      ,"700'000.--." -> "700000 Franken."
      ,"20000CHF" -> "20000 Franken"
      ,"50'000-mal" -> "50000-mal"
      ,"CHF 15'000.00 " -> "15000%2C00 Franken "
      ,"25 000 Fr " -> "25000 Franken "
      ,"100.000 Tote" -> "100000 Tote"
      ,"CHF 1'000" -> "1000 Franken"
      ,"241 000 Euro" -> "241000 Euro"
      ,"ca 8'000'000'000 Fr." -> "ca 8000000000 Franken"
      //,"UBS mit 60 000 000 000 .- gerettet" -> "UBS mit 60000000000 Franken gerettet" // todo: support this without endless while loops
      ,"1,77" -> "1%2C77"
      ,"9,77 Millionen" -> "9%2C77 Millionen"
      ,"12'000.-- Franken" -> "12000 Franken"
      ,"8'000.--" -> "8000 Franken"
      ,"CHF 300'000.- p.a." -> "300000 Franken p.a."
      ,"60'000 CHF" -> "60000 Franken"
      ,"10 000 Franken" -> "10000 Franken"
      ,"22 000 Arbeitsplätze" -> "22000 Arbeitsplätze"
      ,"250'000 Franken" -> "250000 Franken"
      ,"Fr. 250'000.-"  -> "250000 Franken"
      ,"120 000 Franken" -> "120000 Franken"
      ,"EUR 100'000" -> "100000 Euro"
      ,"223.000 Euro" -> "223000 Euro"
      ,"200'000 Euro" -> "200000 Euro"
      ,"22,5 Millionen Euro" -> "22%2C5 Millionen Euro"
      ,"10.000 Dollar" -> "10000 Dollar"
      ,"knapp 10.000 Dollar spendete" -> "knapp 10000 Dollar spendete"
      ,"als 250.000 Dollar (223.000 Euro) zusammengekommen" -> "als 250000 Dollar (223000 Euro) zusammengekommen"
      ,"auf 31 000 an" -> "auf 31000 an"
      ,"knapp 16 000, stieg" -> "knapp 16000, stieg"
      , "in 4'600'000 Litern" -> "in 4600000 Litern"
      ,"1`300 000`000.00$ Dollar" -> "1300000000%2C00 Dollar"
      ,"30-70'000" -> "30-70000"
      ,"97.000" -> "97000"
      ,"damit fast 200'000.- im Jahr" -> "damit fast 200000 Franken im Jahr"
      ,"sind 200.000.- realistisch" -> "sind 200000 Franken realistisch"
    )

    testValues.foreach { tup =>
      val (input, output) = tup
      NlpUtils.processNumbers(input) shouldBe output
    }

  }

  "NlpUtils.markNamedEntities" should "mark wikipedia names in the text" in {

    val testValues = Map(
      NlpUtils.normalizeTerm("hello mr. sepp blatter") -> "hello mr sepp_blatt"
      ,NlpUtils.normalizeTerm("hello mr. Johann_Schneider-Ammann was") -> "hello mr johann_schneid_ammann was"
    )

    testValues.foreach { tup =>
      val (input, output) = tup
      NlpUtils.markNamedEntities(input) shouldBe output
    }

  }

//  "NlpUtils.markNamedEntities" should "mark wikipedia names in the text" in {
//
//    val testValues = Map(
//      NlpUtils.normalizeTerm("hello mr. sepp blatter") -> "hello mr sepp_blatt"
//      ,NlpUtils.normalizeTerm("hello mr. Johann_Schneider-Ammann was") -> "hello mr johann_schneid_ammann was"
//    )
//
//    testValues.foreach { tup =>
//      val (input, output) = tup
//      NlpUtils.markNamedEntities(input) shouldBe output
//    }
//
//  }

  "NlpUtils.computeTerms" should "create multi word terms" in {
    val terms = NlpUtils.computeTerms("Hello Lorenz, how are you?", 2).toSet
    terms.contains("Hello") shouldBe true
    terms.contains("Lorenz") shouldBe true
    terms.contains("how") shouldBe true
    terms.contains("are") shouldBe true
    terms.contains("you") shouldBe true
    terms.contains("Hello Lorenz") shouldBe true
    terms.contains("Lorenz how") shouldBe true
    terms.contains("how are") shouldBe true
    terms.contains("are you") shouldBe true
    terms.size shouldBe 9
  }

  it should "create long multi word terms" in {
    val terms = NlpUtils.computeTerms("holy moly this is fun", 4).toSet
    terms.contains("holy") shouldBe true
    terms.contains("moly") shouldBe true
    terms.contains("this") shouldBe true
    terms.contains("is") shouldBe true
    terms.contains("fun") shouldBe true

    terms.contains("holy moly") shouldBe true
    terms.contains("moly this") shouldBe true
    terms.contains("this is") shouldBe true
    terms.contains("is fun") shouldBe true

    terms.contains("holy moly this") shouldBe true
    terms.contains("moly this is") shouldBe true
    terms.contains("this is fun") shouldBe true

    terms.contains("holy moly this is") shouldBe true
    terms.contains("moly this is fun") shouldBe true

    terms.size shouldBe 14
  }

  it should "ignore punctuation" in {
    val terms = NlpUtils.computeTerms("holy moly. this is fun!!!", 3).toSet
    terms.contains("holy") shouldBe true
    terms.contains("moly") shouldBe true
    terms.contains("this") shouldBe true
    terms.contains("is") shouldBe true
    terms.contains("fun") shouldBe true

    terms.contains("holy moly") shouldBe true
    terms.contains("moly this") shouldBe true
    terms.contains("this is") shouldBe true
    terms.contains("is fun") shouldBe true

    terms.contains("holy moly this") shouldBe true
    terms.contains("moly this is") shouldBe true
    terms.contains("this is fun") shouldBe true

    terms.size shouldBe 12
  }

  "NlpUtils" should "read the stopword list" in {
    val swl = NlpUtils.stopWordList
    swl.size should be > 1
  }
}
