package reductions

import java.util.concurrent._
import scala.collection._
import org.scalatest.FunSuite
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import common._

import ParallelParenthesesBalancing._

@RunWith(classOf[JUnitRunner])
class ParallelParenthesesBalancingSuite extends FunSuite {

  test("balance should work for empty string") {
    def check(input: String, expected: Boolean) =
      assert(balance(input.toArray) == expected,
        s"balance($input) should be $expected")

    check("", true)
  }

  test("balance should work for string of length 1") {
    def check(input: String, expected: Boolean) =
      assert(balance(input.toArray) == expected,
        s"balance($input) should be $expected")

    check("(", false)
    check(")", false)
    check(".", true)
  }

  test("balance should work for string of length 2") {
    def check(input: String, expected: Boolean) =
      assert(balance(input.toArray) == expected,
        s"balance($input) should be $expected")

    check("()", true)
    check(")(", false)
    check("((", false)
    check("))", false)
    check(".)", false)
    check(".(", false)
    check("(.", false)
    check(").", false)
  }


  test("parbalance should work for string of length 2") {
    def check(input: String, expected: Boolean) =
      assert(parBalance(input.toArray,1) == expected,
        s"parbalance($input) should be $expected")

    check("()", true)
    check(")(", false)
    check("((", false)
    check("))", false)
    check(".)", false)
    check(".(", false)
    check("(.", false)
    check(").", false)
  }

  test("parbalance should work for more complex input st") {
    def check(input: String, expected: Boolean) =
      assert(parBalance(input.toArray,10) == expected,
        s"parbalance($input) should be $expected")

    check("((Toto est un l)(ent bb))je(( (mue le)))", true)
    check(")(Toto est u((n lent bb je  mue le))()()", false)
    check(")Toto est un ((lent bb je  (mue le)))() ", false)
    check(" (              )(  ",false)
  }
  
}