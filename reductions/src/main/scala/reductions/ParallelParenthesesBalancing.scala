package reductions

import scala.annotation._
import org.scalameter._
import common._

object ParallelParenthesesBalancingRunner {

  @volatile var seqResult = false

  @volatile var parResult = false

  val standardConfig = config(
    Key.exec.minWarmupRuns -> 40,
    Key.exec.maxWarmupRuns -> 80,
    Key.exec.benchRuns -> 120,
    Key.verbose -> true
  ) withWarmer(new Warmer.Default)

  def main(args: Array[String]): Unit = {
    val length = 100000
    val chars = new Array[Char](length)
    val threshold = 50000
    val seqtime = standardConfig measure {
      seqResult = ParallelParenthesesBalancing.balance(chars)
    }
    println(s"sequential result = $seqResult")
    println(s"sequential balancing time: $seqtime ms")

    val fjtime = standardConfig measure {
      parResult = ParallelParenthesesBalancing.parBalance(chars, threshold)
    }
    println(s"parallel result = $parResult")
    println(s"parallel balancing time: $fjtime ms")
    println(s"speedup: ${seqtime / fjtime}")
  }
}

object ParallelParenthesesBalancing {

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
   */
  def balance(chars: Array[Char]): Boolean = {
    def paren(c:Array[Char], count:Int):Boolean = {
      if(c.isEmpty && count == 0) true
      else if(c.isEmpty && count !=0) false
      else if(count < 0) false
      else paren(c.tail, if(c.head == '(') count+1 else if(c.head == ')') count-1 else count)
    }
    paren(chars,0)
  }

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
   */
  def parBalance(chars: Array[Char], threshold: Int): Boolean = {

    def traverse(idx: Int, until: Int, arg1: Int, arg2: Int) : (Int,Int) = {
      var i = idx
      var (o,c) = (0,0)
      while(i < until) {
        if(chars(i)=='(') o = o + 1
        else if (chars(i)==')' && o > 0) o = o - 1
        else if (chars(i)==')' && o == 0) c = c + 1
        i = i + 1
      }
      (o,c)
    }

    def reduce(from: Int, until: Int) : (Int,Int) = {
      if(until-from <= threshold)
        traverse(from,until,0,0)
      else{
        val mid = (until-from) / 2 + from
        val ((lo,lc),(ro,rc)) = parallel(reduce(from,mid), reduce(mid,until))
        (lo+ro-rc,lc)
      }
    }

    if(reduce(0, chars.length) == (0,0)) true else false 
  }

  // For those who want more:
  // Prove that your reduction operator is associative!

}
