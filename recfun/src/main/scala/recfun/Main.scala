package recfun

object Main {
  def main(args: Array[String]) {
    println("Pascal's Triangle")
    for (row <- 0 to 10) {
      for (col <- 0 to row)
        print(pascal(col, row) + " ")
      println()
    }
  }

  /**
   * Exercise 1
   */
  def pascal(c: Int, r: Int): Int = {
    require(c <= r, () => "col must be <= row")
    if (c == 0 || c == r) 1
    else pascal(c - 1, r - 1) + pascal(c, r - 1)
  }

  /**
   * Exercise 2
   */
  def balance(chars: List[Char]): Boolean = {
    def count(chars: List[Char], current: Int): Boolean = {
      if (current < 0)
        false
      else if (chars.isEmpty)
        current == 0
      else if (chars(0) == '(')
        count(chars.tail, current + 1)
      else if (chars(0) == ')')
        count(chars.tail, current - 1)
      else count(chars.tail, current)
    }

    count(chars, 0)
  }

  /**
   * Exercise 3
   */
  def countChange(money: Int, coins: List[Int]): Int = {
    require(money >= 0)

    def addCoins(coins: List[Int], acc: Int): Int = {
      if (money < acc || coins.isEmpty) 0
      else if (money == acc) 1
      else addCoins(coins, acc + coins.head) + addCoins(coins.tail, acc)
    }

    if (coins.isEmpty || money == 0) 0

    else addCoins(coins, coins.head) + countChange(money, coins.tail);

  }
  
  
  
  
  
}
