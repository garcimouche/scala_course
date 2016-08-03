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
      if(c == 0 || c == r) 1
      else pascal (c-1, r-1) + pascal (c, r-1)
    }
  
  /**
   * Exercise 2
   */
    def balance(chars: List[Char]): Boolean = {
      def count(chars: List[Char], current:Int):Boolean={
        if(current < 0) 
          false
        else if (chars.isEmpty)
          current == 0
        else if (chars(0) == '(') 
          count(chars.tail,current+1)
        else if(chars(0) == ')') 
          count(chars.tail,current-1)
        else count(chars.tail,current)
      }
        
        
      count(chars,0)
    }
  
  /**
   * Exercise 3
   */
    def countChange(money: Int, coins: List[Int]): Int = ???
  }
