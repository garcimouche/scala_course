package calculator

object Polynomial {
  def computeDelta(a: Signal[Double], b: Signal[Double],
      c: Signal[Double]): Signal[Double] = {
    Signal(Math.pow(b(),2) - 4 * a() * c())
  }

  def computeSolutions(a: Signal[Double], b: Signal[Double],
      c: Signal[Double], delta: Signal[Double]): Signal[Set[Double]] = {

    def solutions(_a:Signal[Double], _b:Signal[Double],_c:Signal[Double],_d:Signal[Double]):Set[Double]= {
      val sol: Set[Double] = Set()
      if (_d() < 0)
        sol
      else {
        sol + (  (-1 * _b() + Math.sqrt(_d())) / (2 * _a()), (-1 * _b() - Math.sqrt(_d())) / (2 * _a()) )
      }
    }
      Signal(solutions(a,b,c,delta))
    }
}
