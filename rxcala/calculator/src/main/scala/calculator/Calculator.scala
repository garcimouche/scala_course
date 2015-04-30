package calculator

sealed abstract class Expr
final case class Literal(v: Double) extends Expr
final case class Ref(name: String) extends Expr
final case class Plus(a: Expr, b: Expr) extends Expr
final case class Minus(a: Expr, b: Expr) extends Expr
final case class Times(a: Expr, b: Expr) extends Expr
final case class Divide(a: Expr, b: Expr) extends Expr

object Calculator {
  def computeValues(
      namedExpressions: Map[String, Signal[Expr]]): Map[String, Signal[Double]] = {
    namedExpressions.map{case (k,_) => (k,Signal(eval(namedExpressions(k)(),namedExpressions)))}
  }

  def eval(expr: Expr, references: Map[String, Signal[Expr]]): Double = {

    def _eval(expr: Expr,refs:Set[String]):Double = {
      expr match {
        case Literal(v) => v
        case Ref(name) =>
          if(refs.contains(name))
            _eval(Literal(Double.NaN),refs)
          else
            _eval(getReferenceExpr(name,references),refs + name)
        case Plus(a,b) => _eval(a,refs) + _eval(b,refs)
        case Minus(a,b) => _eval(a,refs) - _eval(b,refs)
        case Times(a,b) => _eval(a,refs) * _eval(b,refs)
        case Divide(a,b) => _eval(a,refs) / _eval(b,refs)
      }
    }

    _eval(expr,Set())

  }

  /** Get the Expr for a referenced variables.
   *  If the variable is not known, returns a literal NaN.
   */
  private def getReferenceExpr(name: String,
      references: Map[String, Signal[Expr]]) = {
    references.get(name).fold[Expr] {
      Literal(Double.NaN)
    } { exprSignal =>
      exprSignal()
    }
  }
}
