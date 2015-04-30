import org.scalacheck.Arbitrary._
import org.scalacheck.Gen
import org.scalacheck.Gen._
import quickcheck.{Bogus1BinomialHeap, QuickCheckHeap}
lazy val genMap: Gen[Map[Int, Int]] = for {
    k <- arbitrary[Int]
    v <- arbitrary[Int]
    m <- oneOf(const(Map.empty[Int, Int]), genMap)
  } yield m.updated(k, v)

val mp: Option[Map[Int, Int]] = genMap.sample

val q = new QuickCheckHeap with Bogus1BinomialHeap {}

val h: q.H = q.genHeap.sample.get

val min = q.findMin(h)

var newh = q.deleteMin(h)


q.findMin(newh)





