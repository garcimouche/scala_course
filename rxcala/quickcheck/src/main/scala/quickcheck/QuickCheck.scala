package quickcheck

import common._

import org.scalacheck._
import Arbitrary._
import Gen._
import Prop._

abstract class QuickCheckHeap extends Properties("Heap") with IntHeap {

  property("min1") = forAll { a: Int =>
    val h = insert(a, empty)
    findMin(h) == a
  }

  property("gen1") = forAll { (h: H) =>
    val m = if (isEmpty(h)) 0 else findMin(h)
    findMin(insert(m, h)) == m
  }

  property("2elems") = forAll { (a: Int, b: Int) =>
    if (a <= b)
      findMin(meld(insert(a, empty), insert(b, empty))) == a
    else
      findMin(meld(insert(a, empty), insert(b, empty))) == b
  }

  property("insThenRemove") = forAll { (a: Int) =>
    isEmpty(deleteMin(insert(a,empty)))
  }

  property("sorted") = forAll { (h: H) =>

    def isSorted(h:H):Boolean = {
      if(isEmpty(h))
        true
      else{
        val m = findMin(h)
        val h2 = deleteMin(h)
        isEmpty(h2) || (m <= findMin(h2) && isSorted(h2))
      }
    }

    val b = isSorted(deleteMin(h))
    b
  }

  property("melt") = forAll { (h1: H, h2:H) =>
    val m1 = findMin(h1)
    val m2 = findMin(h2)
    val h3 = meld(h1,h2)
    val m3 = findMin(h3)
    m3 == m1 || m3 == m2
  }


  property("meld") = forAll { (h1: H, h2: H) =>
     def heapEqual(h1: H, h2: H): Boolean =
       if (isEmpty(h1) && isEmpty(h2)) true
       else {
         val m1 = findMin(h1)
         val m2 = findMin(h2)
         m1 == m2 && heapEqual(deleteMin(h1), deleteMin(h2))
       }
     heapEqual(meld(h1, h2),
               meld(deleteMin(h1), insert(findMin(h1), h2)))
   }

  lazy val genHeap: Gen[H] =
    for {
      e <- arbitrary[A]
      h <- oneOf(const(empty), genHeap)
      //h <- frequency((1, const(empty)), (6, genHeap))
    } yield insert(e, h)


  implicit lazy val arbHeap: Arbitrary[H] = Arbitrary(genHeap)

}
