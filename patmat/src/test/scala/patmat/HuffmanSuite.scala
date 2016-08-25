package patmat

import org.scalatest.FunSuite

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import patmat.Huffman._

@RunWith(classOf[JUnitRunner])
class HuffmanSuite extends FunSuite {
	trait TestTrees {
		val t1 = Fork(Leaf('a',2), Leaf('b',3), List('a','b'), 5)
		val t2 = Fork(Fork(Leaf('a',2), Leaf('b',3), List('a','b'), 5), Leaf('d',4), List('a','b','d'), 9)
	}


  test("weight of a larger tree") {
    new TestTrees {
      assert(weight(t1) === 5)
    }
  }


  test("chars of a larger tree") {
    new TestTrees {
      assert(chars(t2) === List('a','b','d'))
    }
  }


  test("string2chars(\"hello, world\")") {
    assert(string2Chars("hello, world") === List('h', 'e', 'l', 'l', 'o', ',', ' ', 'w', 'o', 'r', 'l', 'd'))
  }


  test("times for some string of char") {
    assert(times( List('a','b','a','d','c','a','c') ) === List( ('a',3), ('b',1), ('d',1), ('c',2) ) )
  }
  
  
  test("makeOrderedLeafList for some frequency table") {
    assert(makeOrderedLeafList(List(('t', 2), ('e', 1), ('x', 3))) === List(Leaf('e',1), Leaf('t',2), Leaf('x',3)))
  }


  test("combine of some leaf list") {
    val leaflist = List(Leaf('e', 1), Leaf('t', 2), Leaf('x', 4))
    assert(combine(leaflist) === List(Fork(Leaf('e',1),Leaf('t',2),List('e', 't'),3), Leaf('x',4)))
  }

  test("combine of some leaf list keeps ordering") {
    val leaflist = List(Leaf('e', 2), Leaf('t', 3), Leaf('x', 4))
    assert(combine(leaflist) === List(Leaf('x',4),Fork(Leaf('e',2),Leaf('t',3),List('e', 't'),5)))
  }

  test("println decoded secret") {
    println(decodedSecret.toString() === "huffmanestcool")
  }
  
  test("decode and encode a very short text should be identity") {
    new TestTrees {
      assert(decode(t1, encode(t1)("ab".toList)) === "ab".toList)
    }
  }

  test("encode decodedSecret should give secret back") {
    new TestTrees {
      assert(encode(frenchCode)(decodedSecret) === secret)
    }
  }
  
  test("encode unknown character should throw") {
    new TestTrees {
     intercept[IllegalArgumentException]{
      encode(t2)("franck".toList) 
     }
    }
  }

//  test("encode h"){
//    assert(encode(frenchCode)(List('h')) === List(0,0))
//  }
  
  test("quickencode") {
    new TestTrees {
      assert(quickEncode(frenchCode)(decodedSecret) === secret) 
    }
  }
  
}
