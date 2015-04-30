package nodescala

import nodescala.NodeScala._
import org.junit.runner.RunWith
import org.scalatest._
import org.scalatest.junit.JUnitRunner

import scala.async.Async.async
import scala.collection._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._
import scala.concurrent.duration._
import scala.language.postfixOps

@RunWith(classOf[JUnitRunner])
class NodeScalaSuite extends FunSuite {

  test("A Future should always be completed") {
    val always = Future.always(517)

    assert(Await.result(always, 0 nanos) == 517)
  }

  test("A Future should never be completed") {
    val never = Future.never[Int]

    try {
      Await.result(never, 1 second)
      assert(false)
    } catch {
      case t: TimeoutException =>
      // ok!
    }
  }

//  test("Any futures in the given list") {
//    val any = Future.any(
//      List(
//        Future{ blocking { Thread.sleep(3000);1}},
//        Future { blocking {Thread.sleep(500);2}},
//        Future { blocking {Thread.sleep(1500); throw new IllegalArgumentException("error in future")}}
//      )
//    );
//
//    try {
//      val res = Await.result(any, 1 second)
//      assert(res == 1 || res == 2)
//      println("any result is " + res)
//    } catch {
//      case iae: IllegalArgumentException => {
//        assertResult(iae.getMessage)("error in future")//ok
//        println("error in future")
//      }
//    }
//  }
//
//
//
//  test("aLL futures in the given list") {
//    val all = Future.all(
//      List(
//        Future{ blocking { Thread.sleep(1000);1}},
//        Future { blocking {Thread.sleep(500);2}},
//        Future { blocking {Thread.sleep(1500); throw new IllegalArgumentException("error in future")}}
//      )
//    );
//
//    try {
//      val res = Await.result(all, 5 second)
//      assert(res == List(1,2));
//    } catch {
//      case iae: IllegalArgumentException => {
//        assertResult(iae.getMessage)("error in future")//ok
//      }
//    }
//  }
//
//  test("future.delay") {
//    val res = Await.result(Future.delay(1 second),6 seconds);
//    assert(res == {})
//  }
//
//  test("future.now thows error") {
//    try{
//      Future.never.now
//      assert(false,"a never completable future should not be completed now")
//    }
//    catch {
//      case nse : NoSuchElementException => //ok
//    }
//  }
//
//  test("future.now return val") {
//    val res = Future.always(123).now
//    assert(res == 123)
//  }

  test("CancellationTokenSource should allow stopping the computation") {
    val cts = CancellationTokenSource()
    val ct = cts.cancellationToken
    val p = Promise[String]()

    async {
      while (ct.nonCancelled) {
        // do work
      }

      p.success("done")
    }

    cts.unsubscribe()
    assert(Await.result(p.future, 1 second) == "done")
  }


//  test("run then cancel"){
//    val working = Future.run() { ct =>
//      Future {
//        while (ct.nonCancelled) {
//          println("working")
//        }
//        println("done")
//      }
//    }
//    Future.delay(1 seconds) onSuccess {
//      case _ => working.unsubscribe()
//    }
//  }

  class DummyExchange(val request: Request) extends Exchange {
    @volatile var response = ""
    val loaded = Promise[String]()
    def write(s: String) {
      response += s
    }
    def close() {
      loaded.success(response)
    }
  }

  class DummyListener(val port: Int, val relativePath: String) extends NodeScala.Listener {
    self =>

    @volatile private var started = false
    var handler: Exchange => Unit = null

    def createContext(h: Exchange => Unit) = this.synchronized {
      assert(started, "is server started?")
      handler = h
    }

    def removeContext() = this.synchronized {
      assert(started, "is server started?")
      handler = null
    }

    def start() = self.synchronized {
      started = true
      new Subscription {
        def unsubscribe() = self.synchronized {
          started = false
        }
      }
    }

    def emit(req: Request) = {
      val exchange = new DummyExchange(req)
      if (handler != null) handler(exchange)
      exchange
    }
  }

  class DummyServer(val port: Int) extends NodeScala {
    self =>
    val listeners = mutable.Map[String, DummyListener]()

    def createListener(relativePath: String) = {
      val l = new DummyListener(port, relativePath)
      listeners(relativePath) = l
      l
    }

    def emit(relativePath: String, req: Request) = this.synchronized {
      val l = listeners(relativePath)
      l.emit(req)
    }
  }
  test("Server should serve requests") {
    val dummy = new DummyServer(8191)
    val dummySubscription = dummy.start("/testDir") {
      request => for (kv <- request.iterator) yield (kv + "\n").toString
    }

    // wait until server is really installed
    Thread.sleep(500)

    def test(req: Request) {
      val webpage = dummy.emit("/testDir", req)
      val content = Await.result(webpage.loaded.future, 2 second)
      val expected = (for (kv <- req.iterator) yield (kv + "\n").toString).mkString
      assert(content == expected, s"'$content' vs. '$expected'")
    }

    test(immutable.Map("StrangeRequest" -> List("Does it work?")))
    test(immutable.Map("StrangeRequest" -> List("It works!")))
    test(immutable.Map("WorksForThree" -> List("Always works. Trust me.")))

    dummySubscription.unsubscribe()
  }

}




