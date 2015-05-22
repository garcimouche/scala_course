/**
 * Copyright (C) 2013-2015 Typesafe Inc. <http://www.typesafe.com>
 */
package kvstore

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import kvstore.Replicator.{Snapshot, SnapshotAck}
import org.scalactic.ConversionCheckedTripleEquals
import org.scalatest.{BeforeAndAfterAll, FunSuiteLike, Matchers}

import scala.concurrent.duration._

class IntegrationSpec(_system: ActorSystem) extends TestKit(_system)
    with FunSuiteLike
        with Matchers
    with BeforeAndAfterAll
    with ConversionCheckedTripleEquals
    with ImplicitSender
    with Tools {

  import kvstore.Arbiter._

  def this() = this(ActorSystem("ReplicatorSpec"))

  override def afterAll: Unit = system.shutdown()

  /*
   * Recommendation: write a test case that verifies proper function of the whole system,
   * then run that with flaky Persistence and/or unreliable communication (injected by
   * using an Arbiter variant that introduces randomly message-dropping forwarder Actors).
   */

  test("case1: Make sure Global Acknowledgment overlapped overt time") {
    val arbiter = TestProbe()
    val persistence = TestProbe()
    val primary = system.actorOf(Replica.props(arbiter.ref, Persistence.props(flaky = false)), "case1-primary")
    val client = session(primary)
    val secondaryA, secondaryB = TestProbe()


    arbiter.expectMsg(Join)
    arbiter.send(primary, JoinedPrimary)

    arbiter.send(primary, Replicas(Set(primary, secondaryA.ref, secondaryB.ref)))

    val setId = client.set("foo", "bar")
    val seqA = secondaryA.expectMsgType[Snapshot].seq
    val seqB = secondaryB.expectMsgType[Snapshot].seq
    client.nothingHappens(300.milliseconds)
    secondaryA.reply(SnapshotAck("foo", seqA))
    client.nothingHappens(300.milliseconds)
    secondaryB.reply(SnapshotAck("foo", seqB))
    val setId2 = client.set("foo2", "bar2")
    client.waitAck(setId)
    client.nothingHappens(600.milliseconds)
    secondaryA.reply(SnapshotAck("foo2", 1))
    secondaryB.reply(SnapshotAck("foo2", 1))
    client.waitAck(setId2)
  }



  }
