package kvstore

import akka.actor.{Actor, ActorRef, Props}

object Replicator {

  case class Replicate(key: String, valueOption: Option[String], id: Long)

  case class Replicated(key: String, id: Long)

  case class Snapshot(key: String, valueOption: Option[String], seq: Long)

  case class SnapshotAck(key: String, seq: Long)

  def props(replica: ActorRef): Props = Props(new Replicator(replica))
}

class Replicator(val replica: ActorRef) extends Actor {

  import kvstore.Replicator._

  import scala.concurrent.duration._
  import scala.concurrent.ExecutionContext.Implicits.global
  import scala.language.postfixOps

  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */

  /**
   * In case Snapshot are not Acknowledged then we need to resend
   */
  private case object ResendSnapshot;

  // map from sequence number to pair of sender and request
  var acks = Map.empty[Long, (ActorRef, Replicate)]

  // a sequence of not-yet-sent snapshots (you can disregard this if not implementing batching)
  var pending = Vector.empty[Snapshot]

  var _seqCounter = 0L

  def nextSeq = {
    val ret = _seqCounter
    _seqCounter += 1
    ret
  }

  def receive: Receive = {

    case Replicate(key, value, id) =>
      val seq = nextSeq
      println(s"Replicate key $key id $id next seq is $seq")
      acks = acks + ((seq, (replica, Replicate(key, value, id))))
      replica ! Snapshot(key, value, seq)

    case SnapshotAck(key, seq) =>
      println(s"SnapshotAck key $key seq $seq")
      acks = acks - seq //remove acknowledged
      context.parent ! Replicated(key,seq)//acknowledge primary

    case ResendSnapshot =>
      println(s"ResendSnapshot")
      acks.foreach {
        case (seq, (rep, req)) => rep ! Snapshot(req.key, req.valueOption, seq)
      }

  }

  //scheduler will resend Snapshot every 100 ms to achieve atLeastOnce Delivery to Replica
  context.system.scheduler.schedule(100 milliseconds, 100 milliseconds, self, ResendSnapshot);


}
