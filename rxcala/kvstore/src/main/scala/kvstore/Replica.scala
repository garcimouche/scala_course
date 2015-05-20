package kvstore

import akka.actor.{Actor, ActorRef, Props}
import kvstore.Arbiter._
import kvstore.Replica._
import kvstore.Replicator.{SnapshotAck, Snapshot}

object Replica {
  sealed trait Operation {
    def key: String
    def id: Long
  }
  case class Insert(key: String, value: String, id: Long) extends Operation
  case class Remove(key: String, id: Long) extends Operation
  case class Get(key: String, id: Long) extends Operation

  sealed trait OperationReply
  case class OperationAck(id: Long) extends OperationReply
  case class OperationFailed(id: Long) extends OperationReply
  case class GetResult(key: String, valueOption: Option[String], id: Long) extends OperationReply

  def props(arbiter: ActorRef, persistenceProps: Props): Props = Props(new Replica(arbiter, persistenceProps))
}

class Replica(val arbiter: ActorRef, persistenceProps: Props) extends Actor {

  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */
  
  var kv = Map.empty[String, String]
  // a map from secondary replicas to replicators
  var secondaries = Map.empty[ActorRef, ActorRef]
  // the current set of replicators
  var replicators = Set.empty[ActorRef]

  //
  var expectedSeq = 0L

  arbiter ! Join //join @ creation

  def receive = {
    case JoinedPrimary   => context.become(leader)
    case JoinedSecondary => context.become(replica)
  }

  val leader: Receive = {
    case Insert(key,value,id) =>
      kv = kv + ((key,value))
      sender ! OperationAck(id)
    case Remove(key,id) =>
      kv = kv - key
      sender ! OperationAck(id)
    case Get(key,id) =>
      sender ! GetResult(key,kv.get(key),id)
  }

  val replica: Receive = {
    case Get(key,id) =>
      sender ! GetResult(key,kv.get(key),id)
    case Snapshot(key,value,seq) =>
      if(expectedSeq == seq){
        kv = value.fold(kv - key)(v => kv + ((key,v)))
        expectedSeq = seq+1
        sender ! SnapshotAck(key,seq)
      }
      else if(expectedSeq > seq)
        sender ! SnapshotAck(key,seq)
  }

}

