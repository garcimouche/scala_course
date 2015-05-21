package kvstore

import akka.actor._
import kvstore.Arbiter._
import kvstore.Persistence.{Persisted, Persist, PersistenceException}
import kvstore.Replica._
import kvstore.Replicator.{Replicated, Replicate, SnapshotAck, Snapshot}

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

  import scala.concurrent.duration._
  import scala.concurrent.ExecutionContext.Implicits.global
  import scala.language.postfixOps

  //msg use to retry persist
  case object ResendPersist

  //msg use to manage global acknowledgement
  case object ManageGlobalAck

  type Retry = Int

  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */

  var kv = Map.empty[String, String]
  // a map from secondary replicas to replicators
  var secondaries = Map.empty[ActorRef, ActorRef]

  // the current set of replicators
  //var replicators = Set.empty[ActorRef]

  //
  var expectedSeq = 0L

  var persistence = context.actorOf(persistenceProps)

  var notPersistedSeq = Map.empty[Long, (Retry,Persist)]

  //a map of ops id to a Set of Actor acknowledging persistence
  var globalAcks = Map.empty[Long,Set[ActorRef]]

  var ackRequesters = Map.empty[Long,ActorRef]

  //a set of operations already acknowledged
  var alreadyAcknoweledged = Set.empty[Long]

  var replicator:ActorRef = _

  arbiter ! Join
  //join @ creation

  override val supervisorStrategy = OneForOneStrategy() {
    case _: PersistenceException => SupervisorStrategy.resume
  }

  def receive = {
    case JoinedPrimary => context.become(leader)
    case JoinedSecondary => context.become(replica)
  }

  val common : Receive = {
    case Get(key, id) =>
      sender ! GetResult(key, kv.get(key), id)

    case ResendPersist =>
      notPersistedSeq.foreach{
        case (k,v) =>
          val (retry,persist) = v
          if(retry < 10)
            persistence ! Persist(persist.key,persist.valueOption,persist.id)
          else
            notPersistedSeq -= persist.id//retry persist for 1 sec max => 10 * 100 ms

      }
      notPersistedSeq = notPersistedSeq.mapValues{
        case(retry,persist) => (retry+1,persist)
      }
  }

  val leader: Receive = {

    common.orElse {

      case Insert(key, value, id) =>
        println(s"Insert $key,$value")
        globalAcks += ((id, Set(self)))
        ackRequesters = ackRequesters.updated(id,sender)
        kv = kv + ((key, value))
        val persist = Persist(key, Some(value), id)
        notPersistedSeq += ((id, (0, persist)))
        persistence ! persist
        secondaries.values.foreach {
          replicator =>
            replicator ! Replicate(key, Some(value), id)
            globalAcks = globalAcks.updated(id,globalAcks.get(id).get + replicator)
        }
        println(s"Insert globalAcks is now $globalAcks")

      case Remove(key, id) =>
        println(s"Remove $key id $id")
        globalAcks += ((id, Set(self)))
        ackRequesters = ackRequesters.updated(id,sender)
        kv = kv - key
        val persist = Persist(key, None, id)
        notPersistedSeq += ((id, (0,persist)))
        persistence ! persist
        secondaries.values.foreach {
          replicator =>
            replicator ! Replicate(key, None, id)
            globalAcks = globalAcks.updated(id,globalAcks.get(id).get + replicator)
        }
        println(s"Remove globalAcks is now $globalAcks")

      case Persisted(key, id) =>
        println(s"Persisted key $key id $id")
        notPersistedSeq -= id
        globalAcks = globalAcks.updated(id,globalAcks.get(id).get - self)
        if(globalAcks.get(id).get.isEmpty){
          println(s"from Persisted key $key OperationAck $id")
          ackRequesters(id) ! OperationAck(id)
          alreadyAcknoweledged+=id
        }
        else println("Persisted cannot acknowledged operation remaining:"+globalAcks.get(id).get)



      case Replicas(reps) =>
        //replicate kv store to new replica
        reps
          .filterNot(_ == self)//filter out primary
          .filterNot(secondaries.get(_).isDefined)//filter out already known replicas
          .foreach{
            replica =>
              val replicator = context.actorOf(Replicator.props(replica))//create Replicator
              kv.foreach{ case (k,v) =>
                replicator ! Replicate(k,Some(v),k.hashCode)//replicate kv store
              }
              secondaries +=((replica,replicator))//register new Replicator
          }

        println(s"Replicas secondaries is now $secondaries")
        //remove replicator having replica not present anymore
        val obsoleteReplicas = secondaries.filter{case (k,_) => !reps(k)}
        println(s"obsolete replicas $obsoleteReplicas")
        obsoleteReplicas.foreach(_._2 ! PoisonPill)//stop associated replicators
        //update outstanding acks
        globalAcks = globalAcks.mapValues(_ -- obsoleteReplicas.values)
        //acknowledgment might be ready to be sent off now that some replicas are out
        globalAcks
          .filterNot{case(id,_) => alreadyAcknoweledged(id)}
          .filter{case(_,v) => v.isEmpty}
          .foreach {case (id, _) => ackRequesters(id) ! OperationAck(id)}

        //update secondaries refs removing obsolete replica
        secondaries = secondaries -- obsoleteReplicas.keys
        println(s"secondaries after removing obsolete replicas $secondaries")


      case Replicated(key,id) =>
        println(s"Replicated key:$key id:$id globalAck is $globalAcks, sender is $sender")
        if(!alreadyAcknoweledged(id)){
          val remainingReplicators = globalAcks.get(id).get
          globalAcks = globalAcks.updated(id,remainingReplicators - sender)
          if(globalAcks.get(id).get.isEmpty){
            println(s"from Replicated key $key OperationAck $id")
            ackRequesters(id) ! OperationAck(id)
            alreadyAcknoweledged+=id
          }
        }
        else println(s"operation id $id already acknowledged")

      case ManageGlobalAck =>
        println("in ManageGlobalAck primary, globalAcks " + globalAcks);
        globalAcks
          .filterNot{case (k,v) => v.isEmpty}//remove acknowledged
          .foreach{
            case (id,v) =>
              ackRequesters(id) ! OperationFailed(id)
              alreadyAcknoweledged+=id
          }
        globalAcks = Map.empty[Long,Set[ActorRef]]

    }
  }

  val replica: Receive = {

    common.orElse{

      case Snapshot(key, value, seq) =>
        replicator=sender;
        println(s"Snapshot key:$key seq:$seq")
        if (expectedSeq == seq) {
          kv = value.fold(kv - key)(v => kv + ((key, v)))
          expectedSeq = seq + 1
          notPersistedSeq += ( (seq, (0,Persist(key, value, seq)) ) )
          persistence ! Persist(key, value, seq)
        }
        else if (expectedSeq > seq)
          replicator ! SnapshotAck(key, seq)

      case Persisted(key, id) =>
        println(s"sec.Persisted key $key id $id")
        notPersistedSeq -= id
        replicator ! SnapshotAck(key, id)

      case ManageGlobalAck => //nothing special destination is primary
        println("in replica, globalAcks should be empty" + globalAcks);
    }

  }

  //scheduler will resend Persist every 100 ms to achieve atLeastOnce Delivery to Persistence
  context.system.scheduler.schedule(100 milliseconds, 100 milliseconds, self, ResendPersist);

  //scheduler will make sure the global acknowledgment has been sent after 1 second
  context.system.scheduler.schedule(1 second, 1 second, self, ManageGlobalAck);

}

