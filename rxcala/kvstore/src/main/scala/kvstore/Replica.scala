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
  case class ResendPersist(msg:Persist)

  //msg use to manage global acknowledgement
  case class FailGlobalAck(id:Long)

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

  var persisted = Set.empty[Long]

  //a map of ops id to a Set of Actor acknowledging persistence
  var globalAcks = Map.empty[Long,Set[ActorRef]]

  var ackRequesters = Map.empty[Long,ActorRef]

  //a set of operations already acknowledged
  var alreadyAcknoweledged = Set.empty[Long]

  var replicator:ActorRef = _

  //join @ creation
  arbiter ! Join

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

    case ResendPersist(persist) =>
      if(!persisted(persist.id)){
        persistence ! persist
        context.system.scheduler.scheduleOnce(100 milliseconds, self, ResendPersist(persist));
      }

  }

  val leader: Receive = {

    common.orElse {

      case Insert(key, value, id) =>
        kv = kv + ((key, value))
        update(Insert(key,value,id),Some(value))

      case Remove(key, id) =>
        kv = kv - key
        update(Remove(key,id),None)

      case Persisted(key, id) =>
        println(s"Persisted key $key id $id")
        persisted = persisted + id
        globalAcks = globalAcks.updated(id,globalAcks.get(id).get - self)
        if(!alreadyAcknoweledged(id) && globalAcks.get(id).get.isEmpty){
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
                replicator ! Replicate(k,Some(v), Integer.MIN_VALUE)//replicate kv store
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
          val remainingReplicators = globalAcks.get(id)
          if(remainingReplicators.isDefined){
            globalAcks = globalAcks.updated(id,remainingReplicators.get - sender)
            if(globalAcks.get(id).get.isEmpty){
              println(s"from Replicated key $key OperationAck $id")
              ackRequesters(id) ! OperationAck(id)
              alreadyAcknoweledged+=id
            }
          }
          else println(s"Replicated recvd but cannot find replicator in globalAck for id $id!!")
        }
        else println(s"operation id $id already acknowledged")

      case FailGlobalAck(id) =>
        println("in FailGlobalAck primary, globalAcks " + globalAcks);
        if(!globalAcks(id).isEmpty){
          ackRequesters(id)!OperationFailed(id)
          alreadyAcknoweledged+=id
        }
        //clean up already processed
        globalAcks -= id
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
          val persist = Persist(key, value, seq)
          persistence ! persist
          context.system.scheduler.scheduleOnce(100 milliseconds, self, ResendPersist(persist));
        }
        else if (expectedSeq > seq)
          replicator ! SnapshotAck(key, seq)

      case Persisted(key, id) =>
        println(s"sec.Persisted key $key id $id")
        persisted = persisted + id
        replicator ! SnapshotAck(key, id)
    }

  }


  private def update(op:Operation, value:Option[String]):Unit={
    val key=op.key
    println(s"$op $key,$value")
    globalAcks += ((op.id, Set(self)))
    ackRequesters = ackRequesters.updated(op.id,sender)
    secondaries.values.foreach {
      replicator =>
        replicator ! Replicate(key, value, op.id)
        globalAcks = globalAcks.updated(op.id,globalAcks.get(op.id).get + replicator)
    }
    //persist
    val persist = Persist(key, value, op.id)
    persistence ! persist
    //schedule an eventual resend of persistence message
    context.system.scheduler.scheduleOnce(100 milliseconds, self, ResendPersist(persist));
    //scheduler will make sure the global acknowledgment has been sent after 1 second
    context.system.scheduler.scheduleOnce(1 second, self, FailGlobalAck(op.id));
    println(s"$op globalAcks is now $globalAcks")
 }
}

