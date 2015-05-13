/**
 * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */
package actorbintree

import actorbintree.BinaryTreeNode.{CopyFinished, CopyTo}
import actorbintree.BinaryTreeSet._
import akka.actor._

import scala.collection.immutable.Queue

object BinaryTreeSet {

  trait Operation {
    def requester: ActorRef

    def id: Int

    def elem: Int
  }

  trait OperationReply {
    def id: Int
  }

  /** Request with identifier `id` to insert an element `elem` into the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Insert(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to check whether an element `elem` is present
    * in the tree. The actor at reference `requester` should be notified when
    * this operation is completed.
    */
  case class Contains(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to remove the element `elem` from the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Remove(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request to perform garbage collection */
  case object GC

  /** Holds the answer to the Contains request with identifier `id`.
    * `result` is true if and only if the element is present in the tree.
    */
  case class ContainsResult(id: Int, result: Boolean) extends OperationReply

  /** Message to signal successful completion of an insert or remove operation. */
  case class OperationFinished(id: Int) extends OperationReply

}


class BinaryTreeSet extends Actor {

  import actorbintree.BinaryTreeSet._

  def createRoot: ActorRef = context.actorOf(BinaryTreeNode.props(0, initiallyRemoved = true))

  var root = createRoot

  // optional
  var pendingQueue: Queue[Operation] = Queue.empty[Operation]

  // optional
  def receive = normal

  // optional
  /** Accepts `Operation` and `GC` messages. */
  val normal: Receive = {

    case msg:Operation => root ! msg

    case GC => {
      val newRoot = createRoot
      context.become(garbageCollecting(newRoot))
      root ! CopyTo(newRoot)
    }

  }


  // optional
  /** Handles messages while garbage collection is performed.
    * `newRoot` is the root of the new binary tree where we want to copy
    * all non-removed elements into.
    */
  def garbageCollecting(newRoot: ActorRef): Receive = {

    case msg:Operation => pendingQueue = pendingQueue.enqueue(msg)

    case CopyFinished =>
      while(!pendingQueue.isEmpty){//drain pending queue
        val (op,q) = pendingQueue.dequeue;
        newRoot ! op;
        pendingQueue = q;
      }
      root ! PoisonPill//kill old root actor and his descendants
      root = newRoot
      context.become(normal)

  }

}

object BinaryTreeNode {

  trait Position

  case object Left extends Position

  case object Right extends Position

  case class CopyTo(treeNode: ActorRef)

  case object CopyFinished

  def props(elem: Int, initiallyRemoved: Boolean) = Props(classOf[BinaryTreeNode], elem, initiallyRemoved)
}

class BinaryTreeNode(val elem: Int, initiallyRemoved: Boolean) extends Actor {

  import actorbintree.BinaryTreeNode._

  var subtrees = Map[Position, ActorRef]()
  var removed = initiallyRemoved

  // optional
  def receive = normal

  // optional

  /** Handles `Operation` messages and `CopyTo` requests. */
  val normal: Receive = {

    case Contains(req, id, elm) =>
      if (elm < elem)
        subtrees.get(Left).fold(req ! ContainsResult(id, false))(left => left ! Contains(req, id, elm))
      else if (elm > elem)
        subtrees.get(Right).fold(req ! ContainsResult(id, false))(right => right ! Contains(req, id, elm))
      else req ! ContainsResult(id,!removed)

    case Insert(req, id, elm) =>
      if (elm < elem)
        handleInsert(Left, req, id, elm)
      else if (elm > elem)
        handleInsert(Right, req, id, elm)
      else {
        if(removed) removed=false
        req ! OperationFinished(id)
      }

    case Remove(req, id, elm) =>
      if (elm < elem)
        handleRemove(Left, req, id, elm)
      else if (elm > elem)
        handleRemove(Right, req, id, elm)
      else {
        removed = true
        req ! OperationFinished(id)
      }

    case CopyTo(target) =>
      val children = subtrees.values.toSet
        children.foreach(_ ! CopyTo(target))
        context.become(copying(children, removed))
        if (!removed)
          target ! Insert(self, -1, elem)
        else
          self ! OperationFinished(-1)
  }

  private def handleInsert(pos: Position, requester: ActorRef, id: Int, elm: Int) = {
    if (subtrees.get(pos).isDefined)
      subtrees.get(pos).get ! Insert(requester, id, elm) //pass the msgs downward
    else {
      //not found add it
      subtrees = subtrees + ((pos, context.actorOf(BinaryTreeNode.props(elm, initiallyRemoved = false))))
      requester ! OperationFinished(id)
    }
  }

  def handleRemove(pos: Position, requester: ActorRef, id: Int, elm: Int): Unit = {
    if (subtrees.get(pos).isDefined)
      subtrees.get(pos).get ! Remove(requester, id, elm) //pass the msg to subtree
    else
      requester ! OperationFinished(id) //not found
  }

  // optional
  /** `expected` is the set of ActorRefs whose replies we are waiting for,
    * `insertConfirmed` tracks whether the copy of this node to the new tree has been confirmed.
    */
  def copying(expected: Set[ActorRef], insertConfirmed: Boolean): Receive = {

    case CopyFinished =>
      val newExpected = expected - sender
      if (newExpected.isEmpty && insertConfirmed) {
        context.parent ! CopyFinished
        context.become(normal)
      } else {
        context.become(copying(newExpected, insertConfirmed))
      }
    case OperationFinished(_) =>
      if (expected.isEmpty) {
        context.parent ! CopyFinished
        context.become(normal)
      } else {
        context.become(copying(expected, true))
      }

  }


}
