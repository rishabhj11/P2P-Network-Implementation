import akka.actor.{ActorRef, Actor, ActorSystem, Props}

import scala.collection.mutable

/**
 * Created by PRITI on 10/17/15.
 */
object project3 {

  def main(args: Array[String]) {

    var numNodes: Int = args(0).toInt;
    var numRequests: Int = args(1).toInt;
    val system = ActorSystem("Chord")

    var master = system.actorOf(Props(new Master(numNodes, numRequests)));
    master ! START

  }

  class Master(numNodes: Int, numRequests: Int) extends Actor {

    def receive = {

      case START() =>
        var node = context.actorOf(Props(new Node(numNodes, 0)), name = "Node"
          + 0)
        node ! Create

    }

  }

  class Node(numNodes: Int, id: Int) extends Actor {

    var NodeIdentifier: Int = 0
    NodeIdentifier = id
    var predessor: ActorRef = null
    var successor: ActorRef = sender
    var fingerTable = new mutable.HashMap[Int, Int] //start and successor
    //    var key = new mutable.HashSet[Int] {}

    def receive = {

      case Create =>
//        successor = sender
        for (i: Int <- 1 until numNodes) {
          var newNode = context.actorOf(Props(new Node(numNodes, i)))
           newNode ! Join
          successor = newNode
        }

      case Join =>
        predessor = sender
        successor = find_successor(NodeIdentifier)
    }

    def find_successor(nID: Int): ActorRef = {

      return
      //      return finger[1].node;
    }

  }

  case class Create()

  case class Join()

  case class START()

}

