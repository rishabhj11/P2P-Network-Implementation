import akka.actor.{Actor, ActorRef, ActorSystem, Props}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.math._
import scala.util.Random
import ExecutionContext.Implicits.global

/**
 * Created by PRITI on 10/1/15.
 */
/**
 *
 */
object Project2 {

  var disqualifiedNeighbors = new ArrayBuffer[Int]() with mutable
  .SynchronizedBuffer[Int]

  def main(args: Array[String]) {

    var numNodes = args(0).toInt
    val topology: String = args(1)
    val algorithm: String = args(2)

    if (topology.equals("3D") || topology.equals("imp3D")) {
      numNodes = ceil(cbrt(numNodes)).toInt
      numNodes = numNodes * numNodes * numNodes
      println("number of nodes: " + numNodes)
    }

    val system = ActorSystem("GossipSimulator")
    val Master: ActorRef = system.actorOf(Props(new Master(numNodes,
      topology, algorithm)), name = "Master")

    Master ! START
  }

  /**
   *
   * @param numNodes : Number of nodes in the network
   * @param topology : Type of network (full, line, 3D, imp3D)
   * @param algorithm : Type of algorithm (gossip, push-sum)
   */
  class Master(numNodes: Int, topology: String, algorithm: String) extends
  Actor {

    var NodeActors: ArrayBuffer[ActorRef] = new ArrayBuffer[ActorRef]()
    var NodeActorsCompleted: ArrayBuffer[ActorRef] = new
        ArrayBuffer[ActorRef]()
    var counter: Int = 0
    for (i <- 0 until numNodes) {
      NodeActors += context.actorOf(Props(new GossipActor(i, self)), name =
        "NodeActor" + i)
    }
    var Neighbors: ArrayBuffer[ActorRef] = new ArrayBuffer[ActorRef]()
    if (topology.equals("full")) {
      for (i <- 0 until NodeActors.length) {
        for (j <- 0 until NodeActors.length)
          if (i != j)
            Neighbors += NodeActors(j)

        NodeActors(i) ! SetNeighborList(Neighbors)
        //              println(Neighbors)
        Neighbors = new ArrayBuffer[ActorRef]()
      }
    }

    if (topology.equals("line")) {
      Neighbors += NodeActors(1)
      NodeActors(0) ! SetNeighborList(Neighbors)

      Neighbors = new ArrayBuffer[ActorRef]()

      Neighbors += NodeActors(NodeActors.length - 2)
      NodeActors(NodeActors.length - 1) ! SetNeighborList(Neighbors)
      Neighbors = new ArrayBuffer[ActorRef]()

      for (i <- 1 until NodeActors.length - 1) {
        Neighbors +=(NodeActors(i - 1), NodeActors(i + 1))
        NodeActors(i) ! SetNeighborList(Neighbors)
        //      println(Neighbors)
        Neighbors = new ArrayBuffer[ActorRef]()
      }
    }

    if (topology.equals("3D")) {
      val sideIndex: Int = cbrt(NodeActors.length).toInt

      val nodeArray = Array.ofDim[ActorRef](sideIndex, sideIndex, sideIndex)
      var len: Int = 0

      for (i: Int <- 0 until sideIndex)
        for (j: Int <- 0 until sideIndex)
          for (k: Int <- 0 until sideIndex) {
            nodeArray(i)(j)(k) = NodeActors(len)
            len += 1
          }

      len = 0
      for (i: Int <- 0 until sideIndex)
        for (j: Int <- 0 until sideIndex)
          for (k: Int <- 0 until sideIndex) {
            //top,bottom,left,right,front,back

            if (!(i - 1 < 0))
              Neighbors += nodeArray(i - 1)(j)(k)

            if (!(i + 1 >= sideIndex))
              Neighbors += nodeArray(i + 1)(j)(k)

            if (!(j - 1 < 0))
              Neighbors += nodeArray(i)(j - 1)(k)

            if (!(j + 1 >= sideIndex))
              Neighbors += nodeArray(i)(j + 1)(k)

            if (!(k - 1 < 0))
              Neighbors += nodeArray(i)(j)(k - 1)

            if (!(k + 1 >= sideIndex))
              Neighbors += nodeArray(i)(j)(k + 1)


            //          println(Neighbors)
            NodeActors(len) ! SetNeighborList(Neighbors)
            len += 1
            Neighbors = new ArrayBuffer[ActorRef]()
          }
    }

    if (topology.equals("imp3D")) {
      val sideIndex: Int = cbrt(NodeActors.length).toInt

      val nodeArray = Array.ofDim[ActorRef](sideIndex, sideIndex, sideIndex)
      var len: Int = 0

      for (i: Int <- 0 until sideIndex)
        for (j: Int <- 0 until sideIndex)
          for (k: Int <- 0 until sideIndex) {
            nodeArray(i)(j)(k) = NodeActors(len)
            len += 1
          }

      len = 0
      for (i: Int <- 0 until sideIndex)
        for (j: Int <- 0 until sideIndex)
          for (k: Int <- 0 until sideIndex) {
            //top,bottom,left,right,front,back

            if (!(i - 1 < 0))
              Neighbors += nodeArray(i - 1)(j)(k)

            if (!(i + 1 >= sideIndex))
              Neighbors += nodeArray(i + 1)(j)(k)

            if (!(j - 1 < 0))
              Neighbors += nodeArray(i)(j - 1)(k)

            if (!(j + 1 >= sideIndex))
              Neighbors += nodeArray(i)(j + 1)(k)

            if (!(k - 1 < 0))
              Neighbors += nodeArray(i)(j)(k - 1)

            if (!(k + 1 >= sideIndex))
              Neighbors += nodeArray(i)(j)(k + 1)

            var m: Int = Random.nextInt(NodeActors.length)

            while (Neighbors.contains(NodeActors(m)) || m == len) {
              m = Random.nextInt(NodeActors.length)
            }
            //            println(m+" "+len)
            Neighbors += NodeActors(m)
            //            println(Neighbors)
            NodeActors(len) ! SetNeighborList(Neighbors)
            len += 1
            Neighbors = new ArrayBuffer[ActorRef]()
          }
    }
    val b = System.currentTimeMillis;

    override def receive: Receive = {

      case START => {
        if (algorithm == "gossip") {
          val i: Int = Random.nextInt(NodeActors.length)
          NodeActors(i) ! KeepGossiping
          println("Gossip started")
        }
        else {
          val i: Int = Random.nextInt(NodeActors.length)
          NodeActors(i) ! StartPushSum(0, 1)
          println("Push-Sum started")
        }
      }

      case KillNode =>
        counter += 1
        //        println(counter)
        if (counter == NodeActors.length) {
          println("Time in milliseconds: " + (System.currentTimeMillis - b))
          context.system.shutdown()
        }

      case KillIt =>
        println("Time in milliseconds: " + (System.currentTimeMillis - b))
        context.system.shutdown()
    }
  }

  /**
   *
   * @param id : Unique ID for each Actor
   * @param Master : Master Actor
   */
  class GossipActor(id: Int, Master: ActorRef) extends Actor {

    var neighborList = new ArrayBuffer[ActorRef]()
    val ID: Int = id
    var done: Boolean = false
    var calledAtLeastOnce: Boolean = false

    var rumorCount: Int = 0
    var maxRumorCount = 10
    var s: Double = ID
    var w: Double = 0.0
    var count: Int = 0
    var naiveCounter: Int = 0
    var previousSW: Double = s / w
    var currentSW: Double = s / w

    override def receive: Receive = {

      case SetNeighborList(neighborList: ArrayBuffer[ActorRef]) =>
        this.neighborList = neighborList

      //    case StartGossiping =>
      //      rumorCount += 1
      //      val i: Int = Random.nextInt(neighborList.length)
      //      neighborList(i) ! KeepGossiping(sender)
      //
      //    case KeepGossiping(parent) =>
      //      if (rumorCount == 0) {
      //        rumorCount += 1
      //        val i: Int = Random.nextInt(neighborList.length)
      //        neighborList(i) ! KeepGossiping(parent)
      //        sender ! CheckGossipStatus(parent)
      //      }
      //      else if (rumorCount == maxRumorCount) {
      //        //        println("found the match")
      //        sender ! DisqualifyNeighbor(ID)
      //        //        println("check parent's status")
      //        sender ! CheckGossipStatus(parent)
      //      }
      //      else if (rumorCount < maxRumorCount) {
      //        rumorCount += 1
      //        //                println("Node " + ID + "'s rumor count: " +
      // rumorCount)
      //        sender ! CheckGossipStatus(parent)
      //      }
      //
      //    case CheckGossipStatus(node) =>
      //      if ((disqualifiedNeighbors.length == neighborList.length) ||
      // rumorCount == maxRumorCount) {
      //        node ! KillNode
      //        //        println("Killed node: " + ID + " with count: " +
      // rumorCount)
      //      }
      //      if (rumorCount < maxRumorCount && disqualifiedNeighbors.length <
      // neighborList.length) {
      //        val i: Int = Random.nextInt(neighborList.length)
      //        neighborList(i) ! KeepGossiping(node)
      //      }
      //
      //    case DisqualifyNeighbor(nodeID: Int) =>
      //      if (!disqualifiedNeighbors.contains(nodeID)) {
      //        //        println(sender.path.name + " is a disqualified
      // neighbor for NodeActor" + ID)
      //        disqualifiedNeighbors += nodeID
      //      }

      case KeepGossiping =>
        if (rumorCount < maxRumorCount && !done && (neighborList.length !=
          0)) {
          rumorCount += 1
          //          println("actor running: " + self.path.name + "
          // rumorcount: "
          //            + rumorCount + " done? " + done + " neighbor length:
          // " +
          //            neighborList
          //            .length + " called by: " + sender.path.name)
          val i: Int = Random.nextInt(neighborList.length)
          //          println("random value: " + i)
          neighborList(i) ! KeepGossiping
          context.system.scheduler.scheduleOnce(10 milliseconds, self,
            KeepGossiping)
          //          self ! KeepGossiping
        }
        else if (!calledAtLeastOnce) {
          //          println(ID + " called by: " + sender.path.name)
          done = true
          calledAtLeastOnce = true
          //          println("setting done true for: " + ID)
          if (neighborList.length != 0)
            for (eachActor <- neighborList)
              eachActor ! Remove

          //          println("killed: " + ID)
          Master ! KillNode

        }
      case Remove =>
        //        println("removing: " + sender.path.name + " from its
        // neighbors: " + ID)
        neighborList = neighborList - sender
        if (neighborList.length <= 0) {
          done = true
          //          println("from remove setting done true for: " + ID)
          //          Master ! KillNode
        }

      case StartPushSum(s1: Double, w1: Double) =>

        naiveCounter += 1
        previousSW = s / w

        s += s1
        w += w1

        currentSW = s / w

        s = s / 2
        w = w / 2

        if (naiveCounter == 1 || Math.abs(currentSW - previousSW) > math.pow
        (10, -10)) {
          count = 0
          val i = Random.nextInt(neighborList.length);
          neighborList(i) ! StartPushSum(s, w)
        }
        else {
          count += 1
          if (count >= 3) {
            Master ! KillIt
          }
          else {
            val i = Random.nextInt(neighborList.length);
            neighborList(i) ! StartPushSum(s, w)
          }
        }
    }
  }

}

case class SetNeighborList(neighborList: ArrayBuffer[ActorRef])

case class StartGossiping()

case class KeepGossiping(node: ActorRef)

case class CheckGossipStatus(node: ActorRef)

case class DisqualifyNeighbor(nodeID: Int)

case class StartPushSum(s: Double, w: Double)

case class START()

case class KillNode()

case class KillIt()

case class Remove()