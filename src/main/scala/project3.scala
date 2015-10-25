import java.util.concurrent.ConcurrentHashMap

import akka.actor.{Actor, ActorRef, ActorSystem, Props}

import scala.collection._
import scala.collection.convert.decorateAsScala._
import scala.collection.immutable.HashSet
import scala.math._
import scala.util.Random

/**
 * Created by PRITI on 10/17/15 at 2:50 PM.
 */
object project3 {

  val hopMap: concurrent.Map[String, Array[Int]] = new
      ConcurrentHashMap[String, Array[Int]]().asScala
  //key:number of nodes; (Array[0]):number of nodes that searched 'key'
  // (Array[1]): total number of hops

  var m: Int = 0

  def main(args: Array[String]) {

    var ActorMap = new HashSet[ActorRef]
    val numNodes: Int = args(0).toInt
    val numRequests: Int = args(1).toInt
    val system = ActorSystem("Chord")
    m = 2 * log2(numNodes)
    val networkIPList: Set[String] = new scala
    .collection.mutable.HashSet[String]
    var IDList: Set[String] = Set()
    var id: String = ""
    var temp_id: String = ""

    // The first node
    temp_id = getHash(getIP(networkIPList))
    id = BigInt(temp_id.substring(0, m), 2).toString(10)
    IDList += id
    var firstNode = system.actorOf(Props(new Node()), name = "0")

    ActorMap += firstNode

    // Rest of the nodes
    for (i <- 1 until numNodes) {
      while (IDList.contains(id)) {
        temp_id = getHash(getIP(networkIPList))
        id = BigInt(temp_id.substring(0, m), 2).toString(10)
      }
      IDList += id
      var nodeActor = system.actorOf(Props(new Node()), name = id)

      ActorMap += nodeActor
      Thread.sleep(100)
      //      nodeActor ! Join(ActorMap.last)
      nodeActor ! Join(firstNode)
    }

    for (ar <- ActorMap) {
      Thread.sleep(100)
      ar ! Print
    }

    Thread.sleep(1000)
    val numPositions: Int = scala.math.pow(2, m).toInt
    println("Number of Positions on Identifier Circle: " + numPositions)
    println("Number of Nodes: " + numNodes)
    println("Number of Requests: " + numRequests)
    var k: Int = 0

    for (i <- 0 until numRequests) {
      k = Random.nextInt(numPositions)
      for (ar <- ActorMap) {
        ar ! SearchKey(ar, k.toString, 0)
        Thread.sleep(100)
      }
    }

    Thread.sleep(1000)
    for ((k, v) <- hopMap) {
      val avgHops = hopMap.get(k)

      avgHops match {
        case Some(x) =>
          println("=========================================================")
          println("| Number of nodes that searched for key \"" + k + "\": " + x(0))
          println("| Total number of hops: " + x(1))
          println("| Average: " + (x(1).toDouble / x(0).toDouble))
          println("=========================================================")

        case None =>
          println("found none")
      }
    }

    system.shutdown()

  }

  def log2(x: Int): Int = {

    val lnOf2: Double = scala.math.log(2) // natural log of 2
    val newValue = scala.math.log(x) / lnOf2

    ceil(newValue).toInt
  }

  def getHash(string: String): String = {

    val md = java.security.MessageDigest.getInstance("SHA-1")
    val hexValue = md.digest(string.getBytes("UTF-8")).map("%02x".format(_))
      .mkString

    BigInt(hexValue, 16).toString(2)
  }

  def getIP(networkIPList: Set[String]): String = {

    val ip = Random.nextInt(256) + "." + Random.nextInt(256) + "." + Random.nextInt(256) + "." + Random.nextInt(256)
    if (!networkIPList.contains(ip)) ip
    else getIP(networkIPList)
  }

  class Node extends Actor {

    var successor = self
    var predecessor = self
    var nDash: ActorRef = null
    var fingerTable = new Array[FingerEntry](m)

    for (i <- 0 until m) {
      val start = (self.path.name.toInt + scala.math.pow(2, i).toInt) % scala
        .math.pow(2, m).toInt
      val end = (self.path.name.toInt + scala.math.pow(2, i + 1).toInt) %
        scala.math.pow(2, m).toInt
      fingerTable(i) = new FingerEntry(start, end, self)
    }

    def receive: Receive = {

      case Join(nDash: ActorRef) =>
        this.nDash = nDash
        nDash ! Find_Predecessor(self, self.path.name.toInt)

      case Find_Successor(nodeActor: ActorRef, id: Int) =>
        self ! Find_Predecessor(nodeActor, id)

      case Find_Predecessor(nodeActor: ActorRef, id: Int) =>
        //if the id is contained in the interval
        if (isIncluded("n", self.path.name.toInt, fingerTable(0).getHash(),
          "y", id)) {

          //          nodeActor ! Set_Predecessor(self)
          //          nodeActor ! Set_Successor(this.successor)
          nodeActor ! Join_Continue(self, this.successor)
        }
        //else find the closest preceding finger and recurse
        else {
          val nextFinger = closest_preceding_finger(id)
          nextFinger ! Find_Predecessor(nodeActor, id)
        }

      case Join_Continue(predecessor: ActorRef, successor: ActorRef) =>
        this.predecessor = predecessor
        this.successor = successor
        predecessor ! Set_Successor(self)
        successor ! Set_Predecessor(self)
        init_finger_table()
        update_others()

      case Set_Predecessor(nodeActor: ActorRef) =>
        this.predecessor = nodeActor

      case Set_Successor(nodeActor: ActorRef) =>
        this.successor = nodeActor

      case Find_Finger(nodeActor: ActorRef, i: Int, start: Int) =>
        if (isIncluded("n", self.path.name.toInt, fingerTable(0).getHash(),
          "y", start)) {
          nodeActor ! Found_Finger(i, successor)
        } else {
          val nextFinger = closest_preceding_finger(start)
          nextFinger ! Find_Finger(nodeActor, i, start)
        }

      case Found_Finger(i: Int, successor: ActorRef) =>
        this.fingerTable(i).setNode(successor)

      case update_finger_table(before: Int, i: Int, nodeActor: ActorRef, nodeID: Int) =>
        if (nodeActor != self) {
          if (isIncluded("n", self.path.name.toInt, fingerTable(0).getHash()
            , "y", before)) {
            if (isIncluded("n", self.path.name.toInt, fingerTable(i).getHash
            (), "y", nodeID)) {
              fingerTable(i).setNode(nodeActor)
              predecessor ! update_finger_table(self.path.name.toInt, i, nodeActor, nodeID)
            }
          } else {
            val nextFinger = closest_preceding_finger(before)
            nextFinger ! update_finger_table(before, i, nodeActor, nodeID)
          }
        }

      case SearchKey(nodeActor: ActorRef, code: String, hops: Int) =>
        if (isIncluded("n", self.path.name.toInt, fingerTable(0)
          .getHash(), "y", code.toInt)) {
          var arrayMap: Array[Int] = null
          val result = hopMap.get(code)

          result match {
            case None =>
              arrayMap = new Array[Int](2)
              arrayMap(0) = 1
              arrayMap(1) = hops
              hopMap.put(code, arrayMap)

            case Some(x) =>
              x(0) += 1
              x(1) += hops
              hopMap.put(code, x)
          }

          println("I am nodeActor: " + nodeActor.path.name + "; Found key: " + code + " at" +
            " " + "nodeActor " + successor.path.name + " in" + " " + "" + hops + " hops")
        } else {
          val nextFinger = closest_preceding_finger(code.toInt)
          nextFinger ! SearchKey(nodeActor, code, hops + 1)
        }

      case Print => {
        println("============================================")
        println("Node Identifier (SHA1): %s".format(self.path.name.toInt))
        println("Predecessor: %s".format(predecessor.path.name))
        println("Successor: %s".format(successor.path.name))
        println("Finger Table: ")
        for (i <- 0 until m) {
          println("   %d : ".format(i) + fingerTable(i).print)
        }
        println("============================================")
      }

    }

    def isIncluded(l_close: String, intStart: Int, intEnd: Int, r_close: String, value: Int): Boolean = {

      if (intStart > intEnd)
        if (value == intStart && (l_close.equals("y")) || value == intEnd && (r_close.equals("y")) || (value > intStart || value < intEnd))
          return true
      if (intStart < intEnd)
        if (value == intStart && (l_close.equals("y")) || value == intEnd && (r_close.equals("y")) || (value > intStart && value < intEnd))
          return true
      if (intStart == intEnd) {
        if (l_close.equals("n") && r_close.equals("n") && value == intStart)
          return false
        else
          return true
      }

      false
    }

    def closest_preceding_finger(id: Int): ActorRef = {

      for (i <- m - 1 to 0 by -1) {
        if (isIncluded("n", self.path.name.toInt, id, "n",
          fingerTable(i).getHash())) {
          return fingerTable(i).nodeActor
        }
      }
      self
    }

    def init_finger_table(): Unit = {

      fingerTable(0).setNode(successor)
      for (i <- 0 until m - 1) {
        if (isIncluded("y", self.path.name.toInt, fingerTable(i).getHash(),
          "y", fingerTable(i + 1).getStart())) {
          fingerTable(i + 1).setNode(fingerTable(i).getNode())
        }
        else {
          if (nDash != null) {
            nDash ! Find_Finger(self, i + 1, fingerTable(i + 1).getStart())
          }
        }
      }
    }

    def update_others(): Unit = {

      for (i <- 0 to m - 1) {
        val p = (self.path.name.toInt - scala.math.pow(2, i).toInt +
          scala.math.pow(2, m).toInt + 1) % scala.math.pow(2, m).toInt
        successor ! update_finger_table(p, i, self, self.path.name.toInt)
      }
    }

  }

  case class Join(nDash: ActorRef)

  case class Join_Continue(predecessor: ActorRef, successor: ActorRef)

  case class Set_Predecessor(nodeActor: ActorRef)

  case class Set_Successor(nodeActor: ActorRef)

  case class Find_Predecessor(nodeActor: ActorRef, id: Int)

  case class Find_Finger(nodeActor: ActorRef, i: Int, start: Int)

  case class Found_Finger(i: Int, successor: ActorRef)

  case class update_finger_table(
                                  before: Int, i: Int, nodeActor: ActorRef,
                                  nodeID: Int)

  case class SearchKey(nodeActor: ActorRef, code: String, hops: Int)

  case class Find_Successor(nodeActor: ActorRef, id: Int)

  case class Print()

}

