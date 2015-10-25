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
      ConcurrentHashMap[String,
        Array[Int]]().asScala;
  //key:number of nodes; (Array[0]):number of nodes that searched 'key'
  // (Array[1]): total number of hops

  var m: Int = 0;

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
      var node = system.actorOf(Props(new Node()), name = id)

      ActorMap += node
      Thread.sleep(100)
      //      node ! Join(ActorMap.last)
      node ! Join(firstNode)
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
    for (ar <- ActorMap) {
      for (i <- 0 until numRequests) {
        k = Random.nextInt(numPositions)
        ar ! Find(ar, k.toString, 0)
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
      val start = (self.path.name.toInt + BigInt(2).pow(i)) % BigInt(2).pow(m)
      val end = (self.path.name.toInt + BigInt(2).pow(i + 1)) % BigInt(2)
        .pow(m)
//      val interval = new Interval(true, start, end, false)
      fingerTable(i) = new FingerEntry(start, end, self)
    }

    def receive: Receive = {

      case Join(nDash: ActorRef) =>
        this.nDash = nDash
        nDash ! Find_Predecessor(self, self.path.name.toInt)

      case Find_Predecessor(node: ActorRef, id: BigInt) =>
//        val interval = new Interval(false, self.path.name.toInt, fingerTable(0)
//          .getHash(),
//          true)

        //if the id is contained in the interval
        if (isIncluded(false, self.path.name.toInt, fingerTable(0)
          .getHash(),
          true,id))
          node ! Found_Position(self, this.successor)
        //else find the closest preceding finger and recurse
        else {
          val target = closest_preceding_finger(id)
          target ! Find_Predecessor(node, id)
        }

      case Found_Position(predecessor: ActorRef, successor: ActorRef) =>
        this.predecessor = predecessor
        this.successor = successor
        predecessor ! Set_Successor(self)
        successor ! Set_Predecessor(self)
        init_fingers()
        update_others()

      case Set_Predecessor(node: ActorRef) =>
        this.predecessor = node

      case Set_Successor(node: ActorRef) =>
        this.successor = node

      case Find_Finger(node: ActorRef, i: Int, start: BigInt) =>
//        val interval = new Interval(false, self.path.name.toInt, fingerTable(0)
//          .getHash
//          (),
//          true)
        if (isIncluded(false, self.path.name.toInt, fingerTable(0)
          .getHash
          (),
          true, start)) {
          node ! Found_Finger(i, successor)
        } else {
          val target = closest_preceding_finger(start)
          target ! Find_Finger(node, i, start)
        }

      case Found_Finger(i: Int, successor: ActorRef) =>
        this.fingerTable(i).setNode(successor)

      case Update_Finger(before: BigInt, i: Int, node: ActorRef, nodeHash: BigInt) =>
        if (node != self) {
//          val interval1 = new Interval(false, self.path.name.toInt,
//            fingerTable(0)
//              .getHash(), true)
          if (isIncluded(false, self.path.name.toInt,
            fingerTable(0)
              .getHash(), true, before)) {
//            val interval2 = new Interval(false, self.path.name.toInt,
//              fingerTable(i).getHash(), false)
            if (isIncluded(false, self.path.name.toInt,
              fingerTable(i).getHash(), false, nodeHash)) {
              fingerTable(i).setNode(node)
              predecessor ! Update_Finger(self.path.name.toInt, i, node, nodeHash)
            }
          } else {
            val target = closest_preceding_finger(before)
            target ! Update_Finger(before, i, node, nodeHash)
          }
        }

      case Find(node: ActorRef, code: String, hops: Int) =>
//        val interval = new Interval(false, self.path.name.toInt, fingerTable(0)
//          .getHash(), true)

        if (isIncluded(false, self.path.name.toInt, fingerTable(0)
          .getHash(), true, code.toInt)) {
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

          //          node ! Found(code, self, successor, self.path.name.toInt, fingerTable(0).getHash(), hops)
          println("I am node: "+node.path.name+"; Found key: " + code + " at" +
            " " +
            "node " + successor
            .path.name + " in" +
            " " +
            "" + hops + " hops")
        } else {
          val target = closest_preceding_finger(code.toInt)
          target ! Find(node, code, hops + 1)
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

    def isIncluded(leftInclude: Boolean, leftValue: BigInt, rightValue: BigInt,
                 rightInclude: Boolean, value: BigInt): Boolean = {

      if (leftValue == rightValue) {
        if (!leftInclude && !rightInclude && value == leftValue)
          false
        else
          true
      }
      else if (leftValue < rightValue) {
        if (value == leftValue && leftInclude || value == rightValue && rightInclude || (value > leftValue && value < rightValue))
          true
        else
          false
      }
      else {
        if (value == leftValue && leftInclude || value == rightValue && rightInclude || (value > leftValue || value < rightValue))
          true
        else
          false
      }

    }

    def closest_preceding_finger(id: BigInt): ActorRef = {

//      val interval = new Interval(false, self.path.name.toInt, id, false)
      for (i <- m - 1 to 0 by -1) {
        if (isIncluded(false, self.path.name.toInt, id, false,
          fingerTable(i).getHash())) {
          return fingerTable(i).node
        }
      }
      self
    }

    def init_fingers(): Unit = {

      fingerTable(0).setNode(successor)
      for (i <- 0 until m - 1) {
//        val interval = new Interval(true, self.path.name.toInt, fingerTable(i).getHash(),
//          true)
        if (isIncluded(true, self.path.name.toInt, fingerTable(i).getHash(),
          true, fingerTable(i + 1).getStart())) {
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
        val position = (self.path.name.toInt - BigInt(2).pow(i) + BigInt(2).pow
        (m) +
          1) %
          BigInt(2).pow(m)
        successor ! Update_Finger(position, i, self, self.path.name.toInt)
      }
    }

  }

  case class Join(nDash: ActorRef)

  case class Found_Position(predecessor: ActorRef, successor: ActorRef)

  case class Set_Predecessor(node: ActorRef)

  case class Set_Successor(node: ActorRef)

  case class Find_Predecessor(node: ActorRef, id: BigInt)

  case class Find_Finger(node: ActorRef, i: Int, start: BigInt)

  case class Found_Finger(i: Int, successor: ActorRef)

  case class Update_Finger(
                            before: BigInt, i: Int, node: ActorRef,
                            nodeHash:
  BigInt)

  case class Find(node: ActorRef, code: String, hops: Int)

  case class Print()

}

