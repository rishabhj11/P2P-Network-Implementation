
import akka.actor.{ActorRef, Actor, Props, ActorSystem}

import scala.collection.immutable
import scala.collection.immutable.HashSet
import scala.math._

/**
 * Created by PRITI on 10/17/15.
 */
object project3 {

  var m: Int = 0;

  def main(args: Array[String]) {

    var ActorMap = new HashSet[ActorRef]
    var numNodes: Int = args(0).toInt;
    var numRequests: Int = args(1).toInt;
    val system = ActorSystem("Chord")
        m = 2 * log2(numNodes)
    //    println(m)
//    m = log2(numNodes)
//    m=3
    var networkIPList: Set[String] = Set();
    var IDList: Set[String] = Set();
    var id: String = ""
    var temp_id: String = ""


    temp_id = getHash(getnetworkIp(networkIPList))
    id = BigInt(temp_id.substring(0, m), 2).toString(10)
    IDList += id
    var firstNode = system.actorOf(Props(new Node()), name = "0")

    ActorMap += firstNode

    for (i <- 1 until numNodes) {
      while (IDList.contains(id)) {
        temp_id = getHash(getnetworkIp(networkIPList))
        id = BigInt(temp_id.substring(0, m), 2).toString(10)
      }
      IDList += id
      var node = system.actorOf(Props(new Node()), name = id)

      ActorMap += node
      Thread.sleep(100)
      node ! Join(firstNode)
    }

//    var node1 = system.actorOf(Props(new Node()), name = "1")
//    ActorMap+=node1
//    var node2 = system.actorOf(Props(new Node()), name = "3")
//    ActorMap+=node2

//    node1 ! Join(firstNode)
//    Thread.sleep(100)
//    node2 ! Join(firstNode)
//    Thread.sleep(1000)

    for (ar <- ActorMap) {
      Thread.sleep(100)
      ar ! Print
    }
  }

  def log2(x: Int): Int = {

    val lnOf2: Double = scala.math.log(2) // natural log of 2
    val newValue = scala.math.log(x) / lnOf2

    return ceil(newValue).toInt
  }

  def getHash(string: String): String = {

    val md = java.security.MessageDigest.getInstance("SHA-1")
    var hexValue = md.digest(string.getBytes("UTF-8")).map("%02x".format(_))
      .mkString

    BigInt(hexValue, 16).toString(2)
  }

  def getnetworkIp(networkIPList: immutable.Set[String]): String = {

    val ip = scala.util.Random.nextInt(256) + "." + scala.util.Random.nextInt(256) + "." + scala.util.Random.nextInt(256) + "." + scala.util.Random.nextInt(256)
    if (!networkIPList.contains(ip))
      ip
    else {
      println("stuck in while")
      getnetworkIp(networkIPList)
    }

  }

  class Node extends Actor {

    var successor = self
    var predecessor = self
    var exist: ActorRef = null
    var fingerTable = new Array[FingerEntry](m)

    for (i <- 0 until m) {
      val start = (self.path.name.toInt + BigInt(2).pow(i)) % (BigInt(2).pow(m))
      val end = (self.path.name.toInt + BigInt(2).pow(i + 1)) % (BigInt(2).pow(m))
      val range = new Interval(true, start, end, false)
      fingerTable(i) = new FingerEntry(start, range, self)
    }

    def receive: Receive = {

      case Join(exist: ActorRef) => {
        println("join started for: " + self.path.name.toInt)
        println("m: " + m)
        this.exist = exist
        //println("I am %S, I am asking %s to find my position".format(getHash,exist.toString().charAt(25)))
        exist ! Find_Predecessor(self, self.path.name.toInt)
      }

      //
      case Find_Predecessor(node: ActorRef, id: Int) => {
        println("Find_Predecessor started")
        val interval = new Interval(false, self.path.name.toInt, fingerTable(0)
          .getHash(),
          true)

        //if the id is contained in the interval
        if (interval.includes(id))
          {
            println("calling: "+self.path.name+" id: "+id)
          node ! Found_Position(self, this.successor)
        //else find the closest preceeding finger and recurse
             }
        else {

          val target = closest_preceding_finger(id)
          println("target: " + target.path.name)
          target ! Find_Predecessor(node, id)
        }
      }

      case Found_Position(predecessor: ActorRef, successor: ActorRef) => {
        this.predecessor = predecessor
        this.successor = successor
        //println("I am %s, I found my position (%s , %s)".format(getHash(),predecessor.toString().charAt(25),successor.toString().charAt(25)))
        predecessor ! Set_Successor(self) //Add by myself
        successor ! Set_Predecessor(self)
        init_fingers()
        update_others()
      }

      case Set_Predecessor(node: ActorRef) => {
        //println("I am %s, I need to set my predecessor to %s".format(getHash(),node.toString().charAt(25)))
        this.predecessor = node
      }

      case Set_Successor(node: ActorRef) => {
        //println("I am %s, I need to set my successor to %s".format(getHash(),node.toString().charAt(25)))
        this.successor = node
      }

      case Find_Finger(node: ActorRef, i: Int, start: BigInt) => {
        val interval = new Interval(false, self.path.name.toInt, fingerTable(0)
          .getHash
          (),
          true)
        if (interval.includes(start)) {
          node ! Found_Finger(i, successor)
        } else {
          val target = closest_preceding_finger(start)
          target ! Find_Finger(node, i, start)
        }
      }

      case Found_Finger(i: Int, successor: ActorRef) => {
        this.fingerTable(i).setNode(successor)
      }

      case Update_Finger(before: BigInt, i: Int, node: ActorRef, nodeHash: BigInt) => {
        if (node != self) {
          val interval1 = new Interval(false, self.path.name.toInt,
            fingerTable(0)
              .getHash(), true)
          if (interval1.includes(before)) {
            //I am the node just before N-2^i
            val interval2 = new Interval(false, self.path.name.toInt,
              fingerTable(i).getHash(), false)
            if (interval2.includes(nodeHash)) {
              //println("I am %s,successor %s the first node before %s, I need to change my %s th finger to %s".format(getHash(),fingerTable(0).getHash(),before,i,nodeHash))
              fingerTable(i).setNode(node)
              //println("I am %s I also ask my predecessor %s to change its %s finger to %s".format(getHash(),predecessor.toString().charAt(25),i,nodeHash))
              predecessor ! Update_Finger(self.path.name.toInt, i, node, nodeHash) //just let my predecessor to check whether its finger at i need be changed
            }
          } else {
            val target = closest_preceding_finger(before)
            //println("I am %s, I am not the first node before %s I am asking %s to update".format(getHash(),before,target.toString().charAt(25)))
            target ! Update_Finger(before, i, node, nodeHash)
          }
        }
      }

      case Print => {
        println("============================================")
        println("Node: %s".format(self.toString()))
        println("Hash: %s".format(self.path.name.toInt))
        println("Predecessor: %s".format(predecessor.path.name))
        println("Successor: %s".format(successor.path.name))
        println("Finger Table: ")
        for (i <- 0 until m) {
          println("   %d : ".format(i) + fingerTable(i).print)
        }
        println("============================================")
      }

    }

    def closest_preceding_finger(id: BigInt): ActorRef = {

      val interval = new Interval(false, self.path.name.toInt, id, false)
      for (i <- m - 1 to 0 by -1) {
        if (interval.includes(fingerTable(i).getHash())) {
          return fingerTable(i).node;
        }
      }
      return self;
    }

    def init_fingers(): Unit = {

      fingerTable(0).setNode(successor)
      for (i <- 0 until m - 1) {
        val interval = new Interval(true, self.path.name.toInt, fingerTable(i).getHash(),
          true)
        if (interval.includes(fingerTable(i + 1).getStart())) {
          // println("I am %s, finger %s: %s is in the range [%s,%s),so we just set its finger node".format(getHash(),i+1,fingerTable(i+1).getStart(),getHash(),fingerTable(i).getHash()))
          fingerTable(i + 1).setNode(fingerTable(i).getNode())
        }
        else {
          //println("I am %s, finger %s: %s is not in the range [%s,%s)".format(getHash(),i+1,fingerTable(i+1).getStart(),getHash(),fingerTable(i).getHash()))
          if (exist != null) {
            exist ! Find_Finger(self, i + 1, fingerTable(i + 1).getStart())
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
        //println("I am %s, I need node before %s to change it's %s th finger".format(getHash(),position,i))
        successor ! Update_Finger(position, i, self, self.path.name.toInt)
      }
    }

  }

  case class Join(exist: ActorRef)

  case class Found_Position(predecessor: ActorRef, successor: ActorRef)

  case class Set_Predecessor(node: ActorRef)

  case class Set_Successor(node: ActorRef)

  case class Find_Predecessor(node: ActorRef, id: Int)

  case class Find_Finger(node: ActorRef, i: Int, start: BigInt)

  case class Found_Finger(i: Int, successor: ActorRef)

  case class Update_Finger(before: BigInt, i: Int, node: ActorRef, nodeHash:
  BigInt)

  case class Print()

}

