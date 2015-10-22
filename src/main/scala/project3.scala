
import akka.actor.{Actor, Props, ActorSystem}

import scala.collection.immutable
import scala.math._

/**
 * Created by PRITI on 10/17/15.
 */
object project3 {

  def main(args: Array[String]) {

    var numNodes: Int = args(0).toInt;
    var numRequests: Int = args(1).toInt;
    val system = ActorSystem("Chord")
    val m = 2 * log2(numNodes)
    println(m)
    var networkIPList: Set[String] = Set();
    var id: String = ""
    var temp_id: String = ""

    temp_id = getHash(getnetworkIp(networkIPList))
    id = BigInt(temp_id.substring(0, m), 2).toString(10)
    //      println(id)
    var node = system.actorOf(Props(new Node(id, id)), name = "Node"+id)
    var forNextNode = id

    for (i <- 2 to numNodes) {
      temp_id = getHash(getnetworkIp(networkIPList))
      id = BigInt(temp_id.substring(0, m), 2).toString(10)
//      println(id)
      var node = system.actorOf(Props(new Node(id, forNextNode)), name = "Node"+id)
      forNextNode = id
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

  class Node (id:String, responsibleNode:String) extends Actor {


    var alive:Boolean = false
    var NodeIdentifier = id

    def receive: Receive = {

      case join => {

      }

      case find_successor =>{}



    }
      def join(): Unit =
      {

      }
  }

}

