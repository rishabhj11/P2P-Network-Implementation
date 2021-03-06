import java.util.concurrent._
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import akka.util.Timeout
import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.pattern.ask

import scala.collection._
import scala.collection.convert.decorateAsScala._
import scala.collection.immutable.HashSet
import scala.math._
import scala.util.Random

/**
 * Created by PRITI on 10/17/15 at 2:50 PM.
 */
object project3bonus {

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

    //    for (ar <- ActorMap) {
    //      Thread.sleep(100)
    //      ar ! Print
    //    }

    // This would kill(shut down) a node, the system will hold and will implement fault tolerance
    Thread.sleep(1000)
    var ar:ActorRef=ActorMap.last
    println("Killed "+ ar.path.name.toInt)
     ar ! kill()



    Thread.sleep(1000)
    val numPositions: Int = scala.math.pow(2, m).toInt
    //    println("Number of Positions on Identifier Circle: " + numPositions)
    println("Number of Nodes: " + numNodes)
    println("Number of Requests: " + numRequests)
    var k: Int = 0

    for (i <- 0 until numRequests) {
      k = Random.nextInt(numPositions)
      for (ar <- ActorMap) {
        ar ! SearchKey(ar, k.toString, 0)
        Thread.sleep(10)
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
    var next:Int=0
    var r:Int=m // Ideal value for r(length of successor list) would be 2*log(NumofNodes)
    var successorList= ArrayBuffer[Int]()

    // Initialize the Finger Tables with self as soon as created
    for (i <- 0 until m) {
      val start = (self.path.name.toInt + scala.math.pow(2, i).toInt) % scala
        .math.pow(2, m).toInt
      val end = (self.path.name.toInt + scala.math.pow(2, i + 1).toInt) %
        scala.math.pow(2, m).toInt
      fingerTable(i) = new FingerEntry(start, end, self)
    }

    // Periodically runs and checks if the immediate successor is alive, if not update the next successor
    // from the successor list
    val ex1 = new ScheduledThreadPoolExecutor(1)
    val task1 = new Runnable {
      def run() = updateSuccessorIfFailed()
    }
    val f1 = ex1.scheduleAtFixedRate(task1, 1, 1, TimeUnit.SECONDS)
    f1.cancel(false)

    // Periodically runs and update finger entries, this is spercifically useful in case a node has failed.
    val ex2 = new ScheduledThreadPoolExecutor(1)
    val task2 = new Runnable {
      def run() = fix_fingers()
    }
    val f2 = ex2.scheduleAtFixedRate(task2, 2, 2, TimeUnit.SECONDS)
    f2.cancel(false)


    def closest_preceding_finger(id: Int): ActorRef = {

      for (i <- m - 1 to 0 by -1)
        if (isIncluded("n", self.path.name.toInt, id, "n",
          fingerTable(i).getNodeID()))
          return fingerTable(i).nodeActor
      //else just return self
      self
    }

    def receive: Receive = {

      case Find_Successor(nodeActor: ActorRef, id: Int) =>
        self ! Find_Predecessor(nodeActor, id)

      case Find_Predecessor(nodeActor: ActorRef, id: Int) =>
        //if the id is contained in the interval
        if (isIncluded("n", self.path.name.toInt, fingerTable(0).getNodeID(),
          "y", id)) {
          nodeActor ! Set_Predecessor(self) // set n's predecessor
          nodeActor ! Set_Successor(this.successor) // set n's successor
          nodeActor ! Join_Continue(self, this.successor) // continue join
        }
        //else find the closest preceding finger and recurse
        else {
          val nextFinger = closest_preceding_finger(id)
          nextFinger ! Find_Predecessor(nodeActor, id)
        }

      case Join(nDash: ActorRef) =>
        this.nDash = nDash
        nDash ! Find_Predecessor(self, self.path.name.toInt)

      case Join_Continue(predecessor: ActorRef, successor: ActorRef) =>
        predecessor ! Set_Successor(self) // let n' set its successor
        successor ! Set_Predecessor(self) // let n' set its predecessor
        // join has been done, update the initial finger tables
        init_finger_table()
        // update other nodes about it
        update_others()

      case Set_Predecessor(nodeActor: ActorRef) =>
        this.predecessor = nodeActor

      case Set_Successor(nodeActor: ActorRef) =>
        this.successor = nodeActor

      case Find_Successor1(nodeActor: ActorRef, i: Int, start: Int) =>
        if (isIncluded("n", self.path.name.toInt, fingerTable(0).getNodeID(),
          "y", start)) {
          // if it belongs to the interval then set it as the node
          nodeActor ! Set_FNode(i, successor)
        } else {
          // else find the closest to the target
          val nextFinger = closest_preceding_finger(start)
          nextFinger ! Find_Successor1(nodeActor, i, start)
        }

      case Set_FNode(i: Int, successor: ActorRef) =>
        this.fingerTable(i).setNode(successor)

      case Update_Finger_Table(nodeActor: ActorRef, nodeID: Int, pvalue:
        Int, i: Int) =>
        if (nodeActor != self) {
          if (isIncluded("n", self.path.name.toInt, fingerTable(0).getNodeID()
            , "y", pvalue)) {
            if (isIncluded("n", self.path.name.toInt, fingerTable(i).getNodeID
            (), "y", nodeID)) {
              fingerTable(i).setNode(nodeActor)
              predecessor ! Update_Finger_Table(nodeActor, nodeID, self
                .path.name.toInt, i)
            }
          } else {
            val nextFinger = closest_preceding_finger(pvalue)
            nextFinger ! Update_Finger_Table(nodeActor, nodeID, pvalue, i)
          }
        }

      //Determines successor for Update_SuccessorList function
      case successorElement(node: ActorRef, id: Int) => {
        if (isIncluded("n", self.path.name.toInt, fingerTable(0).getNodeID(),"y",id)){
          node ! successorElement_continues(self, self.path.name.toInt)
        } else {
          val target = closest_preceding_finger(id)
          target ! successorElement(node, id)
        }
      }

      case successorElement_continues(successor: ActorRef,id: Int) => {
        successorList+=id
        //
      }

      // Determines the successors for the fix_finger function
      case successor_fixFinger(node: ActorRef, id: Int) => {// calculates successor for fix_fingers function
        if (isIncluded("n", self.path.name.toInt, fingerTable(0).getNodeID(),"y",id)){
          node ! fix_fingers_continues(self, self.path.name.toInt)
        } else {
          val target = closest_preceding_finger(id)
          target ! successor_fixFinger(node, id)
        }
      }

      case fix_fingers_continues(successor: ActorRef,id: Int) => {
        fingerTable(next).setNode(successor)
        next=next+1
        fix_fingers()
      }

      case SearchKey(nodeActor: ActorRef, code: String, hops: Int) =>

        if (isIncluded("n", self.path.name.toInt, fingerTable(0)
          .getNodeID(), "y", code.toInt)) {
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
        } else {
          val nextFinger = closest_preceding_finger(code.toInt)
          nextFinger ! SearchKey(nodeActor, code, hops + 1)
        }

      case kill() => {context.system.shutdown()}


      case iamalive() => {sender ! "yes"}

    }

    def init_finger_table(): Unit = {

      // set finger[1].node = successor
      fingerTable(0).setNode(successor)
      for (i <- 0 until m - 1) {
        // if (finger[i+1].start belongs to [n, finger[i].node))
        if (isIncluded("y", self.path.name.toInt, fingerTable(i).getNodeID(),
          "n", fingerTable(i + 1).getStart())) {
          // then set its node
          fingerTable(i + 1).setNode(fingerTable(i).getNode())
        }
        else {
          if (nDash != null) {
            // find the successor of the start and then call Set_FNode
            nDash ! Find_Successor1(self, i + 1, fingerTable(i + 1).getStart())
          }
        }
      }
    }

    def isIncluded(l_close: String, intStart: Int, intEnd: Int, r_close: String, value: Int): Boolean = {

      if (intStart > intEnd)
        if (value == intStart && l_close.equals("y") || value == intEnd && r_close.equals("y") || (value > intStart || value < intEnd))
          return true
      if (intStart < intEnd)
        if (value == intStart && l_close.equals("y") || value == intEnd && r_close.equals("y") || (value > intStart && value < intEnd))
          return true
      if (intStart == intEnd) {
        if (!(l_close.equals("n") && r_close.equals("n") && value == intStart))
          return true
      }
      false
    }

    def update_others(): Unit = {

      for (i <- 0 to m - 1) {
        val p = (self.path.name.toInt - scala.math.pow(2, i).toInt +
          scala.math.pow(2, m).toInt + 1) % scala.math.pow(2, m).toInt
        successor ! Update_Finger_Table(self, self.path.name.toInt, p, i)
      }
    }

    //Creates and updates a successor list for each node, number of successors is chosen by the formula r= 2*log(numOfNodes)
    def Update_SuccessorList() : Unit ={
      successorList+=((fingerTable(0).getNodeID).toInt)
      var lastSuccessor:Int = successorList(0);

      for (j<-1 until r) {
        var id:Int = lastSuccessor;
        var path="akka://Chord/user/"+id.toString
        var awaitTimeout=Duration(100, "millis")
        var t:ActorRef=Await.result(context.actorSelection(path).resolveOne()(100), awaitTimeout)
        nDash ! successorElement(t,id);
      }
    }

    // Periodically runs and checks if the immediate successor is alive, if not update the next successor
    // from the successor list
    def updateSuccessorIfFailed() : Unit = {
      var i:Int=0
      while(!this.isAlive(this.successor) && i<r){
        successorList.remove(i)
        var t=successorList(i+1)
        var path = "akka://Chord/user/"+t.toString
        var awaitTimeout=Duration(100, "millis")
        val actorRef = Await.result(context.actorSelection(path).resolveOne()(100), awaitTimeout)
        this.successor= actorRef
      }
      this.successor ! Set_Predecessor(self)
    }

    //Runs periodically to update fingers in case network change, for example, node failure
    def fix_fingers():Unit={
      println("Fix fingers called at "+self.path.name.toInt)
      if (next>scala.math.pow(2, m).toInt){
        next=1
      }
      var x:Int=self.path.name.toInt+scala.math.pow(2, next-1).toInt
      var path="akka://Chord/user/"+x.toString
      var awaitTimeout=Duration(100, "millis")
      var t:ActorRef=Await.result(context.actorSelection(path).resolveOne()(100), awaitTimeout)
      nDash ! successor_fixFinger(t,x) // checks successor for finger
    }

    // Function to check if a node is alive
    def isAlive(tocheck:ActorRef):Boolean={
      var alive = true
      try{
        println("Checking "+tocheck.path.name.toInt)
        val f = tocheck ? iamalive()
        val re =  Await.result(f,Duration.create(5, "seconds"))
      }
      catch{
        case e:Exception => {
          if(tocheck != null){
            println( "Node "+tocheck.path.name.toInt+" failed", e)
            alive = false
          }
        }
      }
      alive
    }

  }

  class FingerEntry(start: Int, end: Int, var nodeActor: ActorRef) {

    def getStart(): Int = this.start

    def getNode(): ActorRef = this.nodeActor

    def getNodeID(): Int = nodeActor.path.name.toInt

    def setNode(n: ActorRef): Unit = this.nodeActor = n

  }

  case class Join(nDash: ActorRef)

  case class Join_Continue(predecessor: ActorRef, successor: ActorRef)

  case class Set_Predecessor(nodeActor: ActorRef)

  case class Set_Successor(nodeActor: ActorRef)

  case class Find_Predecessor(nodeActor: ActorRef, id: Int)

  case class Find_Successor(nodeActor: ActorRef, id: Int)

  case class Find_Successor1(nodeActor: ActorRef, i: Int, start: Int)

  case class Set_FNode(i: Int, successor: ActorRef)

  case class Update_Finger_Table(nodeActor: ActorRef, nodeID: Int, pvalue:
  Int, i: Int)

  case class SearchKey(nodeActor: ActorRef, code: String, hops: Int)


  case class successorElement(node: ActorRef, id: Int)

  case class successorElement_continues(successor: ActorRef,id: Int)

  case class successor_fixFinger(node: ActorRef, id: Int)

  case class fix_fingers_continues(successor: ActorRef,id: Int)

  case class kill()

  case class iamalive()

}

