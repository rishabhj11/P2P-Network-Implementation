import akka.actor.ActorRef

/**
 * Created by PRITI on 10/23/15.
 */
class FingerEntry(start: BigInt, interval: Interval, var node: ActorRef) {

  def getStart(): BigInt = {

    return this.start
  }

  def getRange(): Interval = {

    return this.interval
  }

  def getNode(): ActorRef = {

    return this.node
  }

  def getHash(): BigInt = {

    //return BigInt.apply(node.toString().sha1.hex,16)
    //    (node.toString.charAt(25)-48).toInt
    return node.path.name.toInt
  }

  def setNode(newNode: ActorRef): Unit = {

    this.node = newNode
  }

  def print:String={
    return ("Start: %s, End: %s, Node: %s".format(start,interval.getEnd,getHash()))
  }
}
