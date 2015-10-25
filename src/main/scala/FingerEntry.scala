import akka.actor.ActorRef

/**
 * Created by PRITI on 10/23/15.
 */
class FingerEntry(start: BigInt, end: BigInt, var node: ActorRef) {

  def getStart(): BigInt = this.start

//  def getInterval(): Interval = this.interval

  def getNode(): ActorRef = this.node

  def getHash(): BigInt = node.path.name.toInt

  def setNode(newNode: ActorRef): Unit = this.node = newNode

  def print: String = "Start: %s, End: %s, Node: %s".format(start, end,
    getHash())

}
