import akka.actor.ActorRef

/**
 * Created by PRITI on 10/23/15.
 */
class FingerEntry(start: Int, end: Int, var nodeActor: ActorRef) {

  def getStart(): Int = this.start

  def getNode(): ActorRef = this.nodeActor

  def getHash(): Int = nodeActor.path.name.toInt

  def setNode(newNode: ActorRef): Unit = this.nodeActor = newNode

  def print: String = "Start: %s, End: %s, Node: %s".format(start, end,
    getHash())

}
