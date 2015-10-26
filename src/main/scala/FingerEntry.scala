import akka.actor.ActorRef

/**
 * Created by PRITI on 10/23/15.
 */
class FingerEntry(start: Int, end: Int, var nodeActor: ActorRef) {

  def getStart(): Int = this.start

  def getNode(): ActorRef = this.nodeActor

  def getNodeID(): Int = nodeActor.path.name.toInt

  def setNode(n: ActorRef): Unit = this.nodeActor = n

}
