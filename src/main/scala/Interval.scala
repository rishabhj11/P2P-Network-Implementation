/**
 * Created by PRITI on 10/23/15.
 */
class Interval(leftInclude: Boolean, leftValue: BigInt, rightValue: BigInt,
        rightInclude: Boolean) {

  def includes(value: BigInt): Boolean = {

    if (leftValue == rightValue) {
      if (leftInclude == false && rightInclude == false && value == leftValue) {
        return false
      }
      else {
        return true
      }
    }
    else if (leftValue < rightValue) {
      if ((value == leftValue && leftInclude == true) || (value == rightValue && rightInclude == true) || (value > leftValue && value < rightValue)) {
        return true
      } else {
        return false
      }
    }
    else {
      if ((value == leftValue && leftInclude == true) || (value == rightValue && rightInclude == true) || (value > leftValue || value < rightValue)) {
        return true
      } else {
        return false
      }
    }

  }

  def getEnd(): BigInt = {

    return rightValue
  }

}
