/**
 * Created by PRITI on 10/23/15.
 */
class Interval(
                leftInclude: Boolean, leftValue: BigInt, rightValue: BigInt,
                rightInclude: Boolean) {

  def includes(value: BigInt): Boolean = {

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

  def getEnd: BigInt = {

    rightValue
  }

}
