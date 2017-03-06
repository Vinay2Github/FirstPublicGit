package vinay.extendingSparkAPI

/**
  * Created by YJ02 on 2/27/2017.
  */
class SalesRecord(val transactionId: String,
                  val customerId: String,
                  val itemId: String,
                  val itemValue: Double) extends Comparable[SalesRecord]
  with Serializable {

  override def compareTo(o: SalesRecord): Int = {
    return this.transactionId.compareTo(o.transactionId)
  }

  override def toString: String = {
    transactionId+","+customerId+","+itemId+","+itemValue
  }
}