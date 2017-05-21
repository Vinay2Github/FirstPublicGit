package vinay



import org.json4s._
import org.json4s.jackson.JsonMethods

import scala.util.parsing.json._
/**
  * Created by YJ02 on 5/8/2017.
  */
import org.json4s.Formats
object Jsons {

  def main(args:Array[String]):Unit= {

    val json="{\"messageHeader\":{\"eventId\":\"5165c279-881c-4f6a-96f4-590e026dce6d\",\"version\":\"1.0\",\"timeStamp\":\"2017-04-06T09:17:11.691-0400\",\"correlationId\":\"a960ff41-2742-4a56-85d4-54f85361d1fc\",\"eventType\":\"OrderReceivedEvent\"},\"messageBody\":{\"orderId\":2,\"recommendedFulfillmentTime\":\"2017-04-06T14:30:00.000-0400\",\"tenantId\":\"2212\",\"lineItems\":[{\"lineItemId\":2,\"tpnb\":\"062312324\"}]}}"

   // val json="""{"hello": "world", "age": 42, "nested": { "deeper": { "treasure": true }}}"""

    implicit val formats = DefaultFormats
    val output = JsonMethods.parse(json)
    println(">>>>>>>>> "+output.values)
    val message = output.extract[Map[String, JObject]]
       var eventId:String =""

    val messageheader=message.get("messageHeader").foreach(x=>eventId=x.values.get("eventId").toString)

    val messageBody=message.get("messageBody")
   // messageHeader/"eventId"
  // val output1 = JsonMethods.parse(messageHeader.toString)
    println("productList="+eventId)
   // println("productList="+(messageHeader"eventId").extract[JString])


  }
}
case class MessageHeader(eventId:String, version: String,timeStamp:String,correlationId:String,eventType:String)
