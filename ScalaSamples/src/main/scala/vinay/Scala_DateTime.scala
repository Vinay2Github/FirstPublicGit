package vinay

import java.text.SimpleDateFormat
import java.util.Calendar

/**
  * Created by YJ02 on 5/9/2017.
  */
object Scala_DateTime {

  def main(args:Array[String]): Unit ={

    println("Hello World")
    val dateTimeFormat = new SimpleDateFormat("yyy:MM:dd:hh:mm:ss")
    val now = Calendar.getInstance().getTime()
    println(now+"now="+dateTimeFormat.format(now))
  }

}
