package scala_cookbook

/**
  * Created by YJ02 on 3/24/2017.
  This code demonstrate yeild keyword functionality.
  */
object ForYeild {

  def main(args:Array[String]): Unit ={

    println("Hello World");

    val a = Array("Apple","Mango","Orange")

    for(e<-a) {println(e)}
    for(i<-0 to a.length-1){ println(i+","+a(i))}
    for(i<-0 until a.length if(i<2)){ println(i+","+a(i))}

    val b= for(e<-a) yield{e.toUpperCase()}

    b.foreach(println)

    for(i<-1 to 2; j <-3 to 4){println(i + ","+ j)}

  }



}
