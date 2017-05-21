package scala_impatient.Chapter_2

/**
  * Created by YJ02 on 3/21/2017.
  In Scala, an if/else has a value, namely the value of the expression that follows.
  A block has a value—the value of its last expression.
the if or else.

  */
object IfOrElseReturnsValue {

  def main(args: Array[String]):Unit={

    val x= 1;

    println(if(x==1) {"x is equal to 1"; "Hello"}else {"x is not equal to 1"} )

    val y= 2;

    println(if(y==1)"y is equal to 1";else {"y is not equal to 1" ; "Hello World"} )

  }

}
