package scala_impatient.Chapter_2

/**
  * Created by YJ02 on 3/21/2017.
  When a val is declared as lazy, its initialization is deferred until it is accessed for
the first time

  */
object LazyValues {

  def main(args:Array[String])={

    println("Hello World!!!")

    var x=2
    lazy val one=x
    lazy val two=x

    println("Value of one="+one)
    x=3
    println("Value of two="+two)
    println("Value of one="+one)
  }

}
