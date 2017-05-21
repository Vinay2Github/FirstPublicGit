package scala_impatient.Chapter_1

/**
  * Created by YJ02 on 3/21/2017.

  We can think apply() method as overloaded form of the () operator. i.e., "Hello"(4) is same as "Hello".apply(4)

  */
object ApplyMethod {

  def main(args: Array[String]): Unit = {
    println("Hello World");

    val property = new ApplyExample()

    println(property("India"))
    println(property.apply("India"))

  }

}

class ApplyExample {

  def apply( a:String):String={
 "Hello "+ a
  }

}
