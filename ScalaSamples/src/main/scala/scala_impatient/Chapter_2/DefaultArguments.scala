package scala_impatient.Chapter_2

/**
  * Created by YJ02 on 3/21/2017.
  You can provide default arguments for functions that are used when you don’t
specify explicit values.

  You can also specify the parameter names when you supply the arguments, Note that the named arguments need not be in the same
order as the parameters.


  */
object DefaultArguments {

  def main(args:Array[String]):Unit={
println("Hello World!!")
  println(defaultArgs("Abc"))
  println(defaultArgs("Abc","("))
  println(defaultArgs(left="[", str="Hai"))


  }

  def defaultArgs(str:String, left:String="{", right:String="}"):String={

    left+str+right

  }

}
