package scala_in_action.oops_in_action

/**
  * Created by YJ02 on 5/1/2017.
  */
object CaseClassDemo{

  def main(args:Array[String]): Unit ={
   val a:SampleCaseClass = new SampleCaseClass("Hello","world")
    val b:SampleCaseClass = new SampleCaseClass()

    println("a.property1="+a.property1)
    println("a.property2="+a.property2)
    println("b.property1="+b.property1)
    println("b.property1="+b.property2)

    a.property1="Hello KVJ"
    println("a.property1="+a.property1)
    println("a.property2="+a.property2)

  }


}

class SampleCaseClass(var property1:String, val property2:String){
   def this()=this("default_Value_for_Property1","default_Value_for_Property2")

}