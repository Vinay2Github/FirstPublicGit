package scala_in_action

/**
  * Created by yj02 on 4/29/2017.
  */
object PatrenMatchingDemo {

  def main(args:Array[String]):Unit= {

    println("Hello World PattrenMatching example")
    println("Scala Pattrenmatching is similar to Java Swtich case statements")

    val a=51

    a match {
      case 1=>println("a=1")
      case 2=>println("a=2")
      case 3=>println("a=3")
      case 4=>println("a=4")
      case 5=>println("a=5")
      case _=> println("a is something else") // case _ is not madatory but with it we get a run time
                                              // exception if value doesnt match any case.

        //Java Switch statement is used only with integers and Enums, Scala pattren matching can be applied to any objects.
        val b:Any = "India"

        b match {

          case int:Int=> print("b is integer")
          case float:Float=> print("b is Float")
          case str:String=> print("b is String")
          case arr:Array[_]=> print("b is Array")

//We can even add gaurd in the case statement as below.

            val c=91

            c match {

              case oneTo10 if 1 to 10 contains oneTo10 => print("C is between one and 10")
              case _=> print("C is not between 1 and 10")
            }

            println("End of pattren matching")


        }

    }

  }



}
