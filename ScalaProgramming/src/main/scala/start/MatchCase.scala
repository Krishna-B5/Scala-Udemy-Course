package start

object MatchCase {
  
  def main(args: Array[String]){
    // Type should be same int means int, String means String
    val age = 200
    
    age match {
      case 10 => println(age)
      case 20 => println(age)
      case 30 => println(age)
      case 40 => println(age)
      
      case _ => println("Case didn't match") // if the case didn't match _ will
    }
    
    println("Multiple Case Values")
    
    val i = 5
    i  match {
      case 1|3|5|7|9 => println("Odd Number")
      case 2|4|6|8 => println("Even Number")
    }
    
    // experssion is nothing but returning a value
    
    val res = age match {
      case 10 => age
      case 20 => age
      case 30 => age
      case 40 => age 
      
      case _ => 0 // if the case didn't match _ will
    }
    println("Result is : "+res)
  }
}