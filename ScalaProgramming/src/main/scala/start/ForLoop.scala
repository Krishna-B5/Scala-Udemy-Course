package start

object ForLoop {
  
  def main(args: Array[String]){
    
    // using to(range)
    for(i <- 1 to 5){
      println("Value of i using to : " +i)
    }
    println
    // using until(range but n-1)
    for(i <- 1 until(6)){
      println("Value of i using until : " +i)
    }
    println
    for(i <- 1 to 3; j <- 1 until 4){
      println("Values of i and j is : " +i +" "+j)
    }
    println
    // For loop using List
    val lst = List(1,2,3,4,5,6,7,8,9)
    for(i <- lst){
      println("Value of List i is : " +i)
    }
    println
    // For loop using List and Condition
    println("Printing greater than 5 \n")
    for(i <- lst; if i > 5){
      println("Value of List i is : " +i)
    }
    println
    println("Using Experssion it will return value type List \n")
    val res = for{i <- lst; if i < 5} yield {
      i*i
    }
    println("Value of Result is : " +res)
  }
}