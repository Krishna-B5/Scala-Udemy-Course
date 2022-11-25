package start

object PrimeNumber {

  def main(args: Array[String]){
    
  for(i <- 2 to 100)
  {
    var sum = 0
    for(j <- 2 to i)
      if(i%j == 0)
        sum = sum + j
        if(sum == i)
      {
        println("No is prime number" +i)
      }
  }
    
//    for ( i <- 2 to 8){
//    
//    
//    val a = 8%i
//    
//    println(a)
//    }
  }
}