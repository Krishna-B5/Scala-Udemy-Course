package start

object SumOfN_Nubers {
  
  def main(args: Array[String]){
    
    var sum = 0
    var res = 0
    
    for(i <- 1 to 365)
    {
      res = sum + i
      sum = res
  //    println(sum)
    }
    println(" sum of 365 is "+res)
  }
}