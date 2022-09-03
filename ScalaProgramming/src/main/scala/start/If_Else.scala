package start

object If_Else {
  
  def main(args: Array[String]){
    
    val x = 5
    var res = ""
    
    if(x == 5){
      println(" X is equal to 5")
      res = "X = 5"
    }
    else{
      println(" X is not equal to 5 ")
      res = "X != 5"
    }
    println(res)
    
    println(if(x == 6) "x = 5" else "x != 5")
  }
  
}