package start

object If_Else {
  
  def main(args: Array[String]){
    
    val x = 5
    var res = ""
    
    if(x == 5){
      println(s"$x is equal to 5\n")
      res = "X = 5"
    }
    else{
      println(" $x is not equal to 5 ")
      res = "X != 5"
    }
    println(res)
    
    println(if(x == 6) "x = 5" else "x != 5")
  }
  
}