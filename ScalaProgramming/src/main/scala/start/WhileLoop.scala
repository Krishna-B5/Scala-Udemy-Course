package start

object WhileLoop {

  def main(args: Array[String]) {

    var x = 0
    var y = 0

    println("===== Started =====\n")
    println("\n ====== while loop =====\n")
    while (x < 10) {
      x += 1
      println(s"Value of X is $x") 
    }

    println("\n ====== do while loop =====\n")
    
    do {
      println(s"Value of x is $x\n") // here x value will be 10 because above incremented
      x += 1
    } while (x < 10)

    do {
      println(s"Value of Y is $y")
      y += 1
    } while (y < 10)

    println("\n ===== Done =====")
  }
}