package start

object FunctionsInScala {

  object Math {
    def add(x: Int, y: Int): Int = {
      x + y
    }
  }

  def main(args: Array[String]): Unit = {
    println(Math.add(20, 10))
    val i = sub(10, 5)
    println("Minus of 2 numbers :" + i)
    println(div(5, 2))
  }

  def sub(x: Int, y: Int): Int = {
    x - y
  }

  def div(x: Int, y: Int): Float = {
    x / y
  }
}