package start

object StringInter {

  def main(args: Array[String]) {

    println("===== Started =====")
    println
    
    val name = "Krishna"
    val age = 32
    
    println(name +" is " + age + " years old.")
    println
    println(s"$name is $age years old.\n")
    
    println(f"$name%s is $age%d years  old.\n")
    
    println(raw"Hello \n world")
    println
    println(s"Hello \n world")
    
  }
}