package example

object Hello extends Greeting with App {
  val name = 
  println(greeting)
}

trait Greeting {
  lazy val greeting: String = "hello"
}
