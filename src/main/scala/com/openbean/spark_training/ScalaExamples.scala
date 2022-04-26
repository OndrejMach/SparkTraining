package com.openbean.spark_training

case class Employee(id: Long, name: String, address: Option[String])

object ScalaExamples {
  def filteringF(i: String) = i > "1" && i <"3"

  def main(args: Array[String]): Unit = {
    val i = if (4<5) {println("gonna be returning 6"); 6} else 7
    val a = for ( i <- 1 to 10) yield {i+5}
    println(a.mkString(";"))


    val c = 5
    val s = c match {
      case 4 => "four"
      case _ => "something else"
    }

    val list = Seq.apply("3", "2", "1")
    val n = list.map( i => "number "+i)
    list.filter( filteringF(_)).foreach(println(_))
    list.map( i => "number "+i).foreach(println(_))
    //touples
    val t =(1,2,3,4)

    val res = list.reduce(_ + _)

    val employees = Seq(Employee(id = 1, address= Some("doma"),name = "Petr"),Employee(1, "Petr2", Some("doma")) )

    employees.map(i => if (i.name == "Petr") 0 else 1).foreach(println(_))


    println(res)


    val age = Map("Zdenek" -> 20, "Radek" -> 6, "Pavel" -> 15)

    println(age.getOrElse("Ondrej", 0))

    println(age("Zdenek"))

    val petr = Employee(1,"Petr",None)
    println(petr.address.getOrElse("No Address specified"))
  }
}
