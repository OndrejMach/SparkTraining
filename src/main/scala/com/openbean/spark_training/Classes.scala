package com.openbean.spark_training

import org.apache.spark.sql.DataFrame

object myFunc {

  def main(args: Array[String]): Unit = {
    val cl:GeomTransform = new Circle(5,6,7)
    val cl2 = cl.scale(4.5)
    val e = Employee("Petr", 34235252342354.4, Address(10000, "Prazsky hrad"))
  }


}


class Class1(data: DataFrame, index: Int) {

}
object Class1 {
  def apply(data: DataFrame, index: Int): Class1 = new Class1(data, index)
}

abstract class GeomObj(fillColor: Int)

class Circle(r: Double, x: Double, y: Double) extends GeomObj(5) with GeomTransform {
  def scale(factor:  Double) = new Circle(r*factor, x, y)
  def translate(x: Double, y: Double) = new Circle(r,this.x+x, this.y +y)
}

case class Address(zip: Long, city: String)

case class Employee(name: String, salary: Double, address: Address)

trait GeomTransform {
  def scale(factor: Double) : GeomTransform
  def translate(x: Double, y: Double) : GeomTransform
}