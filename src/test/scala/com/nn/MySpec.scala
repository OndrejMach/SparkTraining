package com.nn

import org.specs2.mutable._


object MySpec extends Specification {
    "Arithmetic" should {
      "add two numbers" in {
        1 + 1 mustEqual 2
      }
      "add three numbers" in {
        1 + 1 + 1 mustEqual 3
      }
    }
  }
