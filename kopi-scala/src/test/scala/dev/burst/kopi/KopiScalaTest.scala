package dev.burst.kopi

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration.*

/**
 * Unit tests for the Scala kopi wrapper.
 *
 * These tests verify the Scala API surface without touching AWS — they call
 * registration and isWorker() only.
 */
class KopiScalaTest extends AnyFunSuite with Matchers:

  given ExecutionContext = ExecutionContext.global

  test("register does not throw") {
    noException should be thrownBy {
      Kopi.register[Int, Int]("scala_test_double", x => Right(x * 2))
    }
  }

  test("register with Either error variant") {
    noException should be thrownBy {
      Kopi.register[String, String](
        "scala_test_upper",
        s => if s.nonEmpty then Right(s.toUpperCase) else Left("empty input")
      )
    }
  }

  test("isWorker returns false outside worker environment") {
    // BURST_WORKER is not set in the test environment
    Kopi.isWorker() shouldBe false
  }

  test("Kopi object is accessible") {
    // Verify the object is properly accessible and callable
    Kopi should not be null
  }
