package dev.burst.kopi

import dev.burst.kopi.worker.Worker

import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters.*
import scala.reflect.ClassTag
import scala.util.{Failure, Success}

/**
 * Thin Scala wrapper around the Java kopi library.
 *
 * The same JAR IS the worker. Register functions before the worker check,
 * then call [[map]] to distribute work across AWS Fargate.
 *
 * {{{
 * @main def run(): Unit =
 *   Kopi.register[Int, Int]("double", x => Right(x * 2))
 *
 *   if Kopi.isWorker() then
 *     System.exit(Kopi.runWorker())
 *
 *   given ec: ExecutionContext = ExecutionContext.global
 *   val items: Seq[Int] = (0 until 100).toSeq
 *   Kopi.map[Int, Int]("double", items).foreach(results =>
 *     println(s"got ${results.size} results")
 *   )
 * }}}
 */
object Kopi:

  /**
   * Registers a function under `name` for worker dispatch.
   *
   * The function receives items of type `T` and returns either
   * a result `U` (Right) or an error message (Left).
   *
   * @param name  unique function name
   * @param fn    function returning Either[errorMsg, result]
   * @tparam T    input type (must be JSON-serializable)
   * @tparam U    output type (must be JSON-serializable)
   */
  def register[T: ClassTag, U: ClassTag](
    name: String,
    fn: T => Either[String, U]
  ): Unit =
    val inputClass  = implicitly[ClassTag[T]].runtimeClass.asInstanceOf[Class[T]]
    val outputClass = implicitly[ClassTag[U]].runtimeClass.asInstanceOf[Class[U]]
    val javaFn: ThrowingFunction[T, U] = (input: T) =>
      fn(input) match
        case Right(result) => result
        case Left(err)     => throw new RuntimeException(err)
    // Delegate to the Java FunctionRegistry directly to avoid name-shadowing
    // (the Scala object Kopi and Java class Kopi share the same binary name)
    dev.burst.kopi.registry.FunctionRegistry.register(name, inputClass, outputClass, javaFn)

  /**
   * Returns true if this process is running in worker mode
   * (`BURST_WORKER=1` environment variable).
   */
  def isWorker(): Boolean = Worker.isWorker()

  /**
   * Runs the worker lifecycle. Call from your main method when
   * [[isWorker]] returns true, then pass the return value to
   * `System.exit`.
   *
   * @return 0 on success, 1 on error
   */
  def runWorker(): Int = Worker.run()

  /**
   * Distributes `items` across AWS Fargate workers, calling the function
   * registered under `fnName` on each item. Returns a `Future` of ordered
   * results.
   *
   * @param fnName  name of the registered function
   * @param items   sequence of items to process
   * @param opts    options (workers, CPU, memory, etc.)
   * @param ec      execution context for wrapping results in Future
   * @tparam T      input item type
   * @tparam U      result type
   * @return        Future of ordered results
   */
  def map[T: ClassTag, U: ClassTag](
    fnName: String,
    items: Seq[T],
    opts: MapOptions = MapOptions.defaults()
  )(using ec: ExecutionContext): Future[Seq[U]] =
    val resultClass = implicitly[ClassTag[U]].runtimeClass.asInstanceOf[Class[U]]
    val itemList    = items.toList.asJava
    Future {
      // Delegate to Session directly; Config.load() is called inside Session
      val cfg = dev.burst.kopi.config.Config.load()
      val mapper = new com.fasterxml.jackson.databind.ObjectMapper()
      val itemNodes = new java.util.ArrayList[com.fasterxml.jackson.databind.JsonNode]()
      for item <- items do
        itemNodes.add(mapper.valueToTree[com.fasterxml.jackson.databind.JsonNode](item))
      val results = dev.burst.kopi.session.Session.runSession(
        cfg, itemNodes, fnName, resultClass, opts)
      results.asScala.toSeq
    }.transform {
      case Success(v)  => Success(v)
      case Failure(ex: KopiException) => Failure(ex)
      case Failure(ex) => Failure(new KopiException(ex.getMessage, ex))
    }
