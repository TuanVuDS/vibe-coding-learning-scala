package vn.scalaeng.ch08c.solutions

import scala.compiletime.{constValue, erasedValue, error, summonInline}
import scala.deriving.Mirror
import vn.scalaeng.ch08c.{CsvEncoder, CsvWriter}

object BatchPolicy:
  inline def bounded(inline size: Int): Int =
    inline if size >= 1 && size <= 10000 then size
    else error("batch size must be in [1, 10000]")

object TupleExercises:
  type Leaf[X] = X match
    case Option[t] => Leaf[t]
    case List[t]   => Leaf[t]
    case _         => X

  inline def arity[Ts <: Tuple]: Int =
    inline erasedValue[Ts] match
      case _: EmptyTuple   => 0
      case _: (_ *: tail) => 1 + arity[tail]

  def acceptedRow[Ts <: Tuple](row: Ts): Tuple.Map[row.type, [A] =>> Either[String, A]] =
    row.map([A] => (value: A) => (Right(value): Either[String, A]))

final case class EventReport(eventId: String, attempts: Int, accepted: Boolean, note: Option[String])
    derives CsvEncoder

trait Show[A]:
  def show(value: A): String

object Show:
  given Show[String] with
    def show(value: String): String = value
  given Show[Int] with
    def show(value: Int): String = value.toString

  inline def derived[A <: Product](using mirror: Mirror.ProductOf[A]): Show[A] =
    productShow[A](
      constValue[mirror.MirroredLabel].toString,
      vn.scalaeng.ch08c.TypeTools.labels[mirror.MirroredElemLabels],
      writers[mirror.MirroredElemTypes]
    )

  private inline def writers[Ts <: Tuple]: List[Any => String] =
    inline erasedValue[Ts] match
      case _: EmptyTuple => Nil
      case _: (head *: tail) => writer(summonInline[Show[head]]) :: writers[tail]

  private def writer[A](instance: Show[A]): Any => String =
    value => instance.show(value.asInstanceOf[A])

  private def productShow[A <: Product](name: String, labels: List[String], renderers: List[Any => String]): Show[A] =
    new Show[A]:
      def show(value: A): String =
        val fields = labels.iterator.zip(value.productIterator).zip(renderers.iterator).map {
          case ((label, field), render) => s"$label=${render(field)}"
        }
        fields.mkString(s"$name(", ",", ")")

final case class OrderSummary(orderId: String, quantity: Int) derives Show
final case class EmptyReport() derives Show, CsvEncoder

final case class ExportResult(topic: String, batchSize: Int, csv: String)

object EventExport:
  def render(events: List[EventReport]): ExportResult =
    ExportResult(
      vn.scalaeng.ch08c.TopicLiteral.checked("orders.audit"),
      BatchPolicy.bounded(100),
      CsvWriter.write(events)
    )

object SolutionsDemo:
  def main(args: Array[String]): Unit =
    println(s"batch=${BatchPolicy.bounded(10000)}")
    println(s"arity=${TupleExercises.arity[(String, Int, Boolean)]}")
    println(s"accepted=${TupleExercises.acceptedRow(("ORD-1", 2))}")
    println(summon[Show[OrderSummary]].show(OrderSummary("ORD-1", 2)))
    println(s"percent=${PercentLiteral.checked(100)}")
    val exportResult = EventExport.render(List(EventReport("EV-1", 1, true, Some("ok, retry"))))
    println(s"topic=${exportResult.topic}; batch=${exportResult.batchSize}")
    println(exportResult.csv)
