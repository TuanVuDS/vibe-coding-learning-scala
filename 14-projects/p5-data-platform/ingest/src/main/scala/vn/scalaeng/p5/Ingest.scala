package vn.scalaeng.p5

import org.apache.kafka.clients.consumer.{KafkaConsumer, OffsetAndMetadata}
import org.apache.kafka.common.TopicPartition
import io.circe.Json
import io.circe.syntax.*

import java.time.{Duration, LocalDate}
import java.util.Properties
import scala.jdk.CollectionConverters.*

/** Kafka → raw. Mỗi lần poll: ghi các record hợp lệ thành MỘT file (fsync + rename), record vi phạm contract vào quarantine, RỒI MỚI commit offset.
  * Chết giữa "ghi file" và "commit" → lần sau đọc lại các offset đó → raw có BẢN SAO (at-least-once). Không sao: staging bỏ sự kiện trùng theo
  * `event_id` (Chương 30 — idempotency ở phía tiêu thụ). Đánh đổi: raw có thể chứa trùng; đối soát đếm chúng ở `duplicate_events`.
  */
object KafkaLanding:
  def consumer(bootstrap: String, group: String, maxPollRecords: Int = 500): KafkaConsumer[String, String] =
    val p = new Properties()
    Map(
      "bootstrap.servers" -> bootstrap, "group.id" -> group, "enable.auto.commit" -> "false", "auto.offset.reset" -> "earliest",
      "max.poll.records" -> maxPollRecords.toString, "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer"
    ).foreach((k, v) => p.setProperty(k, v))
    new KafkaConsumer[String, String](p)

  final case class Batch(files: List[String], records: Int, quarantined: Int, committed: Map[Int, Long])

  /** Một vòng poll → land → commit. `beforeCommit` chỉ cho test mô phỏng chết sau khi ghi file, trước commit. */
  def pollOnce(lake: Lake, c: KafkaConsumer[String, String], ingestDate: LocalDate, timeout: Duration = Duration.ofSeconds(2),
      beforeCommit: () => Unit = () => ()): Batch =
    val records = c.poll(timeout).asScala.toVector
    val byPartition = records.groupBy(r => (r.topic(), r.partition()))
    val written = byPartition.toList.sortBy(_._1._2).flatMap { case ((topic, part), rs) =>
      val (bad, good) = rs.partitionMap(r => problem(r.value()).map(why => (r, why)).toLeft(r))
      val base = s"kafka-$topic-p$part-${rs.head.offset()}-${rs.last.offset()}"
      // Quarantine của đường Kafka: mỗi dòng một PHONG BÌ JSON (topic, partition, offset, lý do, giá trị gốc) — đọc được bằng máy, lý do nằm
      // cạnh dữ liệu. Phục hồi: lấy `value`, sửa, gửi lại vào topic — README runbook 3b (review P5 N3: bản đầu ghi "offset<TAB>value" không lý do).
      if bad.nonEmpty then
        Io.writeAtomically(lake.root.resolve("quarantine").resolve("orders").resolve(s"ingest_date=$ingestDate").resolve(s"$base.jsonl"),
          bad.map((r, why) => Json.obj("topic" -> topic.asJson, "partition" -> part.asJson, "offset" -> r.offset().asJson, "reason" -> why.asJson,
            "value" -> Option(r.value()).asJson).noSpaces).mkString("", "\n", "\n"))
      Option.when(good.nonEmpty) {
        val f = lake.rawDir("orders").resolve(s"ingest_date=$ingestDate").resolve(s"$base.jsonl")
        Io.writeAtomically(f, good.map(_.value()).mkString("", "\n", "\n"))
        lake.relative(f)
      }.toList
    }
    beforeCommit()
    val offsets = byPartition.map { case ((topic, part), rs) => new TopicPartition(topic, part) -> new OffsetAndMetadata(rs.last.offset() + 1) }
    if offsets.nonEmpty then c.commitSync(offsets.asJava)
    Batch(written, records.size, records.count(r => problem(r.value()).isDefined), offsets.map((tp, o) => tp.partition() -> o.offset()).toMap)

  private def problem(value: String): Option[String] = Option(value).fold(Some("value null"))(v => OrderEvent.conform(v).left.toOption)

  /** Lag = end offset − committed offset, từng partition: chỉ số đầu tiên cần xem khi dữ liệu realtime "đứng" (runbook 1 của README). */
  def lag(c: KafkaConsumer[String, String], topic: String): Map[Int, Long] =
    val parts = c.partitionsFor(topic).asScala.map(p => new TopicPartition(topic, p.partition())).toSet
    val ends = c.endOffsets(parts.asJava).asScala
    val committed = c.committed(parts.asJava).asScala
    parts.toList.map(tp => tp.partition() -> (ends(tp).longValue - Option(committed.getOrElse(tp, null)).map(_.offset()).getOrElse(0L))).toMap
