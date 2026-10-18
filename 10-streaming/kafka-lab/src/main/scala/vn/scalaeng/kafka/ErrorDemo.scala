package vn.scalaeng.kafka

import org.apache.kafka.clients.producer.ProducerConfig

object ErrorDemo:
  def main(args: Array[String]): Unit =
    val _ = args
    val p = Clients.producerProperties("localhost:9092")
    p.setProperty("acks", "0")
    try { new ProducerConfig(p); () }
    catch case e: org.apache.kafka.common.config.ConfigException => println(e.toString)
    try { Retry.run(0, _ => ())(1); () }
    catch case e: IllegalArgumentException => println(e.toString)
    val sink = new MemoryLedger
    val e = OrderEvent(1, "e1", "o1", 100)
    sink(e)
    try { sink(e.copy(amount = 200)); () }
    catch case e: Poison => println(e.toString)
