package jvmlab

import java.lang.management.ManagementFactory
import scala.jdk.CollectionConverters.*

/** Thí nghiệm 1: JVM đang chạy với cấu hình gì? Luôn in thông tin này khi khởi động service/job. */
@main def jvmInfo(): Unit =
  val rt = Runtime.getRuntime
  val mb = 1024 * 1024
  println(s"Java: ${System.getProperty("java.version")} (${System.getProperty("java.vm.name")})")
  println(s"CPU khả dụng: ${rt.availableProcessors()}")
  println(s"Heap tối đa (-Xmx): ${rt.maxMemory() / mb} MB")
  println(s"GC: ${ManagementFactory.getGarbageCollectorMXBeans.asScala.map(_.getName).mkString(", ")}")
  println(s"JVM args: ${ManagementFactory.getRuntimeMXBean.getInputArguments.asScala.mkString(" ")}")
  println(s"Số class đã nạp: ${ManagementFactory.getClassLoadingMXBean.getLoadedClassCount}")
