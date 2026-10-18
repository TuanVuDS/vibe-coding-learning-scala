package vn.scalaeng.spark

import org.apache.spark.sql.SparkSession

/** SparkSession chạy LOCAL: driver và executor cùng nằm trong MỘT tiến trình JVM, mỗi "executor core" là một thread.
  * Khác cluster thật ở chỗ không có mạng giữa các máy — nhưng mô hình thực thi (job → stage → task, shuffle, lineage) giống hệt.
  */
object LocalSpark {

  /** `local[cores,maxFailures]`: `cores` thread chạy task; một task được thử lại tối đa `maxFailures` lần (mặc định local = 1, tức không thử lại). */
  def session(app: String, cores: Int = 2, maxFailures: Int = 1): SparkSession =
    SparkSession
      .builder()
      .master(s"local[$cores,$maxFailures]")
      .appName(app)
      .config("spark.ui.enabled", "false")
      // Windows: thay LocalFileSystem của Hadoop (cần winutils.exe để đặt quyền file) bằng bản thuần Java.
      .config("spark.hadoop.fs.file.impl", "com.globalmentor.apache.hadoop.fs.BareLocalFileSystem")
      .getOrCreate()
}
