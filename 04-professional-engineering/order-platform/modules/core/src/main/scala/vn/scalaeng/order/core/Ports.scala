package vn.scalaeng.order.core

/** "Port" logging: core chỉ biết interface này, không biết thư viện logging cụ thể.
  * App module cung cấp implementation (JSON ra stderr, hoặc SLF4J/Logback trong production).
  */
trait Logger:
  def info(message: String, fields: (String, String)*): Unit
  def warn(message: String, fields: (String, String)*): Unit
  def error(message: String, fields: (String, String)*): Unit
