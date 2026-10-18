package vn.scalaeng.order.app

import java.nio.file.{Path, Paths}

enum Env:
  case Dev, Staging, Prod

enum LogLevel:
  case Debug, Info, Warn, Error

final case class AppConfig(env: Env, inputPath: Path, outputDir: Path, maxRejectRatio: Double, logLevel: LogLevel)

object AppConfig:
  /** Đọc cấu hình từ biến môi trường (12-factor). Hàm THUẦN: nhận Map, không tự gọi sys.env -> test được.
    * Tích lũy MỌI lỗi để người vận hành sửa một lần.
    */
  def load(vars: Map[String, String]): Either[List[String], AppConfig] =
    val envE: Either[String, Env] =
      vars.getOrElse("APP_ENV", "dev").trim.toLowerCase match
        case "dev"     => Right(Env.Dev)
        case "staging" => Right(Env.Staging)
        case "prod"    => Right(Env.Prod)
        case other     => Left(s"APP_ENV không hợp lệ: '$other' (dev|staging|prod)")

    // Ở prod KHÔNG có giá trị mặc định cho đường dẫn dữ liệu: thiếu là lỗi, không đoán.
    val inputE: Either[String, Path] = (vars.get("INPUT_PATH"), envE) match
      case (Some(p), _) if p.trim.nonEmpty => Right(Paths.get(p.trim))
      case (_, Right(Env.Dev))             => Right(Paths.get("sample-data/orders.csv"))
      case _                               => Left("thiếu INPUT_PATH (bắt buộc ngoài môi trường dev)")

    val outputE: Either[String, Path] = Right(Paths.get(vars.getOrElse("OUTPUT_DIR", "out").trim))

    val ratioE: Either[String, Double] =
      vars.get("MAX_REJECT_RATIO") match
        case None => Right(0.05)
        case Some(raw) =>
          raw.trim.toDoubleOption.filter(r => r >= 0 && r <= 1).toRight(s"MAX_REJECT_RATIO phải trong [0, 1]: '$raw'")

    val levelE: Either[String, LogLevel] =
      val raw = vars.getOrElse("LOG_LEVEL", "info").trim
      LogLevel.values.find(_.toString.equalsIgnoreCase(raw)).toRight(s"LOG_LEVEL không hợp lệ: '$raw'")

    (envE, inputE, outputE, ratioE, levelE) match
      case (Right(e), Right(i), Right(o), Right(r), Right(l)) => Right(AppConfig(e, i, o, r, l))
      case _ => Left(List(envE, inputE, outputE, ratioE, levelE).collect { case Left(err) => err })
