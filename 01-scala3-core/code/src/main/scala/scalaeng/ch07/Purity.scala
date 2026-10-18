package scalaeng.ch07

/** Minh họa pure function, side effect và referential transparency. */
object Purity:
  // Pure: cùng input -> cùng output, không tác động bên ngoài
  def applyVat(amount: BigDecimal, rate: BigDecimal): BigDecimal = amount + amount * rate

  // Impure: kết quả phụ thuộc trạng thái bên ngoài (biến toàn cục)
  private var globalRate: BigDecimal = BigDecimal("0.1")
  def setRate(r: BigDecimal): Unit = globalRate = r
  def applyVatImpure(amount: BigDecimal): BigDecimal = amount + amount * globalRate

  /** Một phép tính trạng thái viết thuần: chuyển trạng thái tài khoản ví theo sự kiện. */
  enum WalletEvent:
    case Deposit(amount: BigDecimal)
    case Withdraw(amount: BigDecimal)

  enum WalletError:
    case InsufficientFunds(balance: BigDecimal, requested: BigDecimal)
    case InvalidAmount(amount: BigDecimal)

  def applyEvent(balance: BigDecimal, event: WalletEvent): Either[WalletError, BigDecimal] =
    import WalletEvent.*, WalletError.*
    event match
      case Deposit(a) if a <= 0              => Left(InvalidAmount(a))
      case Deposit(a)                        => Right(balance + a)
      case Withdraw(a) if a <= 0             => Left(InvalidAmount(a))
      case Withdraw(a) if a > balance        => Left(InsufficientFunds(balance, a))
      case Withdraw(a)                       => Right(balance - a)

  /** foldLeft trên Either: dừng ở lỗi đầu tiên. */
  def replay(initial: BigDecimal, events: List[WalletEvent]): Either[WalletError, BigDecimal] =
    events.foldLeft[Either[WalletError, BigDecimal]](Right(initial)): (acc, e) =>
      acc.flatMap(applyEvent(_, e))

@main def ch07Purity(): Unit =
  import Purity.*, Purity.WalletEvent.*
  val a = applyVat(BigDecimal(100), BigDecimal("0.1"))
  println(s"Pure: $a, $a  (thay biểu thức bằng giá trị không đổi nghĩa)")
  val x1 = applyVatImpure(BigDecimal(100))
  setRate(BigDecimal("0.08"))
  val x2 = applyVatImpure(BigDecimal(100))
  println(s"Impure: lần 1 = $x1, lần 2 = $x2 (cùng input, khác output)")
  println(replay(BigDecimal(0), List(Deposit(500), Withdraw(200), Deposit(50))))
  println(replay(BigDecimal(0), List(Deposit(100), Withdraw(300), Deposit(1000))))
