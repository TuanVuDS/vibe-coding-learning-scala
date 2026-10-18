package vn.scalaeng.p2.security

import cats.Monad
import cats.effect.IO
import cats.syntax.all.*
import vn.scalaeng.p2.domain.*
import vn.scalaeng.p2.service.TransactionRunner

final class AuthService[F[_]: Monad] private (store: Store[F], tx: TransactionRunner[F], tokens: Tokens, dummy: String):
  def register(name: String, email: String, password: String): IO[Either[DomainError, User]] =
    val input = Validation.user(name,email).flatMap { draft =>
      val errors = Passwords.policy(password,draft.email)
      Either.cond(errors.isEmpty,draft,DomainError.Invalid(errors))
    }
    input.fold(e => IO.pure(Left(e)), d => Passwords.hash(password).flatMap(h => tx.run(store.register(d,h))))
  def login(email: String, password: String): IO[Option[String]] =
    val normalized = email.trim.toLowerCase(java.util.Locale.ROOT)
    tx.run(store.account(normalized)).flatMap { account =>
      // Bound input before expensive hashing; all valid-size unknown accounts still verify a dummy hash.
      if password.length > 128 || normalized.length > 254 then IO.pure(None)
      else Passwords.verify(password,account.map(_.hash).getOrElse(dummy)).map { ok =>
        account.filter(_ => ok).map(a => tokens.issue(a.user.id))
      }
    }
  def authenticate(raw: String): IO[Either[DomainError, (Principal, VerifiedToken)]] =
    tokens.verify(raw) match
      case Left(_) => IO.pure(Left(DomainError.Forbidden))
      case Right(v) => tx.run {
        (store.revoked(v.jti), store.accountById(v.userId)).mapN { (revoked, a) =>
          a.filter(_ => !revoked).map(u => (Principal(u.user.id,u.role == "Admin"),v)).toRight(DomainError.Forbidden)
        }
      }
  def logout(token: VerifiedToken): IO[Unit] = tx.run(store.revoke(token.jti,token.expires.getEpochSecond))
object AuthService:
  def make[F[_]: Monad](store: Store[F], tx: TransactionRunner[F], tokens: Tokens): IO[AuthService[F]] =
    Passwords.hash(UUIDPassword).map(new AuthService(store,tx,tokens,_))
  private val UUIDPassword = java.util.UUID.randomUUID().toString
