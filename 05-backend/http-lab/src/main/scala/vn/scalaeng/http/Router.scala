package vn.scalaeng.http

/** Một route: method + mẫu đường dẫn ("api", "v1", "products", ":id") + handler nhận tham số đường dẫn. */
final case class Route(method: Method, pattern: List[String], handler: (Request, Map[String, String]) => Response)

object Route:
  def apply(method: Method, path: String)(h: (Request, Map[String, String]) => Response): Route =
    Route(method, path.split("/").toList.filter(_.nonEmpty), h)

object Router:
  private def matchPath(pattern: List[String], segs: List[String]): Option[Map[String, String]] =
    if pattern.size != segs.size then None
    else
      pattern.zip(segs).foldLeft(Option(Map.empty[String, String])) {
        case (Some(acc), (p, s)) if p.startsWith(":") => Some(acc.updated(p.drop(1), s))
        case (Some(acc), (p, s)) if p == s            => Some(acc)
        case _                                        => None
      }

  /** 404 khi không có đường dẫn khớp; 405 + header Allow khi đường dẫn khớp nhưng sai method. */
  def apply(routes: List[Route]): Handler = req =>
    val byPath = routes.flatMap(r => matchPath(r.pattern, req.segments).map(r -> _))
    byPath.find(_._1.method == req.method) match
      case Some((route, params)) => route.handler(req, params)
      case None if byPath.nonEmpty =>
        val allow = byPath.map(_._1.method).distinct.sortBy(_.ordinal).mkString(", ")
        Response.problem(405, "Method Not Allowed", s"${req.method} không được hỗ trợ cho ${req.path}").withHeader("Allow", allow)
      case None => Response.problem(404, "Not Found", s"không có tài nguyên ${req.path}")
