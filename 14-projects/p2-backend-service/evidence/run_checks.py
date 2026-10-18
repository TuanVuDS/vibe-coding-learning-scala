"""Run scoped, sandbox-compatible checks and keep UTF-8 evidence without PowerShell formatting."""
from pathlib import Path
from datetime import datetime, timezone
import subprocess, os, sys, json, re
sys.stdout.reconfigure(encoding="utf-8")
root = Path(__file__).resolve().parents[3]
project = root / "14-projects/p2-backend-service"
env = dict(os.environ, COURSIER_CACHE=str(root / ".cache/coursier"), NO_COLOR="1", JAVA_TOOL_OPTIONS="-Dfile.encoding=UTF-8 -Dstdout.encoding=UTF-8 -Dstderr.encoding=UTF-8")
env.pop("RUN_DB_TESTS", None)
cli = str(Path(os.environ["LOCALAPPDATA"]) / "Coursier/data/bin/scala-cli.bat")
checks = {
 "tests": [cli,"test",str(project),"--server=false","--suppress-outdated-dependency-warning"],
 "solutions": [cli,"test",str(root/"solutions/ch18-code"),"--server=false","--suppress-outdated-dependency-warning"],
 "domain-isolated": [cli,"compile",str(project/"domain/src/main/scala"),"--server=false","--scala","3.9.0","--jvm","21","--scalac-option","-Wunused:all","--scalac-option","-Werror"],
 "demo": [cli,"run",str(project),"--server=false","--suppress-outdated-dependency-warning","--main-class","vn.scalaeng.p2.service.ArchitectureDemo"],
 "config-missing": [cli,"run",str(project),"--server=false","--suppress-outdated-dependency-warning","--main-class","vn.scalaeng.p2.app.Main"]
}
history_file=project/"evidence/checks.json"
results = json.loads(history_file.read_text(encoding="utf-8")) if history_file.exists() else {}
for name in sys.argv[1:] or checks:
    task_env = dict(env)
    if name=="config-missing":
        for k in ("DB_URL","DB_USER","DB_PASSWORD","JWT_SECRET"):
            task_env.pop(k,None)
    completed = subprocess.run(checks[name],cwd=root,env=task_env,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,encoding="utf-8",errors="replace")
    (project/f"evidence/{name}.txt").write_text(completed.stdout,encoding="utf-8")
    results[name] = {"command": checks[name], "exit_code": completed.returncode,
        "verified_at_utc": datetime.now(timezone.utc).isoformat()}
    print(name+": exit="+str(completed.returncode),flush=True)
    expected=1 if name=="config-missing" else 0 # scala-cli maps non-zero app exits to 1
    if completed.returncode!=expected:
        print(completed.stdout[-12000:],flush=True)
        (project/"evidence/checks.json").write_text(json.dumps(results,indent=2),encoding="utf-8")
        sys.exit(1)
(project/"evidence/checks.json").write_text(json.dumps(results,indent=2),encoding="utf-8")

summary={}
for name in ("tests","solutions"):
    log=project/f"evidence/{name}.txt"
    if not log.exists(): continue
    clean=re.sub(r"\x1b\[[0-9;]*m","",log.read_text(encoding="utf-8"))
    rows=re.findall(r"Test run (\S+) finished: (\d+) failed, (\d+) ignored, (\d+) total",clean)
    skipped=len(re.findall(r"^==> s .* skipped ",clean,re.M))
    total=sum(int(r[3]) for r in rows)
    failed=sum(int(r[1]) for r in rows)
    summary[name]={"total":total,"passed":total-failed-skipped,"failed":failed,"assume_skipped":skipped,
        "compiler_warnings":len(re.findall(r"Unused Symbol Warning|\[warn\]|warnings? found",clean)),
        "suite_summaries":re.findall(r"^Test run .* finished: .*$",clean,re.M)}
review_record = project/"evidence/sbt-postgres-review-round1.json"
if review_record.exists():
    summary["sbt_postgres_round1"] = json.loads(review_record.read_text(encoding="utf-8"))
(project/"evidence/summary.json").write_text(json.dumps(summary,ensure_ascii=False,indent=2)+"\n",encoding="utf-8")
