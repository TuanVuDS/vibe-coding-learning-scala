"""Run from any directory. Logs and temporary data stay in the brief's scope."""
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[3]
env = dict(os.environ, COURSIER_CACHE=str(ROOT / '.cache/coursier'))
temp = ROOT / '.cache/t13-tmp'
temp.mkdir(parents=True, exist_ok=True)
env.update(TMP=str(temp), TEMP=str(temp))
cli = shutil.which('scala-cli.bat')

def run(name, args, log, integration=False):
    current = env.copy()
    current.pop('RUN_KAFKA_IT', None)
    if integration:
        current['RUN_KAFKA_IT'] = '1'
    command = [cli, *args, '--server=false', '--suppress-outdated-dependency-warning']
    if integration:
        command += ['--java-opt', f'-Djava.io.tmpdir={temp}']
    result = subprocess.run(command, cwd=ROOT, env=current, stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT, encoding='utf-8', errors='replace')
    output = re.sub(r'\x1b\[[0-9;]*m', '', result.stdout)
    (ROOT / log).write_text('COMMAND: ' + subprocess.list2cmdline(command) + '\n'
                            + output + f'\nEXIT: {result.returncode}\n', encoding='utf-8')
    print(f'{name}: exit={result.returncode}\n{output[-6000:]}', flush=True)
    return result.returncode

if __name__ == '__main__':
    tasks = {
        'kafka': (['test', '10-streaming/kafka-lab'], '10-streaming/kafka-lab/verification/tests.txt', False),
        'broker': (['test', '10-streaming/kafka-lab', '--test-only', '*BrokerSuite'], '10-streaming/kafka-lab/verification/broker.txt', True),
        'errors': (['run', '10-streaming/kafka-lab', '--main-class', 'vn.scalaeng.kafka.ErrorDemo'], '10-streaming/kafka-lab/verification/errors.txt', False),
        'streaming': (['test', '10-streaming/streaming-lab'], '10-streaming/streaming-lab/verification/tests.txt', False),
        'demo': (['run', '10-streaming/streaming-lab', '--main-class', 'vn.scalaeng.streaming.StreamingDemo'], '10-streaming/streaming-lab/verification/demo.txt', False),
        'ch27': (['test', 'solutions/ch27-code'], 'solutions/ch27-code/tests.txt', False),
        'ch28': (['test', 'solutions/ch28-code'], 'solutions/ch28-code/tests.txt', False),
    }
    selected = sys.argv[1:] or list(tasks)
    failures = [name for name in selected if run(name, *tasks[name]) != 0]
    sys.exit(bool(failures))
