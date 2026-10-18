param(
    [ValidateSet('tests', 'demo', 'memory', 'serializers', 'all')]
    [string] $Mode = 'tests'
)
$ErrorActionPreference = 'Stop'
$t12Root = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '../..'))
$t12Cache = Join-Path $t12Root '.cache/t12'
New-Item -ItemType Directory -Force "$t12Cache/tmp", "$t12Cache/local" | Out-Null
$env:COURSIER_CACHE = Join-Path $t12Root '.cache/coursier'
$env:SPARK_LOCAL_DIRS = Join-Path $t12Cache 'local'
$t12Tmp = (Join-Path $t12Cache 'tmp').Replace('\', '/')
$t12Warehouse = ([Uri]::new((Join-Path $t12Cache 'warehouse'))).AbsoluteUri

function Invoke-T12Scala([string] $Label, [string[]] $ScalaArgs) {
    $t12Common = @('--server=false', '--workspace', "$t12Cache/$Label-build",
        "--java-opt=-Djava.io.tmpdir=$t12Tmp", "--java-opt=-Dspark.sql.warehouse.dir=$t12Warehouse")
    # Windows PowerShell wraps JVM stderr in NativeCommandError even for WARN/hint.
    # Use the process exit code for success, not presence of stderr.
    $t12OldPreference = $ErrorActionPreference
    $ErrorActionPreference = 'Continue'
    try {
        & scala-cli.bat @ScalaArgs @t12Common *> "$t12Cache/$Label.log"
        $t12Code = $LASTEXITCODE
    } finally { $ErrorActionPreference = $t12OldPreference }
    Get-Content -Encoding UTF8 "$t12Cache/$Label.log" | Select-Object -Last 8
    if ($t12Code -ne 0) { throw "$Label failed (exit $t12Code); read $t12Cache/$Label.log" }
}

if ($Mode -in @('tests', 'all')) {
    Invoke-T12Scala 'lab-test' @('test', $PSScriptRoot)
    Invoke-T12Scala 'solutions-test' @('test', (Join-Path $t12Root 'solutions/ch25-code'))
}
if ($Mode -in @('demo', 'all')) {
    Invoke-T12Scala 'demo' @('run', $PSScriptRoot, '--main-class', 'vn.scalaeng.sparkperf.PerformanceDemo')
}
if ($Mode -in @('memory', 'all')) {
    Invoke-T12Scala 'memory' @('run', $PSScriptRoot, '--main-class', 'vn.scalaeng.sparkperf.MemoryDemo', '--java-opt=-Xmx768m')
}
if ($Mode -in @('serializers', 'all')) {
    foreach ($t12Kind in @('Java', 'Kryo')) {
        # Program arguments must follow all Scala CLI options, including common options.
        # A JVM property avoids placing the `--` separator before common options.
        Invoke-T12Scala "serializer-$t12Kind" @('run', $PSScriptRoot, '--main-class',
            'vn.scalaeng.sparkperf.SerializationDemo', "--java-opt=-Dch25.serializer=$t12Kind")
    }
}
