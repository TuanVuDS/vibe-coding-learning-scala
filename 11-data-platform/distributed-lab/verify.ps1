param(
    [ValidateSet('tests', 'demo', 'memory', 'serializers', 'all')]
    [string] $Mode = 'tests'
)
$ErrorActionPreference = 'Stop'
$t19Root = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '../..'))
$t19Cache = Join-Path $t19Root '.cache/t19'
New-Item -ItemType Directory -Force "$t19Cache/tmp", "$t19Cache/local" | Out-Null
$env:COURSIER_CACHE = Join-Path $t19Root '.cache/coursier'
$env:SPARK_LOCAL_DIRS = Join-Path $t19Cache 'local'
$env:TEMP = Join-Path $t19Cache 'tmp'
$env:TMP = $env:TEMP
$t19Tmp = $env:TEMP.Replace('\', '/')
$t19Warehouse = ([Uri]::new((Join-Path $t19Cache 'warehouse'))).AbsoluteUri

function Invoke-T19Scala([string] $Label, [string[]] $ScalaArgs) {
    $t19Common = @('--server=false', '--workspace', "$t19Cache/$Label-build",
        "--java-opt=-Djava.io.tmpdir=$t19Tmp", "--java-opt=-Dspark.sql.warehouse.dir=$t19Warehouse")
    $t19OldPreference = $ErrorActionPreference
    $ErrorActionPreference = 'Continue'
    try {
        & scala-cli.bat @ScalaArgs @t19Common *> "$t19Cache/$Label.log"
        $t19Code = $LASTEXITCODE
    } finally { $ErrorActionPreference = $t19OldPreference }
    Get-Content -Encoding UTF8 "$t19Cache/$Label.log" | Select-Object -Last 12
    Write-Output "EXIT label=$Label code=$t19Code"
    [pscustomobject]@{label=$Label; exitCode=$t19Code; time=(Get-Date -Format o)} |
        ConvertTo-Json -Compress | Add-Content -Encoding UTF8 "$t19Cache/executions.jsonl"
    if ($t19Code -ne 0) { throw "$Label failed (exit $t19Code); read $t19Cache/$Label.log" }
}
if ($Mode -in @('tests', 'all')) {
    Invoke-T19Scala 'lab-test' @('test', $PSScriptRoot)
    Invoke-T19Scala 'solutions-test' @('test', (Join-Path $t19Root 'solutions/ch32-code'))
}
if ($Mode -in @('demo', 'all')) {
    Invoke-T19Scala 'demo' @('run', $PSScriptRoot, '--main-class', 'vn.scalaeng.distributed.DistributedDemo')
}
if ($Mode -in @('memory', 'all')) {
    Invoke-T19Scala 'memory' @('run', $PSScriptRoot, '--main-class', 'vn.scalaeng.distributed.MemoryDemo', '--java-opt=-Xmx768m')
}
if ($Mode -in @('serializers', 'all')) {
    foreach ($t19Kind in @('Java', 'Kryo')) {
        Invoke-T19Scala "serializer-$t19Kind" @('run', $PSScriptRoot, '--main-class',
            'vn.scalaeng.distributed.SerializerDemo', "--java-opt=-Dch32.serializer=$t19Kind")
    }
}
