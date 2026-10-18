$ErrorActionPreference = 'Stop'
$repoRoot = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
$labRoot = $PSScriptRoot
$env:COURSIER_CACHE = Join-Path $repoRoot '.cache/coursier'
$cases = @(
    @('Batch', 'batch size must be positive'),
    @('MatchStuck', 'could not be fully reduced'),
    @('RuntimeInline', 'Cannot reduce'),
    @('BadTopic', 'invalid topic literal'),
    @('RuntimeTopic', 'topic must be a constant string'),
    @('SameFile', 'Cannot call macro'),
    @('MissingCellEncoder', 'No given instance of type vn.scalaeng.ch08c.CellEncoder[List[String]] was found')
)
foreach ($case in $cases) {
    $caseName = $case[0]
    $workDir = Join-Path $repoRoot ('.cache/t22/negative/' + $caseName)
    New-Item -ItemType Directory -Force -Path $workDir | Out-Null
    $source = Join-Path $workDir ($caseName + '.scala')
    Copy-Item -LiteralPath (Join-Path $labRoot ('negative/' + $caseName + '.scala.txt')) -Destination $source
    $log = Join-Path $workDir 'compiler.log'
    $projectFile = Join-Path $labRoot 'project.scala'
    $mainSources = Join-Path $labRoot 'src/main/scala'
    # ProcessStartInfo giữ stderr nguyên bản, không thêm wrapper NativeCommandError của PowerShell 5.
    $start = New-Object System.Diagnostics.ProcessStartInfo
    $start.FileName = 'cmd.exe'
    $start.Arguments = '/d /s /c "scala-cli.bat compile "' + $projectFile + '" "' + $mainSources + '" "' + $source + '" --server=false --workspace "' + $workDir + '" --color never > "' + $log + '" 2>&1"'
    $start.WorkingDirectory = $repoRoot
    $start.UseShellExecute = $false
    $start.CreateNoWindow = $true
    $process = [System.Diagnostics.Process]::Start($start)
    $process.WaitForExit()
    $output = [System.IO.File]::ReadAllText($log)
    if ($process.ExitCode -ne 1 -or -not $output.Contains($case[1])) {
        Write-Output $output
        throw ('Unexpected diagnostic for ' + $caseName + ': exit=' + $process.ExitCode)
    }
    Write-Output ($caseName + ': expected failure (exit 1), diagnostic matched')
}
Write-Output ('Negative compilation: ' + $cases.Count + '/' + $cases.Count + ' expected failures verified.')
