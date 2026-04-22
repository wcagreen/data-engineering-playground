param(
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$ExtraArgs
)

$repoRoot = Resolve-Path (Join-Path $PSScriptRoot "..\..")
Push-Location $repoRoot
try {
    & docker compose --env-file airflow.v3.env down -v @ExtraArgs
}
finally {
    Pop-Location
}
