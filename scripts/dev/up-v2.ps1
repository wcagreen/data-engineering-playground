param(
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$ExtraArgs
)

$repoRoot = Resolve-Path (Join-Path $PSScriptRoot "..\..")
Push-Location $repoRoot
try {
    & docker compose --env-file airflow.v2.env up --build -d --remove-orphans @ExtraArgs
}
finally {
    Pop-Location
}
