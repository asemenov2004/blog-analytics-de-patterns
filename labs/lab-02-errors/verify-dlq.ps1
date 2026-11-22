param([string]$Results="$PSScriptRoot/spark-results.json")
$ErrorActionPreference='Stop'
Push-Location (Join-Path $PSScriptRoot '../..')
try {
    foreach ($run in (Get-Content -Raw $Results | ConvertFrom-Json)) {
        $topic=$run.topic+'_dlq'
        $rows=docker compose -f docker/compose.yml --profile core exec -T redpanda rpk topic describe $topic -p
        if ($LASTEXITCODE) { throw "Ошибка чтения $topic" }
        $row=@($rows | Where-Object { $_ -match '^\s*0\s+' })
        if ($row.Count -ne 1) { throw "Не найдена единственная партиция: $topic" }
        $cols=$row[0].Trim() -split '\s+'
        $actual=[long]$cols[-1]-[long]$cols[-2]
        $expected=[long]$run.outcomes.json+[long]$run.outcomes.schema
        if ($actual -ne $expected) { throw "Несовпадение DLQ: $actual / $expected" }
        Write-Output "$topic $actual"
    }
} finally { Pop-Location }
