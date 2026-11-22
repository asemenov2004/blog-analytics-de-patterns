param([int[]]$Sizes=@(1000,10000), [int]$Seed=42, [int]$Repeats=1)
$ErrorActionPreference='Stop'
Push-Location (Join-Path $PSScriptRoot '../..')
try {
    $compose=@('compose','-f','docker/compose.yml','--profile','core')
    docker @compose up -d --wait redpanda seaweedfs console
    if ($LASTEXITCODE) { throw 'core failed' }
    docker @compose run --rm generator bootstrap --skip-pg
    if ($LASTEXITCODE) { throw 'bootstrap failed' }
    $results=@()
    if ($Repeats -lt 1) { throw "Repeats must be positive" }
    foreach ($n in ($Sizes * $Repeats)) {
        if ($n -lt 1) { throw 'Размер должен быть положительным' }
        $topic='lab02_'+[guid]::NewGuid().ToString('N')
        $mount=(Resolve-Path $PSScriptRoot).Path+':/experiment:ro'
        $output=docker @compose run --rm -v $mount --entrypoint python generator /experiment/publish.py $topic $n --seed $Seed
        if ($LASTEXITCODE) { throw 'publish failed' }
        $truth=(@($output) -match '^TRUTH ' -replace '^TRUTH ','') | ConvertFrom-Json
        $output=docker @compose run --rm spark spark-submit /opt/pipelines/spark/errors/lab02.py $topic
        if ($LASTEXITCODE) { throw 'spark failed' }
        $r=(@($output) -match '^RESULT ' -replace '^RESULT ','') | ConvertFrom-Json
        if ($r.events -ne $n) { throw 'volume mismatch' }
        foreach ($pair in @(@($truth.outcomes,$r.outcomes),@($truth.hours,$r.views.static),@($truth.hours,$r.views.dynamic))) {
            if (@($pair[0].psobject.Properties).Count -ne @($pair[1].psobject.Properties).Count) { throw 'key mismatch' }
            foreach ($item in $pair[0].psobject.Properties) {
                if ($pair[1].psobject.Properties[$item.Name].Value -ne $item.Value) { throw "mismatch: $($item.Name)" }
            }
        }
        $r | Add-Member -NotePropertyName seed -NotePropertyValue $Seed
        $results+=@($r)
        ConvertTo-Json -InputObject $results -Depth 8 | Set-Content "$PSScriptRoot/spark-results.json" -Encoding utf8
    }
} finally { Pop-Location }



