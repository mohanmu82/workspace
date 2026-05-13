$pomPath = "$PSScriptRoot\pom.xml"
$content = Get-Content $pomPath -Raw

if ($content -match '<artifactId>batchworkflow</artifactId>\s*<version>(\d+)\.(\d+)\.(\d+)</version>') {
    $major = $matches[1]
    $minor = $matches[2]
    $oldPatch = [int]$matches[3]
    $newPatch = $oldPatch + 1
    $oldVersion = "$major.$minor.$oldPatch"
    $newVersion = "$major.$minor.$newPatch"

    $content = $content -replace '(<artifactId>batchworkflow</artifactId>\s*<version>)\d+\.\d+\.\d+(</version>)', "`${1}$newVersion`${2}"
    Set-Content $pomPath $content -Encoding UTF8 -NoNewline

    Write-Host "Version bumped: $oldVersion -> $newVersion"
} else {
    Write-Error "Could not find project version in pom.xml"
    exit 1
}

Set-Location $PSScriptRoot
mvn deploy
