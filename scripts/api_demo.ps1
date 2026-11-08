# Walk through the gateway API with curl (Windows PowerShell).
#   .\scripts\api_demo.ps1                 (cluster must be running)
# Each step prints the command's HTTP status and headers/body, which is
# what the report screenshots should show.

param(
    [string]$Base = "http://localhost:8080",
    [string]$Key = "dev-admin-key-change-me",
    [string]$Bucket = "curl-demo"
)
$ErrorActionPreference = "Continue"
$auth = "X-API-Key: $Key"
function Step($title) { Write-Host "`n=== $title ===" -ForegroundColor Cyan }

Step "1. Health (no key needed)"
curl.exe -s "$Base/health"

Step "2. Request without a key -> 401"
curl.exe -s -i "$Base/buckets"

Step "3. Create an erasure-coded bucket (4+2)"
curl.exe -s -X POST "$Base/buckets" -H $auth -H "Content-Type: application/json" `
    --data '{\"name\":\"curl-demo\",\"redundancy\":\"ec\",\"k\":4,\"m\":2}'

Step "4. List buckets"
curl.exe -s "$Base/buckets" -H $auth

Step "5. Upload an object (streaming PUT with tags)"
"Hello from the distributed storage gateway! 0123456789" | Out-File -Encoding ascii -NoNewline demo.txt
curl.exe -s -X PUT "$Base/buckets/$Bucket/objects/docs/demo.txt" -H $auth `
    -H "Content-Type: text/plain" -H "X-Tags: project=midterm,owner=student" `
    --data-binary "@demo.txt"

Step "6. Download it"
curl.exe -s "$Base/buckets/$Bucket/objects/docs/demo.txt" -H $auth

Step "7. Byte range: bytes=0-4 -> 206 Partial Content"
curl.exe -s -i "$Base/buckets/$Bucket/objects/docs/demo.txt" -H $auth -H "Range: bytes=0-4"

Step "8. HEAD (metadata in headers)"
curl.exe -s -I "$Base/buckets/$Bucket/objects/docs/demo.txt" -H $auth

Step "9. Object metadata (versions, chunks, shard placement)"
curl.exe -s "$Base/buckets/$Bucket/metadata/docs/demo.txt" -H $auth

Step "10. List objects with prefix"
curl.exe -s "$Base/buckets/$Bucket/objects?prefix=docs/" -H $auth

Step "11. Search by tag, size and date"
curl.exe -s "$Base/search?tags=project=midterm&min_size=10&created_after=2026-01-01T00:00:00Z" -H $auth

Step "12. Multipart upload (2 parts)"
$up = curl.exe -s -X POST "$Base/buckets/$Bucket/uploads" -H $auth -H "Content-Type: application/json" `
    --data '{\"key\":\"big/multipart.bin\"}' | ConvertFrom-Json
$id = $up.upload_id
"part-one " * 50000 | Out-File -Encoding ascii -NoNewline part1.bin
"part-two " * 50000 | Out-File -Encoding ascii -NoNewline part2.bin
curl.exe -s -X PUT "$Base/uploads/$id/parts/1" -H $auth --data-binary "@part1.bin"
curl.exe -s -X PUT "$Base/uploads/$id/parts/2" -H $auth --data-binary "@part2.bin"
curl.exe -s -X POST "$Base/uploads/$id/complete" -H $auth

Step "13. Cluster status"
curl.exe -s "$Base/cluster/status" -H $auth

Step "14. Rate limiting: a key limited to 1 request/second -> 429"
$k = curl.exe -s -X POST "$Base/admin/api-keys" -H $auth -H "Content-Type: application/json" `
    --data '{\"name\":\"demo-limited\",\"role\":\"readonly\",\"rate_limit_rps\":1}' | ConvertFrom-Json
1..6 | ForEach-Object {
    $code = curl.exe -s -o NUL -w "%{http_code}" "$Base/buckets" -H "X-API-Key: $($k.api_key)"
    Write-Host "request $_ -> HTTP $code"
}
curl.exe -s -i "$Base/buckets" -H "X-API-Key: $($k.api_key)"

Step "15. Read-only key cannot write -> 403"
curl.exe -s -i -X PUT "$Base/buckets/$Bucket/objects/x.txt" -H "X-API-Key: $($k.api_key)" --data "x"
curl.exe -s -X DELETE "$Base/admin/api-keys/$($k.key_id)" -H $auth | Out-Null

Remove-Item demo.txt, part1.bin, part2.bin -ErrorAction SilentlyContinue
Write-Host "`nDone. Delete the demo bucket with:" -ForegroundColor Green
Write-Host "curl.exe -X DELETE `"$Base/buckets/$Bucket?force=true`" -H `"$auth`""
