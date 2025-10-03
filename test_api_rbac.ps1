# Test API with different users

$token = "0e60a3fcb1d4c155fb9ca5835650af37ca5d7ec3a7e42b68ae284eb38d74883e"
$baseUrl = "http://10.200.0.2:8080"

Write-Host "`nTesting API with RBAC...`n"

# Test 1: Harold (CEO) - Should access all tables
Write-Host "Test 1: Harold accessing invoices..."
$response = Invoke-RestMethod -Uri "$baseUrl/api/execute-query" `
    -Method POST `
    -Headers @{
        "Authorization" = "Bearer $token"
        "X-Username" = "harold"
        "Content-Type" = "application/json"
    } `
    -Body '{"query": "SELECT KRFNR, KRBLF FROM DCPO.KRKFAKTR FETCH FIRST 5 ROWS ONLY"}'

Write-Host "Success: $($response.success)"
Write-Host "Rows: $($response.data.row_count)`n"

# Test 2: Pontus (Call Center) - Should NOT access credit limits
Write-Host "Test 2: Pontus accessing credit limits (should fail)..."
try {
    $response = Invoke-RestMethod -Uri "$baseUrl/api/execute-query" `
        -Method POST `
        -Headers @{
            "Authorization" = "Bearer $token"
            "X-Username" = "pontus"
            "Content-Type" = "application/json"
        } `
        -Body '{"query": "SELECT KHKNR, KHKGÄ FROM DCPO.KHKNDHUR FETCH FIRST 5 ROWS ONLY"}'
    
    Write-Host "Unexpected: Query succeeded (should have failed)"
} catch {
    Write-Host "Expected: Access denied`n"
}

# Test 3: Lars (Finance) - Should access invoices
Write-Host "Test 3: Lars accessing invoices..."
$response = Invoke-RestMethod -Uri "$baseUrl/api/execute-query" `
    -Method POST `
    -Headers @{
        "Authorization" = "Bearer $token"
        "X-Username" = "lars"
        "Content-Type" = "application/json"
    } `
    -Body '{"query": "SELECT KRFNR, KRBLF FROM DCPO.KRKFAKTR FETCH FIRST 5 ROWS ONLY"}'

Write-Host "Success: $($response.success)"
Write-Host "Rows: $($response.data.row_count)`n"