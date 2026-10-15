param(
    [ValidateSet('payment', 'refund', 'aml', 'reorg')]
    [string]$Scenario = 'payment',
    [string]$BaseUrl = 'http://localhost:8080',
    [int]$Confirmations = 3
)
$ErrorActionPreference = 'Stop'
if ($Confirmations -lt 2) { throw 'Demo requires at least 2 confirmations' }
$partnerHeaders = @{ 'X-API-Key' = 'alpha-secret' }
$adminHeaders = @{ 'X-Admin-Key' = 'local-admin-key' }
function Send-Admin([string]$Path, $Body) {
    $argsForRequest = @{ Method = 'Post'; Uri = "$BaseUrl$Path"; Headers = $adminHeaders }
    if ($null -ne $Body) {
        $argsForRequest.ContentType = 'application/json'
        $argsForRequest.Body = ConvertTo-Json -InputObject $Body -Compress
    }
    Invoke-RestMethod @argsForRequest
}
$partnerHeaders['Idempotency-Key'] = [guid]::NewGuid().ToString('N')
$invoice = Invoke-RestMethod -Method Post -Uri "$BaseUrl/v1/invoices" -Headers $partnerHeaders -ContentType 'application/json' -Body '{"amount":"100.000000"}'
$sender = '0x0000000000000000000000000000000000000001'
if ($Scenario -eq 'refund') { $sender = '0x0000000000000000000000000000000000000bad' }
try {
    if ($Scenario -eq 'aml') { Send-Admin '/sim/aml' @{ down = $true } | Out-Null }
    Send-Admin '/sim/transfers' @{
        address = $invoice.address; sender = $sender; amount = '100.000000'
        tx_hash = '0x' + [guid]::NewGuid().ToString('N') + [guid]::NewGuid().ToString('N'); log_index = 0
    } | Out-Null
    if ($Scenario -eq 'reorg') {
        Send-Admin '/sim/reorg' @{ blocks = 1 } | Out-Null
    } else {
        Send-Admin '/sim/mine' @{ blocks = $Confirmations - 1 } | Out-Null
    }
    Send-Admin '/sim/process' $null | Out-Null
    Invoke-RestMethod -Uri "$BaseUrl/v1/invoices/$($invoice.id)" -Headers $partnerHeaders | ConvertTo-Json -Depth 8
    Invoke-RestMethod -Uri "$BaseUrl/v1/balance" -Headers $partnerHeaders | ConvertTo-Json
    if ($Scenario -eq 'aml') {
        Send-Admin '/sim/aml' @{ down = $false } | Out-Null
        Start-Sleep -Seconds 3
        Send-Admin '/sim/process' $null | Out-Null
        Invoke-RestMethod -Uri "$BaseUrl/v1/invoices/$($invoice.id)" -Headers $partnerHeaders | ConvertTo-Json -Depth 8
        Invoke-RestMethod -Uri "$BaseUrl/v1/balance" -Headers $partnerHeaders | ConvertTo-Json
    }
    if ($Scenario -eq 'refund') {
        Invoke-RestMethod -Uri "$BaseUrl/v1/refunds" -Headers $partnerHeaders | ConvertTo-Json -Depth 5
    }
} finally {
    if ($Scenario -eq 'aml') { Send-Admin '/sim/aml' @{ down = $false } | Out-Null }
}
