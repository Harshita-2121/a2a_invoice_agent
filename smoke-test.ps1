# Safe protocol smoke test for the deployed A2A invoice agent.
# It creates one synthetic proposal task only; it does not execute or pay anything.
# Set $base to the public A2A base URL shown in your Agent Card.
$base = "https://tds-a2a-invoice-agent.onrender.com/a2a/"
$endpoint = $base.TrimEnd('/') + "/message:send"
$messageId = "smoke-" + [guid]::NewGuid().ToString("N")

$payload = @{
  message = @{
    messageId = $messageId
    role = "ROLE_USER"
    parts = @(
      @{
        mediaType = "application/vnd.gas.invoice-claim-batch+json"
        data = @{
          batchId = "smoke-batch-$messageId"
          policyRevision = "smoke-policy-v1"
          packages = @(
            @{
              packageId = "smoke-package-1"
              vendorName = "Example Supplies Ltd"
              invoiceNumber = "SMOKE-INV-001"
              amountMinor = 12345
              currency = "INR"
              documents = @(
                "[SMOKE-REF-1] Example invoice is matched to the purchase order.",
                "[SMOKE-REF-2] Goods receipt confirms delivery.",
                "[SMOKE-REF-3] Current policy confirms approval within autonomous authority."
              )
            }
          )
        }
      }
    )
  }
  configuration = @{
    returnImmediately = $false
    historyLength = 20
    acceptedOutputModes = @(
      "application/vnd.gas.invoice-action-proposals+json",
      "application/vnd.gas.invoice-action-receipts+json"
    )
  }
} | ConvertTo-Json -Depth 30

try {
  $response = Invoke-WebRequest -Uri $endpoint -Method Post `
    -Headers @{
      Authorization = "Bearer smoke-test-token"
      "A2A-Version" = "1.0"
      Accept = "application/a2a+json"
    } `
    -ContentType "application/a2a+json" `
    -Body $payload
  Write-Host "HTTP status:" $response.StatusCode
  Write-Host "Response body:"
  $response.Content
} catch {
  Write-Host "Request failed:" $_.Exception.Message
  if ($_.Exception.Response) {
    Write-Host "HTTP status:" ([int]$_.Exception.Response.StatusCode)
    try {
      $reader = New-Object System.IO.StreamReader($_.Exception.Response.GetResponseStream())
      Write-Host "Response body:" $reader.ReadToEnd()
    } catch {}
  }
  Write-Host "If this is still HTTP 400, inspect Render -> Logs and copy the JSON error code. Do not use another GA3 Check yet."
}
