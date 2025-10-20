param(
    [switch]$SkipClient,
    [switch]$SkipEndpoints
)

$ErrorActionPreference = 'Stop'

# Ensure venv
if (-not (Test-Path -Path "venv\Scripts\python.exe")) {
    Write-Host "venv not found. Please create and install dependencies first." -ForegroundColor Yellow
    exit 1
}

# Activate venv for this session
$env:VIRTUAL_ENV = (Resolve-Path "venv").Path
$env:PATH = (Resolve-Path "venv\Scripts").Path + ";" + $env:PATH

# UTF-8 console
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8

if (-not $SkipClient) {
    Write-Host "Running client test (WorldBankDataProvider) ..." -ForegroundColor Cyan
    python scripts\test_worldbank_client.py
}

if (-not $SkipEndpoints) {
    Write-Host "Running endpoint tests (server http://localhost:8000 required) ..." -ForegroundColor Cyan
    try {
        python scripts\test_worldbank_endpoints.py
    } catch {
        Write-Host "Endpoint tests failed. Is the server running?" -ForegroundColor Red
        exit 2
    }
}

Write-Host "Done." -ForegroundColor Green
