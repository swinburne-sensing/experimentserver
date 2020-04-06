$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$experimentserverDir = Join-Path -Path $scriptDir -ChildPath ..
$venvDir = Join-Path -Path $experimentserverDir -ChildPath ..\_venv_win64

$scriptDir
$experimentserverDir
$venvDir

& "$venvDir\Scripts\Activate.ps1"
$env:PYTHONPATH= "$env:PYTHONPATH;$venvDir\Lib\site-packages;$experimentserverDir"
python -m experimentserver
