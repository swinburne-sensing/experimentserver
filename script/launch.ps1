Add-Type -AssemblyName System.Windows.Forms

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$experimentserverDir = Join-Path -Path $scriptDir -ChildPath .. | Resolve-Path
$experimentserverDir = $experimentserverDir.Path
$projectDir = Join-Path -Path $experimentserverDir -ChildPath projects
$venvDir = Join-Path -Path $experimentserverDir -ChildPath venv_win

& "$venvDir\Scripts\Activate.ps1"

if (-Not (Test-Path "env:INFLUXDB_V2_BUCKET")) {
    $FileBrowser = New-Object System.Windows.Forms.OpenFileDialog -Property @{
        InitialDirectory = $projectDir
        Filter = 'Environment Variable Script (*.ps1)|*.ps1'
		Title = 'Select Project Variable File'
    }

    $null = $FileBrowser.ShowDialog()
	
	if (-Not ($FileBrowser.FileName)) {
		throw 'No project selected'
	}
	
    & $FileBrowser.FileName
}

$env:PYTHONPATH= "$env:PYTHONPATH;$venvDir\Lib\site-packages;$experimentserverDir"
python -m experimentserver $args
