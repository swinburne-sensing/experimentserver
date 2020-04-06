# Allow scripts using Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser

if (!($influx_username = Read-Host "InfluxDB username [$env:UserName]")) { $influx_username = $env:UserName }
$influx_password = read-host "InfluxDB password"

[System.Environment]::SetEnvironmentVariable('INFLUXDB_USERNAME', $influx_username, [System.EnvironmentVariableTarget]::User)
[System.Environment]::SetEnvironmentVariable('INFLUXDB_PASSWORD', $influx_password, [System.EnvironmentVariableTarget]::User)
[System.Environment]::SetEnvironmentVariable('INFLUXDB_USER_DATABASE', 'experiment_' + $env:UserName, [System.EnvironmentVariableTarget]::User)
