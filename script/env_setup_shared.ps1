# Allow scripts using Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser

[System.Environment]::SetEnvironmentVariable('GRAFANA_URL', 'https://plot.gassensor.cloud.edu.au/grafana/d/Z71ZjFuWk/gas-sensing-results?from={}&to={}', [System.EnvironmentVariableTarget]::Machine)
[System.Environment]::SetEnvironmentVariable('INFLUXDB_URL', 'https://db.gassensor.cloud.edu.au/influxdb/', [System.EnvironmentVariableTarget]::Machine)
[System.Environment]::SetEnvironmentVariable('INFLUXDB_SHARED_DATABASE', 'experiment_shared', [System.EnvironmentVariableTarget]::Machine)
