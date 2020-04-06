# Allow scripts using Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser

$api_key = read-host "Pushover API Key"
$user_key = read-host "Pushover User Key"

[System.Environment]::SetEnvironmentVariable('PUSHOVER_API_KEY', $api_key, [System.EnvironmentVariableTarget]::User)
[System.Environment]::SetEnvironmentVariable('PUSHOVER_USER_KEY', $user_key, [System.EnvironmentVariableTarget]::User)
