@echo off
set EC2_USER=ec2-user
set EC2_IP=13.48.104.197
set KEY_PATH=C:\Users\Maciek\PycharmProjects\grodt-processor-key.pem
set LOCAL_PATH=C:\Users\Maciek\PycharmProjects\getRichOrDieTryin
set REMOTE_PATH=/home/ec2-user/getRichOrDieTryin

REM Sprawdzenie i utworzenie katalogu, jeśli nie istnieje
ssh -i "%KEY_PATH%" %EC2_USER%@%EC2_IP% "mkdir -p %REMOTE_PATH%"

echo Copying project files to EC2...

REM Kopiowanie plików i katalogów, pomijając data/, .venv, tests, TESTZ
scp -i "%KEY_PATH%" "%LOCAL_PATH%\google_api_credentials.json" "%EC2_USER%@%EC2_IP%:%REMOTE_PATH%/"
scp -i "%KEY_PATH%" "%LOCAL_PATH%\pyproject.toml" "%EC2_USER%@%EC2_IP%:%REMOTE_PATH%/"
scp -i "%KEY_PATH%" "%LOCAL_PATH%\docker-compose.yml" "%EC2_USER%@%EC2_IP%:%REMOTE_PATH%/"
scp -i "%KEY_PATH%" "%LOCAL_PATH%\Dockerfile" "%EC2_USER%@%EC2_IP%:%REMOTE_PATH%/"
scp -i "%KEY_PATH%" "%LOCAL_PATH%\requirements.txt" "%EC2_USER%@%EC2_IP%:%REMOTE_PATH%/"
scp -i "%KEY_PATH%" "%LOCAL_PATH%\.env.rds" "%EC2_USER%@%EC2_IP%:%REMOTE_PATH%/"
scp -i "%KEY_PATH%" -r "%LOCAL_PATH%\config" "%EC2_USER%@%EC2_IP%:%REMOTE_PATH%/"
scp -i "%KEY_PATH%" -r "%LOCAL_PATH%\data_processing" "%EC2_USER%@%EC2_IP%:%REMOTE_PATH%/"
scp -i "%KEY_PATH%" -r "%LOCAL_PATH%\helpers" "%EC2_USER%@%EC2_IP%:%REMOTE_PATH%/"
scp -i "%KEY_PATH%" -r "%LOCAL_PATH%\jobs" "%EC2_USER%@%EC2_IP%:%REMOTE_PATH%/"
scp -i "%KEY_PATH%" -r "%LOCAL_PATH%\models" "%EC2_USER%@%EC2_IP%:%REMOTE_PATH%/"
scp -i "%KEY_PATH%" -r "%LOCAL_PATH%\pipelines" "%EC2_USER%@%EC2_IP%:%REMOTE_PATH%/"
scp -i "%KEY_PATH%" -r "%LOCAL_PATH%\scheduler" "%EC2_USER%@%EC2_IP%:%REMOTE_PATH%/"
scp -i "%KEY_PATH%" -r "%LOCAL_PATH%\scripts" "%EC2_USER%@%EC2_IP%:%REMOTE_PATH%/"
scp -i "%KEY_PATH%" -r "%LOCAL_PATH%\services" "%EC2_USER%@%EC2_IP%:%REMOTE_PATH%/"

echo Deployment completed.

REM Opcjonalne: Sprawdź logi na EC2
ssh -i "%KEY_PATH%" %EC2_USER%@%EC2_IP% "ls -l /home/ec2-user/getRichOrDieTryin"