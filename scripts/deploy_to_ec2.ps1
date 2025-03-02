$EC2_USER = "ec2-user"
$EC2_IP = "13.48.104.197"  # Zamień na rzeczywisty IP EC2
$KEY_PATH = "C:\Users\Maciek\PycharmProjects\grodt-processor-key.pem"
$LOCAL_PATH = "C:\Users\Maciek\PycharmProjects\getRichOrDieTryin"
$REMOTE_PATH = "/home/ec2-user/getRichOrDieTryin"

Write-Host "Copying project files to EC2..."

# Kopiowanie plików i katalogów, pomijając data/, .venv, tests, TESTZ
scp -i $KEY_PATH "$LOCAL_PATH\pyproject.toml" "$EC2_USER@$EC2_IP:$REMOTE_PATH/"
scp -i $KEY_PATH "$LOCAL_PATH\docker-compose.yml" "$EC2_USER@$EC2_IP:$REMOTE_PATH/"
scp -i $KEY_PATH "$LOCAL_PATH\.env.rds" "$EC2_USER@$EC2_IP:$REMOTE_PATH/"
scp -i $KEY_PATH -r "$LOCAL_PATH\data_processing" "$EC2_USER@$EC2_IP:$REMOTE_PATH/"
scp -i $KEY_PATH -r "$LOCAL_PATH\helpers" "$EC2_USER@$EC2_IP:$REMOTE_PATH/"
scp -i $KEY_PATH -r "$LOCAL_PATH\jobs" "$EC2_USER@$EC2_IP:$REMOTE_PATH/"
scp -i $KEY_PATH -r "$LOCAL_PATH\models" "$EC2_USER@$EC2_IP:$REMOTE_PATH/"
scp -i $KEY_PATH -r "$LOCAL_PATH\pipelines" "$EC2_USER@$EC2_IP:$REMOTE_PATH/"
scp -i $KEY_PATH -r "$LOCAL_PATH\scheduler" "$EC2_USER@$EC2_IP:$REMOTE_PATH/"
scp -i $KEY_PATH -r "$LOCAL_PATH\scripts" "$EC2_USER@$EC2_IP:$REMOTE_PATH/"
scp -i $KEY_PATH -r "$LOCAL_PATH\services" "$EC2_USER@$EC2_IP:$REMOTE_PATH/"

Write-Host "Deployment completed."

# Opcjonalne: Sprawdź logi na EC2
ssh -i $KEY_PATH $EC2_USER@$EC2_IP "ls -l /home/ec2-user/getRichOrDieTryin"