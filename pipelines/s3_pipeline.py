import os
import zipfile
import boto3
import logging


class S3Uploader:
    def __init__(self, bucket_name: str, data_dir: str = "data"):
        self.s3_client = boto3.client("s3")
        self.bucket_name = bucket_name
        self.data_dir = data_dir

    def pack_and_upload(self, run_timestamp: str) -> None:
        """Zip all files from current run and upload to S3."""
        zip_filename = f"pipeline_run_{run_timestamp}.zip"
        zip_path = os.path.join(self.data_dir, zip_filename)

        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zipf:
            for root, _, files in os.walk(self.data_dir):
                for file in files:
                    if file.startswith(run_timestamp) and file.endswith(".csv"):
                        file_path = os.path.join(root, file)
                        zipf.write(file_path, file)
                        logging.info(f"Dodano {file} do archiwum")

        s3_key = f"runs/{zip_filename}"
        self.s3_client.upload_file(zip_path, self.bucket_name, s3_key)
        logging.info(f"Wysłano {zip_filename} do S3: {s3_key}")

        # Czyszczenie lokalnych plików
        os.remove(zip_path)
        for file in os.listdir(self.data_dir):
            if file.startswith(run_timestamp) and file.endswith(".csv"):
                os.remove(os.path.join(self.data_dir, file))
        logging.info("Wyczyszczono lokalne pliki")
