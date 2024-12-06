import logging
import time
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import List

from data_processing.data_processing import (
    load_all_files_paths_from_data_directory,
)

logging.basicConfig(
    level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s"
)


# TODO: replace daily_refresh logic with below method
# NOTE: ensure that it will work wherever executed


class JSONProcessor:
    def __init__(
        self, entity: str, parse_method, upsert_method, input_dir: str, chunk_size: int
    ):
        self.entity = entity
        self.files = load_all_files_paths_from_data_directory(entity)
        self.parse_method = parse_method
        self.upsert_method = upsert_method
        self.input_dir = input_dir
        self.chunk_size = chunk_size

    def process_files_chunk(
        self, files_chunk: List[Path], chunk_index: int, total_chunks: int
    ) -> pd.DataFrame:
        logging.debug(
            f"Processing chunk {chunk_index + 1}/{total_chunks} with {len(files_chunk)} files for entity={self.entity}"
        )

        dfs = []
        total_files = len(files_chunk)
        last_logged_time = time.time()

        for idx, file in enumerate(files_chunk):
            try:
                # Log progress in percentage for the current chunk
                progress = (idx + 1) / total_files * 100
                current_time = time.time()

                # Log progress every minute
                if current_time - last_logged_time >= 60:
                    logging.debug(
                        f"Parsing chunk {chunk_index + 1}/{total_chunks} - File {idx + 1}/{total_files} ({progress:.2f}% complete)"
                    )
                    last_logged_time = current_time

                # Parse the file
                file_name = file.name
                df = self.parse_method(file_name)
                dfs.append(df)
            except Exception as e:
                logging.error(f"Failed to parse file {file}: {e}")

        if dfs:
            logging.debug(f"Concatenating {len(dfs)} DataFrames for chunk")
            return pd.concat(dfs, ignore_index=True)
        else:
            logging.warning("No DataFrames created in this chunk")
            return pd.DataFrame()

    def run_multiprocessing(self) -> None:
        # Split files into chunks
        file_chunks = [
            self.files[i : i + self.chunk_size]
            for i in range(0, len(self.files), self.chunk_size)
        ]

        total_chunks = len(file_chunks)
        logging.debug(
            f"Split files into {total_chunks} chunks of up to {self.chunk_size} files each."
        )

        # with ProcessPoolExecutor() as executor:
        with ThreadPoolExecutor(max_workers=8) as executor:
            # Create a list of arguments for process_files_chunk function
            chunk_args = [
                (chunk, idx, total_chunks) for idx, chunk in enumerate(file_chunks)
            ]
            results = executor.map(self._process_chunk_wrapper, chunk_args)

            for chunk_df in results:
                if not chunk_df.empty:
                    logging.debug(
                        f"Upserting chunk with {len(chunk_df)} records to the database"
                    )
                    self.upsert_to_database(chunk_df)
                else:
                    logging.warning("Received an empty DataFrame from a chunk")

    def _process_chunk_wrapper(self, chunk_args) -> pd.DataFrame:
        # Unpack the arguments
        chunk, chunk_index, total_chunks = chunk_args
        return self.process_files_chunk(chunk, chunk_index, total_chunks)

    def upsert_to_database(self, df: pd.DataFrame) -> None:
        logging.info(
            f"Upserting {len(df)} records to the database for entity={self.entity}"
        )
        try:
            self.upsert_method(df)
        except Exception as e:
            logging.error(f"Failed to upsert records to the database: {e}")
