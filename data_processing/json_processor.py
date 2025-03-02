import logging
from multiprocessing import Pool
import pandas as pd
from typing import Callable, List

logging.basicConfig(level=logging.INFO)


class JsonProcessor:
    def __init__(
        self,
        parse_method: Callable[[str], pd.DataFrame],
        batch_size: int = 100,
        num_processes: int = 2,
    ):
        """
        Initializes the JsonProcessor class.

        Args:
            parse_method: A function that parses a single JSON file into a DataFrame.
            batch_size: The size of the batch of files to process in one go.
            num_processes: The number of parallel processes.
        """
        self.parse_method = parse_method
        self.batch_size = batch_size
        self.num_processes = num_processes

    def process_batch(self, batch: List[str]) -> pd.DataFrame:
        """
        Processes a batch of files and performs an upsert.

        Args:
            batch: A list of paths to JSON files.
        """
        dataframes = []
        for file_path in batch:
            try:
                df = self.parse_method(file_path)
                if not df.empty:
                    dataframes.append(df)
                else:
                    logging.warning(f"Empty DataFrame from {file_path}")
            except Exception as e:
                logging.error(f"Error parsing {file_path}: {e}")

        return pd.concat(dataframes) if dataframes else pd.DataFrame()

    def process_files(self, file_paths: List[str]) -> pd.DataFrame:
        """Process all files in parallel batches and return the combined DataFrame."""
        total_files = len(file_paths)
        if total_files == 0:
            logging.warning("No files provided to process")
            return pd.DataFrame()
        logging.info(
            f"Processing {total_files} files with {self.num_processes} processes, batch_size={self.batch_size}"
        )
        batches = [
            file_paths[i : i + self.batch_size]
            for i in range(0, total_files, self.batch_size)
        ]
        with Pool(processes=self.num_processes) as pool:
            results = pool.map(self.process_batch, batches)
        logging.info("Completed processing all files")
        return pd.concat(results) if results else pd.DataFrame()
