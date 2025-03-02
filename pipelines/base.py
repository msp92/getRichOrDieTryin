from abc import ABC, abstractmethod
import logging
from typing import Dict, Any
import pandas as pd

from models.base import BaseMixin
from services.db import Db


class BasePipeline(ABC):
    """
    Base class that defines fetch, process, and load phases.
    Subclasses override the three methods or keep the default no-op behavior.
    """

    def __init__(self, entity_config: Dict[str, Dict[str, Any]]):
        self.db = Db()
        BaseMixin.set_db(self.db)
        self.entity_config = entity_config
        self._current_entity_name: str = ""
        self._current_df: pd.DataFrame = pd.DataFrame()

    @property
    def config(self) -> Dict[str, Any]:
        """
        Returns part of config for currently processed entity.
        """
        return self.entity_config[self._current_entity_name]

    @abstractmethod
    def fetch(self) -> None:
        """
        Fetch data from external sources (API, files, etc.).
        """
        pass

    @abstractmethod
    def process(self) -> None:
        """
        Transform or parse the data into a format suitable for loading.
        """
        pass

    @abstractmethod
    def load(self) -> None:
        """
        Insert or upsert processed data into the target database.
        """
        pass

    def export(self) -> None:
        """Export data to external format (e.g., CSV, XLSX)."""
        pass

    def _run_for_entity(self) -> None:
        """
        Orchestrates the full pipeline: fetch -> process -> load.
        """
        logging.info(f"Starting pipeline: {type(self).__name__}")
        self.fetch()
        self.process()
        self.load()
        logging.info(f"Finished pipeline: {type(self).__name__}")

    def run(self, method: str = "run") -> None:
        methods = {"fetch": self.fetch, "process": self.process, "load": self.load}
        for entity_name in self.entity_config.keys():
            logging.info(
                f"--- Running {type(self).__name__} for entity: {entity_name} ---"
            )
            self._current_entity_name = entity_name
            self._current_df = pd.DataFrame()  # reset DF
            if method not in ("run", ""):  # Treat '' as 'run'
                logging.info(f"Running {method} for entity: {entity_name}")
                if "+" in method:
                    for sub_method in method.split("+"):
                        methods[sub_method]()
                else:
                    methods[method]()
            else:
                self._run_for_entity()
