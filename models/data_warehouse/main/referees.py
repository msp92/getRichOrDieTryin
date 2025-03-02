import difflib
import logging
import pandas as pd
import re

from config.entity_names import REFEREES_DIR
from config.vars import ROOT_DIR, DATA_DIR
from helpers.utils import append_data_to_csv


class Referee:
    __table__ = None

    @staticmethod
    def get_names_similarity(incoming_name: str, existing_name: str) -> float:
        return difflib.SequenceMatcher(None, incoming_name, existing_name).ratio()

    @staticmethod
    def find_initial_with_period(name: str) -> str | None:
        # Extract initials from names in the format "J. Doe"
        match = re.match(r"([A-Z]\.)", name)
        if match:
            return match.group(1)  # Return the initial part (e.g., "J.")
        return None

    @classmethod
    def map_referee_name(cls, referee_name: str) -> str | None:
        mapping_df = pd.read_csv(
            f"{ROOT_DIR}/{DATA_DIR}/{REFEREES_DIR}/mapping_referees.csv"
        )

        # Check for exact matches in mapping
        exact_match = mapping_df[mapping_df["original_name"] == referee_name]

        if not exact_match.empty:
            return str(exact_match.iloc[0]["gold_name"])

        # Check for similar names if no exact match is found
        similar_names = mapping_df["original_name"].apply(
            lambda x: (x, cls.get_names_similarity(referee_name, x))
        )

        similar_names = similar_names[
            similar_names.apply(lambda x: x[1] > 0.75)
        ]  # Adjust threshold as needed

        # Automatically add to mapping if similarity is > 0.95
        high_similarity_names = [
            (name, similarity)
            for name, similarity in similar_names
            if similarity > 0.95
        ]
        similar_names = sorted(similar_names, key=lambda x: x[1], reverse=True)

        if high_similarity_names:
            # Automatically take the gold_name of the highest similarity match
            name, similarity = high_similarity_names[0]
            mapped_name: str = mapping_df[mapping_df["original_name"] == name].iloc[0][
                "gold_name"
            ]
            logging.info(
                f"Auto-mapping '{referee_name}' to '{mapped_name}' (Similarity: {similarity:.2f})"
            )
            append_data_to_csv(
                [referee_name, mapped_name],
                f"{ROOT_DIR}/{DATA_DIR}/{REFEREES_DIR}/mapping_referees.csv",
            )
            return mapped_name
        elif similar_names:
            # If incoming_name and original_name contains of initials, and they differ then exclude them
            incoming_initial = cls.find_initial_with_period(referee_name)
            similar_initial = cls.find_initial_with_period(similar_names[0][0])
            if (similar_initial and not incoming_initial) or (
                not similar_initial and incoming_initial
            ):
                logging.info("??? Assess what to do in such case ???")  # FIXME
            if (
                similar_initial
                and incoming_initial
                and not similar_initial == incoming_initial
            ):
                logging.info("Found similar names, but initials differs.")
                append_data_to_csv(
                    referee_name,
                    f"{ROOT_DIR}/{DATA_DIR}/{REFEREES_DIR}/new_referees.csv",
                )
                return None

            # Present similar names to the user, with corresponding gold_names
            logging.info(f"Similar names found for '{referee_name}':")
            for idx, (name, similarity) in enumerate(similar_names, start=1):
                gold_name = mapping_df[mapping_df["original_name"] == name].iloc[0][
                    "gold_name"
                ]
                logging.info(
                    f"{idx}. '{name}' (similarity: {similarity:.2f}) -> gold_name: '{gold_name}'"
                )

            # # Ask the user to select a match by pressing 1/2/3/etc.
            # choice = input(
            #     f"Select mapping for '{referee_name}' (or press Enter to skip): "
            # ).strip()
            #
            # if choice.isdigit():
            #     selected_idx = int(choice) - 1  # Convert input to zero-based index
            #     if 0 <= selected_idx < len(similar_names):
            #         name, _ = similar_names[selected_idx]
            #         # Find the gold_name corresponding to the selected name
            #         selected_gold_name: str = mapping_df[
            #             mapping_df["original_name"] == name
            #         ].iloc[0]["gold_name"]
            #         # Add the new mapping to the list
            #         append_data_to_csv(
            #             [referee_name, selected_gold_name],
            #             f"{ROOT_DIR}/{DATA_DIR}/{REFEREES_DIR}/mapping_referees.csv",
            #         )
            #         return selected_gold_name
            # Add the new mapping to the list
            append_data_to_csv(
                referee_name, f"{ROOT_DIR}/{DATA_DIR}/{REFEREES_DIR}/new_referees.csv"
            )
            return None
        return None
