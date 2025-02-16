# getRichOrDieTryin

## Project Description
A brief description of the project's purpose and functionality. What does the application do, and who is it for?

## Project Structure
```
getRichOrDieTryin/
├── data_processing/    # Aggregations, parsing, processing and transformation utilities
├── helpers/            # Utility methods
├── jobs/               # Automation scripts
│   ├── config/         # Configuration files for pipelines
│   ├── executors/      # Scripts that initialize and execute pipelines
│   ├── exporters/      # Handles exporting data to different destinations (CSV, Excel, cloud storage)
│   ├── pipelines/      # Pipeline classes for different data flows
│   ├── scheduler.py    # Orchestrates pipeline execution
├── models/             # Data models
│   ├── analytics/      # Analytical data models
│   ├── data_warehouse/ # Core data models
├── services/           # Service integration modules
│   ├── fetchers/       # API data fetchers
│   ├── db.py           # Database interaction
├── .gitignore          # File to exclude unnecessary files from the repository
├── Makefile            # Task automation
├── pyproject.toml      # Python package configuration
├── requirements.txt    # List of dependencies
└── README.md           # Project documentation
```

## Models
The `models` directory contains structured data models used throughout the application:
- **analytics/**: Models for aggregated analytical datasets, including team statistics, player performance, and game events.
- **data_warehouse/**: Structured models representing core data used in analytical processing and reporting, organized into:
  - **fixtures/**: Models related to the `dw_fixtures` schema: `fixtures`, `fixture_events`, `fixture_player_stats`, `fixture_stats`.
  - **main/**: Models related to the `dw_main` schema: `coaches`, `countries`, `leagues`, `seasons`, `teams`.

## Services
The `services` directory consists of:
- **fetchers/**: Contains `ApiFetcher` and its subclasses (e.g., stats, teams fetchers) that implement different data retrieval methods.
- **db.py**: Database interaction module handling data persistence.

## Jobs
### Current Jobs
The `jobs` directory includes automated scripts categorized as:
- **config/**: Configuration files for pipelines.
- **executors/**: Scripts responsible for initializing and executing specific pipelines.
- **exporters/**: Handles exporting data to different destinations.
- **pipelines/**: Contains pipeline classes defining data flow.
- **scheduler.py**: Orchestrates the execution of update scripts.

### Planned Jobs
Upcoming automation tasks include:
- Enhancing data pipelines for improved efficiency.
- Adding new functionalities
- Adding more scripts running daily.

### Upcoming enhancements
1. Introduce Docker to enable running the app on remote server.
2. Build API to enable users interact with data.
3. Develop a web-based dashboard for data visualization.

## Requirements
Specify the required software versions:
- Python 3.10
- Libraries listed in `requirements.txt`

## Installation
```bash
# Clone the repository
git clone https://github.com/msp92/getRichOrDieTryin.git
cd getRichOrDieTryin

# Install dependencies
pip install -r requirements.txt
```

## Usage
Examples of running the application or scripts:
```bash
python jobs/executors/daily_runner.py
```

## License
Copying, using, or distributing this project without explicit permission is strictly prohibited.

