# getRichOrDieTryin

## Project Description
The primary goal of this project is to provide **high-value insights into football competitions worldwide**.
By aggregating and analyzing data from over **150 leagues and tournaments**, we leverage cutting-edge technologies
to predict outcomes across various event types. Our data-driven approach empowers users with actionable intelligence,
enhancing decision-making in football analytics and forecasting.


## Project Structure # TODO: UPDATE
```
getRichOrDieTryin/
├── config/             # 
├── data_processing/    # Aggregations, parsing, processing and transformation utilities
├── helpers/            # Utility methods
├── models/             # Data models
│   ├── analytics/      # Analytical data models
│   ├── data_warehouse/ # Core data models
│   ├── base.py         # 
├── pipelines/          # 
├── scheduler/          # 
├── scripts/            # 
├── services/           # Service integration modules
│   ├── api/            # 
│   ├── exporters/      # 
│   ├── db.py           # Database interaction
├── tests/              # 
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

## Pipelines
Coś tam based on Base + config. Plus o S3 jeśli się przyda.
- **Main**:
- **Fixtures**:
- **Analytics Breaks**:

## Scheduler
Scheduler ogarnia aktualne pipeliny i je wlacza o okreslonych porach. Tutaj schedule:


## Services
  The `services` directory consists of:
- **api/**: Contains `ApiFetcher` and its subclasses (e.g., stats, teams fetchers) that implement different data retrieval methods.
- **exporters/**:
- **db.py**: Database interaction module handling data persistence.


### Upcoming enhancements
1. Build API to enable users interact with data.
2. Develop a web-based dashboard for data visualization.

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


## License
Copying, using, or distributing this project without explicit permission is strictly prohibited.
# TODO: more constraints
