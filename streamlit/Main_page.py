import datetime
import streamlit as st
from pathlib import Path
import sys

sys.path.append(str(Path(__file__).resolve().parent.parent))

st.set_page_config(
    page_title="Home page",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Initialize connection.
conn = st.connection("postgresql", type="sql")


def make_clickable(url, text):
    return f'<a target="_blank" href="{url}">{text}</a>'


def get_filtered_list_of_games(date, countries):
    if not countries:
        query = f"SELECT * FROM fixtures WHERE date = '{date}';"
    elif len(countries) == 1:
        query = f"SELECT * FROM fixtures WHERE date = '{date}' AND country_name = '{countries[0]}';"
    else:
        query = f"SELECT * FROM fixtures WHERE date = '{date}' AND country_name IN {tuple(countries)};"

    df = conn.query(query, ttl="10m")
    filtered_df = df.filter(
        items=[
            "country_name",
            "league_name",
            "round",
            "home_team_id",
            "home_team_name",
            "away_team_id",
            "away_team_name",
        ]
    )
    return filtered_df


def get_todays_details():
    todays_date = datetime.date.today()
    query = (
        f"SELECT count(*) FROM fixtures WHERE date = '{todays_date}' AND status = 'NS';"
    )
    df = conn.query(query, ttl="10m")
    st.write(f"It's {todays_date} and there are {df.iloc[0,0]} upcoming games today!")


def body():
    st.write("Hola!")
    get_todays_details()

    return None


def main():
    body()
    return None


# Run main()
if __name__ == "__main__":
    main()
