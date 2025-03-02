from services.db import Db

db = Db()


# class FixtureFactory(SQLAlchemyModelFactory):
#     class Meta:
#         model = Fixture  # Specify the SQLAlchemy model
#         # sqlalchemy_session = scoped_session(sessionmaker(bind=db.engine))
#
#     fixture_id = factory.Sequence(lambda n: n + 1)  # Auto-incrementing ID
#     league_id = Faker().pystr()
#     country_name = Faker().pystr()
#     season_year = Faker().pystr()
#     league_name = Faker().pystr()
#     season_stage = Faker().pystr()
#     round = Faker().pyint()
#     date = Faker().date_time()
#     status = Faker().pystr()
#     referee = Faker().pystr()
#     home_team_id = Faker().pyint()
#     home_team_name = Faker().pystr()
#     away_team_id = Faker().pyint()
#     away_team_name = Faker().pystr()
#     goals_home = Faker().pyint()
#     goals_away = Faker().pyint()
#     goals_home_ht = Faker().pyint()
#     goals_away_ht = Faker().pyint()
#     update_date = Faker().date_time()
