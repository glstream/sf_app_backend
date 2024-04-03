from itsdangerous import URLSafeTimedSerializer
from fastapi import FastAPI, Request, Response
from fastapi import FastAPI, Depends
from psycopg2 import extras
from dotenv import load_dotenv
from fastapi.middleware.cors import CORSMiddleware
from db import get_db
from superflex_models import UserDataModel, LeagueDataModel, RosterDataModel
from pathlib import Path
from datetime import datetime
# UTILS
from utils import (get_user_id, insert_current_leagues,
                       insert_league, player_manager_rosters)

# Load environment variables from .env file
load_dotenv()
# Define a list of allowed origins (use ["*"] for allowing all origins)
origins = [
    "*",
]

app = FastAPI()
# Add CORSMiddleware to the application instance
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,  # Allows all origins
    allow_credentials=True,
    allow_methods=["*"],  # Allows all methods
    allow_headers=["*"],  # Allows all headers
)

# POST ROUTES


@app.post("/user_details")
def user_details(user_data: UserDataModel, db=Depends(get_db)):
    return insert_current_leagues(db, user_data)


@app.post("/roster")
def roster(roster_data: RosterDataModel, db=Depends(get_db)):
    print('attempt rosters')
    return player_manager_rosters(db, roster_data)

# GET ROUTES


@app.get("/leagues")
def leagues(league_year: str, user_name: str, guid: str, db: str = Depends(get_db)):
    cursor_ = db.cursor(cursor_factory=extras.RealDictCursor)
    user_id = get_user_id(user_name)
    # session_id = 'FAST_API'  # testing session_id
    # session_id = 'cc73437c-40b6-4cd4-9835-6b41033968c4'
    session_id = guid

    cursor_.execute(
        f"""select ROW_NUMBER() OVER() as key, session_id, cl.user_name,cl.user_id, cl.league_id, league_name, avatar, total_rosters, qb_cnt, CASE WHEN sf_cnt > 0 THEN 'Superflex' else 'Single QB' end as roster_type, starter_cnt, total_roster_cnt, sport, insert_date, rf_cnt, case when league_cat = 0 THEN 'Redraft' when league_cat = 1 THEN 'Keeper' else 'Dynasty' end as league_type, league_year, rs.ktc_power_rank, rs.sf_power_rank, rs.fc_power_rank, rs.dp_power_rank, rs.espn_contender_rank, rs.nfl_contender_rank, rs.fp_contender_rank, rs.fc_contender_rank, rs.cbs_contender_rank
    from dynastr.current_leagues cl
    left join dynastr.ranks_summary rs on cl.league_id = rs.league_id and cl.user_id = rs.user_id
    where 1=1
    and session_id = '{session_id}'
    and cl.user_id ='{user_id}'
    and league_year = '{league_year}'"""
    )
    db_resp_obj = cursor_.fetchall()

    return db_resp_obj


@app.get("/get_user")
def get_user(user_name: str):
    return {"user_id": get_user_id(user_name)}


@app.get('/ranks')
def ranks(platform: str, db: str = Depends(get_db)):
    cursor_ = db.cursor(cursor_factory=extras.RealDictCursor)

    with open(Path.cwd() / "sql" / "player_values" / "ranks" / f"{platform}.sql", "r",) as player_values_file:
        player_values_sql = player_values_file.read()

    cursor_.execute(player_values_sql)
    db_resp_obj = cursor_.fetchall()
    cursor_.close()

    return db_resp_obj


@app.get('/trade_calculator')
def trade_calculator(platform: str, rank_type: str, db: str = Depends(get_db)):
    cursor_ = db.cursor(cursor_factory=extras.RealDictCursor)

    with open(Path.cwd() / "sql" / "player_values" / "calc" / f"{rank_type}" / f"{platform}.sql", "r",) as player_values_file:
        player_values_sql = player_values_file.read()

    cursor_.execute(player_values_sql)
    db_resp_obj = cursor_.fetchall()
    cursor_.close()

    return db_resp_obj


@app.get("/league")
def league(league_id: str, platform: str, rank_type: str, guid: str, roster_type: str, db: str = Depends(get_db)):
    print(league_id, platform, rank_type)

    cursor_ = db.cursor(cursor_factory=extras.RealDictCursor)
    session_id = guid
    league_type = 'sf_value' if roster_type == 'Superflex' else 'one_qb_value'
    rank_type = 'dynasty' if rank_type.lower() == 'dynasty' else 'redraft'

    if platform in ['espn', 'cbs', 'nfl']:
        rank_source = 'contender'
    else:
        rank_source = 'power'

    if platform == 'sf':
        league_pos_col = (
            "superflex_sf_pos_rank"
            if roster_type == "sf_value"
            else "superflex_one_qb_pos_rank"
        )
        league_type = (
            "superflex_sf_value"
            if roster_type == "sf_value"
            else "superflex_one_qb_value"
        )

    elif platform == 'fc':
        league_pos_col = (
            "sf_position_rank" if league_type == "sf_value" else "one_qb_position_rank"
        )
    else:
        league_pos_col = ''

    with open(
        Path.cwd() / "sql" / "summary" /
        f"{rank_source}" / f"{platform}.sql",
        "r",
    ) as power_summary_file:
        power_summary_sql = (
            power_summary_file.read()
            .replace("'session_id'", f"'{session_id}'")
            .replace("'league_id'", f"'{league_id}'")
            .replace("league_type", f"{league_type}")
            .replace("league_pos_col", f"{league_pos_col}")
            .replace("'rank_type'", f"'{rank_type}'")
        )
    cursor_.execute(power_summary_sql)
    db_resp_obj = cursor_.fetchall()
    cursor_.close()

    return db_resp_obj


@app.get("/league_detail")
def league_detail(league_id: str, platform: str, rank_type: str, guid: str, roster_type: str, db: str = Depends(get_db)):

    cursor_ = db.cursor(cursor_factory=extras.RealDictCursor)
    session_id = guid
    league_type = 'sf_value' if roster_type == 'Superflex' else 'one_qb_value'
    rank_type = 'dynasty' if rank_type.lower() == 'dynasty' else 'redraft'

    if platform == 'sf':

        league_pos_col = (
            "superflex_sf_pos_rank" if league_type == "sf_value" else "superflex_one_qb_pos_rank"
        )

        league_type = (
            "superflex_sf_value"
            if roster_type == "sf_value"
            else "superflex_one_qb_value"
        )
    elif platform == 'fc':
        league_pos_col = (
            "sf_position_rank" if league_type == "sf_value" else "one_qb_position_rank"
        )
    else:
        league_pos_col = ''

    with open(
        Path.cwd() / "sql" / "details" / "power" / f"{platform}.sql",
        "r",
    ) as power_detail_file:
        power_detail_sql = (
            power_detail_file.read()
            .replace("'session_id'", f"'{session_id}'")
            .replace("'league_id'", f"'{league_id}'")
            .replace("league_type", f"{league_type}")
            .replace("league_pos_col", f"{league_pos_col}")
            .replace("'rank_type'", f"'{rank_type}'")
        )
    cursor_.execute(power_detail_sql)
    db_resp_obj = cursor_.fetchall()
    cursor_.close()

    return db_resp_obj


@app.get("/trades_detail")
def trades_detail(league_id: str, platform: str, roster_type: str, league_year: str, rank_type: str, db: str = Depends(get_db)):

    cursor_ = db.cursor(cursor_factory=extras.RealDictCursor)
    league_type = 'sf_value' if roster_type == 'Superflex' else 'one_qb_value'
    rank_type = 'dynasty' if rank_type.lower() == 'dynasty' else 'redraft'

    if platform == 'sf':

        league_type = (
            "superflex_sf_value"
            if roster_type == "sf_value"
            else "superflex_one_qb_value"
        )

    with open(Path.cwd() / "sql" / "details" / "trades" / f"{platform}.sql", "r",) as trades_file:
        trades_sql = (
            trades_file.read()
            .replace("'current_year'", f"'{league_year}'")
            .replace("'league_id'", f"'{league_id}'")
            .replace("league_type", f"{league_type}")
            .replace("'rank_type'", f"'{rank_type}'")
        )
    cursor_.execute(trades_sql)
    trades = cursor_.fetchall()

    transaction_ids = list(
        set([(i["transaction_id"], i["status_updated"]) for i in trades])
    )
    transaction_ids = sorted(
        transaction_ids,
        key=lambda x: datetime.fromtimestamp(int(str(x[-1])[:10])),
        reverse=True,
    )
    managers_list = list(
        set([(i["display_name"], i["transaction_id"]) for i in trades])
    )
    trades_dict = {}
    for transaction_id in transaction_ids:
        trades_dict[transaction_id[0]] = {
            i[0]: [p for p in trades if p["display_name"]
                   == i[0] and p["transaction_id"] == transaction_id[0]]
            for i in managers_list
            if i[1] == transaction_id[0]
        }

    cursor_.close()

    return trades_dict


@app.get("/trades_summary")
def trades_summary(league_id: str, platform: str, roster_type: str, league_year: str, rank_type: str, db: str = Depends(get_db)):

    cursor_ = db.cursor(cursor_factory=extras.RealDictCursor)
    league_type = 'sf_value' if roster_type == 'Superflex' else 'one_qb_value'
    rank_type = 'dynasty' if rank_type.lower() == 'dynasty' else 'redraft'

    if platform == 'sf':

        league_type = (
            "superflex_sf_value"
            if roster_type == "sf_value"
            else "superflex_one_qb_value"
        )

    with open(Path.cwd() / "sql" / "summary" / "trades" / f"{platform}.sql", "r",) as trades_file:
        trades_sql = (
            trades_file.read()
            .replace("'current_year'", f"'{league_year}'")
            .replace("'league_id'", f"'{league_id}'")
            .replace("league_type", f"{league_type}")
            .replace("'rank_type'", f"'{rank_type}'")
        )
    cursor_.execute(trades_sql)
    db_resp_obj = cursor_.fetchall()
    cursor_.close()

    return db_resp_obj


@app.get("/contender_league_summary")
def contender_league_summary(league_id: str, projection_source: str, guid: str, db: str = Depends(get_db)):
    print(league_id, projection_source)

    cursor_ = db.cursor(cursor_factory=extras.RealDictCursor)
    session_id = guid

    with open(
        Path.cwd() / "sql" / "summary" / "contender" /
        f"{projection_source}.sql",
        "r",
    ) as projections_file:
        projections_sql = (
            projections_file.read()
            .replace("'session_id'", f"'{session_id}'")
            .replace("'league_id'", f"'{league_id}'")
        )
    cursor_.execute(projections_sql)
    db_resp_obj = cursor_.fetchall()
    cursor_.close()

    return db_resp_obj


@app.get("/contender_league_detail")
def contender_league_detail(league_id: str, projection_source: str, guid: str, db: str = Depends(get_db)):
    print(league_id, projection_source)

    cursor_ = db.cursor(cursor_factory=extras.RealDictCursor)
    session_id = guid

    with open(
        Path.cwd() / "sql" / "details" / "contender" /
        f"{projection_source}.sql",
        "r",
    ) as projections_file:
        projections_sql = (
            projections_file.read()
            .replace("'session_id'", f"'{session_id}'")
            .replace("'league_id'", f"'{league_id}'")
        )
    cursor_.execute(projections_sql)
    db_resp_obj = cursor_.fetchall()
    cursor_.close()

    return db_resp_obj


@app.get("/best_avialable")
def best_avialable(league_id: str, platform: str, rank_type: str, guid: str, roster_type: str, db: str = Depends(get_db)):

    cursor_ = db.cursor(cursor_factory=extras.RealDictCursor)
    session_id = guid
    rank_type = 'dynasty' if rank_type.lower() == 'dynasty' else 'redraft'

    if platform == 'sf':
        league_type = "superflex_sf_value " if roster_type == "sf_value" else "superflex_one_qb_value"
    else:
        league_type = 'sf_value' if roster_type == 'Superflex' else 'one_qb_value'

    with open(Path.cwd() / "sql" / "best_available" / "power" / f"{platform}.sql",
              "r",
              ) as ba_sql_file:
        ba_sql = (
            ba_sql_file.read()
            .replace("'session_id'", f"'{session_id}'")
            .replace("'league_id'", f"'{league_id}'")
            .replace("league_type", f"{league_type}")
            .replace("'rank_type'", f"'{rank_type}'")
        )
    cursor_.execute(ba_sql)
    db_resp_obj = cursor_.fetchall()
    cursor_.close()

    return db_resp_obj
