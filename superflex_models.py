from pydantic import BaseModel


class UserDataModel(BaseModel):
    user_name: str
    league_year: str
    guid: str


class LeagueDataModel(BaseModel):
    league_id: str


class RosterDataModel(BaseModel):
    league_id: str
    user_id: str
    guid: str
    league_year: str
