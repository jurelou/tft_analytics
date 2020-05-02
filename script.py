import httpx
import time
import threading
from multiprocessing import Process, RawValue, Lock
import sys
from pymongo import MongoClient

import functools
print = functools.partial(print, flush=True)

# Internals
_mongo_client = MongoClient('localhost', 27017)
mongo_db = _mongo_client.tft_analytics

# Riot auth token

RIOT_TOKEN = { "X-Riot-Token": "xx"}
# Rate limit sleeping value
SLEEP_RATE_LIMIT = 0.89 # 100 requests every 2 minutes
# TFT divisions
DIVISIONS = [
    ("DIAMOND", "I"),
    ("DIAMOND", "II"),
    ("DIAMOND", "III"),
    ("DIAMOND", "IV"),
    ("PLATINUM", "I"),
    ("PLATINUM", "II"),
    ("PLATINUM", "III"),
]
# Riot servers regions
REGIONS = [
    "euw1",
    "eun1",
    "jp1",
    "kr",
    "na1",
    "oc1",
    "ru",
    "tr1"
]
# Routing servers
ROUTING_VALUES = {
    "europe": ("euw1", "eun1", "ru", "tr1",), 
    "asia": ("jp1", "kr",),
    "americas": ("na1", "oc1",),
}

# Internal debug thread safe counter, usefull for statistics
class Counter(object):
    def __init__(self, value=0):
        self.val = RawValue('i', value)
        self.lock = Lock()

    def increment(self):
        with self.lock:
            self.val.value += 1

    def value(self):
        with self.lock:
            return self.val.value
#
# SUMMONERS API
#
BASEURL = "https://{}.api.riotgames.com"
def get_challenger_players(region):
    base_url = BASEURL.format(region)

    r = httpx.get(base_url + "/tft/league/v1/challenger", headers=RIOT_TOKEN)
    res = r.json()
    if r.status_code != 200:
        print("ERROR", res)
    print(f"Found {len(res['entries'])} challengers from {region}")
    return res

def get_master_players(region):
    base_url = BASEURL.format(region)
    r = httpx.get(base_url + "/tft/league/v1/master", headers=RIOT_TOKEN)
    res = r.json()
    if r.status_code != 200:
        print("ERROR", res)
    print(f"Found {len(res['entries'])} masters from {region}")
    return res

def get_grandmaster_players(region):
    base_url = BASEURL.format(region)
    r = httpx.get(base_url + "/tft/league/v1/grandmaster", headers=RIOT_TOKEN)
    res = r.json()
    if r.status_code != 200:
        print("ERROR", res)
    print(f"Found {len(res['entries'])} grandmasters from {region}")
    return res

def get_league_players(region, tier, division, nb_pages=10):
    base_url = BASEURL.format(region)
    for i in range(1, nb_pages):
        r = httpx.get(base_url + f"/tft/league/v1/entries/{tier}/{division}?page={i}", headers=RIOT_TOKEN)
        content = r.json()
        if not content:
            break
        print(f"Found {len(content)} {tier} {division} from {region}")
        yield content

def store_puuids(region, list_of_summoners):
    base_url = BASEURL.format(region)
    for summoner in list_of_summoners:
        time.sleep(SLEEP_RATE_LIMIT)
        r = httpx.get(base_url + f"/lol/summoner/v4/summoners/{summoner['summonerId']}", headers=RIOT_TOKEN)
        if r.status_code == 200:
            mongo_db.puuid.insert({**r.json(), "region": region})
        else:
            print(f"ERROR summoner: {summoner}", r)

def get_games_from_puuid(region, puuid, count=20):
    base_url = BASEURL.format(region)
    r = httpx.get(base_url + f"/tft/match/v1/matches/by-puuid/{puuid}/ids?count={count}", headers=RIOT_TOKEN)
    res = r.json()
    if r.status_code != 200:
        print(f"ERROR region {region} {res}")
        return []
    print(f"Found {len(res)} games from puuid {puuid}")
    return res
#
# STORE SUMMONERS
#
def store_all_summoners(region):
    for fn in (get_challenger_players, get_master_players, get_grandmaster_players):
        time.sleep(SLEEP_RATE_LIMIT * 2)
        summoners = fn(region)
        if summoners:
            for summ in summoners["entries"]:
                summ.update({"region": region})
            mongo_db.summoners.insert_many(summoners["entries"])
            store_puuids(region, summoners["entries"])

    for tier, division in DIVISIONS:
        time.sleep(SLEEP_RATE_LIMIT * 2)
        summoners = get_league_players(region, tier, division)
        for summoner in summoners:
            if len(summoner) > 1:
                for summ in summoner:
                    summ.update({"region": region})
                mongo_db.summoners.insert_many(summoner)
                store_puuids(region, summoner)

def store_all_summoners_worldwide():
    threads = []
    for region in REGIONS:
        t = threading.Thread(target=store_all_summoners, args=(region,))
        threads.append(t)
        t.start()

#
# Fetch games from puuid
#
def store_all_games(region, counter = None):
    global_region = ROUTING_VALUES[region]
    total_puuid = mongo_db.puuid.find()
    total_puuid_count = total_puuid.count()

    for summoner in mongo_db.puuid.find({"region": { "$in": global_region}}):
        if counter:
            counter.increment()
        if "games_id" in summoner and len(summoner["games_id"]) > 1:
            continue
        print(f"{counter.value()} / {total_puuid_count} ({(counter.value() * 100) / total_puuid_count}%)")
        games_id = {"$set": {"games_id": get_games_from_puuid(region, summoner["puuid"])}}
        mongo_db.puuid.update_one({"puuid": summoner["puuid"]}, games_id)
        time.sleep(SLEEP_RATE_LIMIT)


def store_all_games_worldwide():
    threads = []
    ts_counter = Counter(0)
    for region in ROUTING_VALUES.keys():
        t = threading.Thread(target=store_all_games, args=(region,ts_counter))
        threads.append(t)
        t.start()

#store_all_summoners_worldwide()
store_all_games_worldwide()

