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
RIOT_TOKENS = [
]

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
def get_challenger_players(apikey, region):
    base_url = BASEURL.format(region)
    print("!!!", base_url)
    r = httpx.get(base_url + "/tft/league/v1/challenger", headers=apikey, timeout=None)
    res = r.json()
    if r.status_code != 200:
        print("ERROR", res)
    print(f"Found {len(res['entries'])} challengers from {region}")
    return res

def get_master_players(apikey, region):
    base_url = BASEURL.format(region)
    r = httpx.get(base_url + "/tft/league/v1/master", headers=apikey, timeout=None)
    res = r.json()
    if r.status_code != 200:
        print("ERROR", res)
    print(f"Found {len(res['entries'])} masters from {region}")
    return res

def get_grandmaster_players(apikey, region):
    base_url = BASEURL.format(region)
    r = httpx.get(base_url + "/tft/league/v1/grandmaster", headers=apikey, timeout=None)
    res = r.json()
    if r.status_code != 200:
        print("ERROR", res)
    print(f"Found {len(res['entries'])} grandmasters from {region}")
    return res

def get_league_players(apikey, region, tier, division, nb_pages=10):
    base_url = BASEURL.format(region)
    for i in range(1, nb_pages):
        r = httpx.get(base_url + f"/tft/league/v1/entries/{tier}/{division}?page={i}", headers=apikey, timeout=None)
        content = r.json()
        if not content:
            break
        print(f"Found {len(content)} {tier} {division} from {region}")
        yield content

def store_puuids(apikey, region, list_of_summoners):
    collection_name = "puuids-{}".format(apikey["X-Riot-Token"][-4:])
    puuids_collection = mongo_db[collection_name]

    base_url = BASEURL.format(region)
    print("BASE", base_url)
    for summoner in list_of_summoners:
        exists = puuids_collection.find_one({"puuid": summoner})
        if exists:
            continue
        time.sleep(SLEEP_RATE_LIMIT)
        r = httpx.get(base_url + f"/lol/summoner/v4/summoners/by-name/{summoner['summonerName']}", headers=apikey, timeout=None)
        if r.status_code == 200:
            puuids_collection.insert({**r.json(), "region": region})
        else:
            print(f"ERROR summoner: {summoner}", r)


def get_games_from_puuid(apikey, region, puuid, count=15):
    base_url = BASEURL.format(region)
    r = httpx.get(base_url + f"/tft/match/v1/matches/by-puuid/{puuid}/ids?count={count}", headers=apikey, timeout=None)
    res = r.json()
    if r.status_code != 200:
        print(f"ERROR region {region} {res} with key {apikey} puuid: {puuid}")
        return []
    print(f"Found {len(res)} games from puuid {puuid} region {region}")
    return res
#
# STORE SUMMONERS
#
def store_all_summoners(region, apikey):
    summs_collection = mongo_db["summoners"]
    for fn in (get_challenger_players, get_master_players, get_grandmaster_players):
        time.sleep(SLEEP_RATE_LIMIT * 2)
        summoners = fn(apikey, region)
        if summoners:
            for summ in summoners["entries"]:
                summ.update({"region": region})
            summs_collection.insert_many(summoners["entries"])
            #store_puuids(apikey, region, summoners["entries"])

    for tier, division in DIVISIONS:
        time.sleep(SLEEP_RATE_LIMIT * 2)
        summoners = get_league_players(apikey, region, tier, division)
        for summoner in summoners:
            if len(summoner) > 1:
                for summ in summoner:
                    summ.update({"region": region})
                summs_collection.insert_many(summoner)
                #store_puuids(apikey, region, summoner)

    store_puuids(apikey, region, summs_collection.find())

def store_all_summoners_worldwide(apikey):
    threads = []
    for region in REGIONS:
        t = threading.Thread(target=store_all_summoners, args=(region, apikey))
        threads.append(t)
        t.start()


def store_match(apikey, region, match_id):
    q = { "metadata.match_id": match_id }
    match = mongo_db.match.find_one(q)
    if match:
        return
    time.sleep(SLEEP_RATE_LIMIT + 0.2)
    base_url = BASEURL.format(region)
    r = httpx.get(base_url + f"/tft/match/v1/matches/{match_id}", headers=apikey, timeout=None)
    res = r.json()
    if r.status_code != 200:
        print(f"ERROR get match region {region} {res} with key {apikey}")
        return
    mongo_db.match.insert(res)
    return

def store_all_matches(apikey, mp_id, region):
    global_region = ROUTING_VALUES[region]

    puuids = mongo_db.puuid.find({"games_id": {"$exists": True}, "_mp_id": mp_id, "region": { "$in": global_region}})
    for puuid in puuids:
        for match in puuid["games_id"]:
            print(f"{mp_id} Downloading match {match} from {region}")
            store_match(apikey, region, match)

#
# Fetch games from puuid
#
def store_all_games_id(apikey, region, counter=None):
    global_region = ROUTING_VALUES[region]
    total_puuid = mongo_db.puuid.find()
    total_puuid_count = total_puuid.count()

    # Retrieve game ids from summoners puuids
    #TODO: 
    #  puuid_collection = "puuids-{}".format(apikey["X-Riot-Token"][-4:])
    tasks = mongo_db.puuid.find({"region": { "$in": global_region}})
    for summoner in tasks:
        if counter:
            counter.increment()
        if "games_id" in summoner and len(summoner["games_id"]) > 1:
            continue
        percent = round(((counter.value() * 100) / total_puuid_count), 2)
        print(f"{counter.value()} / {percent}%)")
        games_id = {"$set": {"games_id": get_games_from_puuid(apikey, region, summoner["puuid"])}}
        mongo_db.puuid.update_one({"puuid": summoner["puuid"]}, games_id)
        time.sleep(SLEEP_RATE_LIMIT)

    # Retrieve games from games id

def store_all_games_worldwide():
    threads = []
    c = mongo_db.puuid.find({"games_id": {"$exists": True}})
    ts_counter = Counter(c.count())

    #Start some threads with apikey 1 downloading match id from puuid
    for region in ROUTING_VALUES.keys():
        t = threading.Thread(target=store_all_games_id, args=(RIOT_TOKENS[0], region,ts_counter))
        threads.append(t)
        t.start()

    # Start some threads with apikey 2 downloading match from match id
    for region in ROUTING_VALUES.keys():
        t = threading.Thread(target=store_all_matches, args=(RIOT_TOKENS[1], 2, region))
        threads.append(t)
        t.start()

    # Start some threads with apikey 3 downloading match from match id
    for region in ROUTING_VALUES.keys():
        t = threading.Thread(target=store_all_matches, args=(RIOT_TOKENS[2], 1, region))
        threads.append(t)
        t.start()

    for index, thread in enumerate(threads):
        thread.join()
        print(f"Thread {index} done.")


def set_mp_ids(collection):
    index = 0
    for item in collection.find():
        if "_mp_id" not in item:
            set_mp_id = {"$set": {"_mp_id": index}}
            collection.update_one({"_id": item["_id"]}, set_mp_id)
        index = (index + 1) % 3

# store_all_summoners_worldwide(RIOT_TOKENS[1])
# store_puuids(RIOT_TOKENS[1], "ru", mongo_db.summoners.find({"region": "ru"}))


#set_mp_ids(mongo_db.puuid)



store_all_games_worldwide()

