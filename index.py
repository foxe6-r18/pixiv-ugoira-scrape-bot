#!/usr/bin/python3.5
import os
import math
import time
import json
import pickle
import datetime
import email.utils
import threading
import traceback
import threadwrapper
from pixivpy3 import *
from omnitools import crc32hd, str2html, dt2rfc822gmt, rfc822gmt2dt
from pythoncgi import _SERVER, _GET, _POST, _SESSION, _COOKIE, _HEADERS, set_status, set_header, execute, print, main, log, log_construct, should_return_304


tokens = None
followed_artist_id = None
cache = None
cache_fp = "cache.json"
rotation_key = 0
apis = []
papis = []


def rotate_key():
    global rotation_key
    rotation_key = (rotation_key+1) % len(tokens)
    return rotation_key


def popular_artists1():
    artists = []
    tws = []
    for token in tokens:
        tws.append(threadwrapper.ThreadWrapper(threading.Semaphore(1)))
    for mode in ["day_r18", "week_r18", "day_male_r18", "day_female_r18"]:
        def job(mode, _rotation_key, _qs=None):
            def _job():
                qs = _qs
                rotation_key = _rotation_key
                while True:
                    try:
                        if not qs:
                            qs = {"mode": mode}
                        result = apis[rotation_key].illust_ranking(**qs)
                        if "illusts" not in result:
                            raise
                        for illust in result["illusts"]:
                            artist_id = illust["user"]["id"]
                            if artist_id in followed_artist_id:
                                if artist_id not in artists:
                                    artists.append(artist_id)
                        if "next_url" not in result:
                            break
                        qs = apis[rotation_key].parse_qs(result["next_url"])
                        if not qs:
                            break
                        elif "offset" in qs:
                            if int(qs["offset"]) >= 5000:
                                print("Offset must be no more than 5000")
                                break
                        time.sleep(1/2)
                        result = None
                    except:
                        break
                if test:
                    return
            return _job
        rk = rotate_key()
        tws[rk].add(job(mode, rk))
    for tw in tws:
        tw.wait()
    return artists


def popular_artists2():
    artists = []
    tws = []
    for token in tokens:
        tws.append(threadwrapper.ThreadWrapper(threading.Semaphore(1)))
    for mode in ["daily_r18", "weekly_r18"]:
        def job(mode, _rotation_key, _qs=None):
            def _job():
                qs = _qs
                rotation_key = _rotation_key
                try:
                    if not qs:
                        qs = {"ranking_type": "ugoira", "mode": mode, "page": 1, "per_page": 1000}
                    result = papis[rotation_key].ranking(**qs)
                    if result["status"] != "success":
                        raise
                    for work in result["response"][0]["works"]:
                        illust = work["work"]
                        artist_id = illust["user"]["id"]
                        if artist_id in followed_artist_id:
                            if artist_id not in artists:
                                artists.append(artist_id)
                    return
                except:
                    pass
            return _job
        rk = rotate_key()
        tws[rk].add(job(mode, rk))
    for tw in tws:
        tw.wait()
    return artists


def popular_artists():
    return list(set(popular_artists2() + popular_artists1()))


def get_cache_fp():
    return os.path.join("_cache", crc32hd(_SERVER["SCRIPT_NAME"])+".cache")


def should_read_from_cache_file():
    if os.path.isfile(get_cache_fp()):
        cache = pickle.loads(open(get_cache_fp(), "rb").read())
        if "Last-Modified" in cache["headers"]:
            ims = rfc822gmt2dt(cache["headers"]["Last-Modified"])
            if ims:
                lastmodified = math.floor(os.path.getmtime(cache_fp))
                lastmodified = datetime.datetime.fromtimestamp(lastmodified)
                if ims >= lastmodified:
                    set_status(cache["status_code"])
                    for k, v in cache["headers"].items():
                        set_header(k, v)
                    print(cache["cache"], end="")
                    return True
    return False


def write_to_cache_file(cache):
    fp = get_cache_fp()
    try:
        os.makedirs(os.path.dirname(fp))
    except:
        pass
    try:
        open(fp, "wb").write(pickle.dumps(cache))
    except:
        pass


@execute(
    "get",
    cacheable=True,
    cache_norm=should_return_304(cache_fp),
    cache_strat=should_read_from_cache_file,
    cache_store=write_to_cache_file,
)
def get():
    global followed_artist_id, cache, tokens, rotation_key
    followed_artist_id, cache = json.loads(open(cache_fp, "rb").read().decode())
    tokens = json.loads(open("auth.json", "rb").read().decode())
    rotation_key = len(tokens) - 1
    for token in tokens:
        _ = AppPixivAPI()
        _.auth(refresh_token=token)
        apis.append(_)
        _ = PixivAPI()
        _.auth(refresh_token=token)
        papis.append(_)
    # now = datetime.datetime.fromtimestamp(time.time()+max_age, tz=tzoffset(None, 0))
    # set_header("Expires", now.strftime('%a, %d %b %Y %H:%M:%S GMT'))
    for k in cache:
        v = [_ for _v, _ in cache[k].items()]
        cache[k] = [len(v), sum(v)/len(v)]
    _ = popular_artists()
    _max = len(_)
    popular_cache = {k: v for k, v in cache.items() if int(k) in _}
    popular_quality_artists = [[k, round(v[1]/1024/1024, 2)] for k, v in sorted(popular_cache.items(), key=lambda artist: artist[1][1], reverse=True)]
    popular_quantity_artists = [[k, v[0]] for k, v in sorted(popular_cache.items(), key=lambda artist: artist[1][0], reverse=True)]
    quality_artists = [[k, round(v[1]/1024/1024, 2)] for k, v in sorted(cache.items(), key=lambda artist: artist[1][1], reverse=True)][:_max]
    quantity_artists = [[k, v[0]] for k, v in sorted(cache.items(), key=lambda artist: artist[1][0], reverse=True)][:_max]
    _ = [
        popular_quality_artists,
        popular_quantity_artists,
        quality_artists,
        quantity_artists,
    ]
    print('''<html>
<head>
    <script src="/JS/jquery.min.js"></script>
    <script async defer src="./script.js"></script>
</head>
<body>
    <script>
        var vars = {};
    </script>
</body>
</html>'''.format(json.dumps(_)))


if __name__ == "__main__":
    main()

