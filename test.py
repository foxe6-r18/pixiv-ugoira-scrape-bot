#!/usr/bin/python3.6
from pixivpy3 import *
import os
import sys
import json
import sqlq
import time
import traceback
from PIL import Image
import zipfile
import io
import threading
import threadwrapper
import socket
import omnitools


test = not True
save_dir = "ugoira"
print(save_dir, flush=True)
tokens = json.loads(open(os.path.join(save_dir, "..", "auth.json"), "rb").read().decode())
rotation_key = len(tokens)-1
apis = []
for token in tokens:
    _ = AppPixivAPI()
    _.auth(refresh_token=token)
    apis.append(_)
def rotate_key():
    global rotation_key
    rotation_key = (rotation_key+1) % len(tokens)
    return rotation_key
authed = False
def api_auth():
    global authed
    while True:
        for i in range(0, len(apis)):
            apis[i].auth(refresh_token=tokens[i])
        authed = True
        time.sleep(60*10)
p = threading.Thread(target=api_auth)
p.daemon = True
p.start()
location = ("127.199.71.10", 39292+10*2)
a_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
c_r = a_socket.connect_ex(location)
while not authed:
    time.sleep(1)
is_server = c_r != 0
if is_server:
    db_fp = os.path.join(save_dir, "..", "pixiv_db.db")
    sql = sqlq.SqlQueueU(server=True, db=db_fp, depth=2, db_port=location[1], auto_backup=True, timeout_backup=10*60*1000)
else:
    sql = sqlq.SqlQueueU(db_port=location[1])


def crawl_tags():
    tws = []
    for token in tokens:
        tws.append(threadwrapper.ThreadWrapper(threading.Semaphore(1)))
    # arts_id = [row["id"] for row in sql.sql('''SELECT `id` FROM `arts`;''')]
    # artists_id = [row["id"] for row in sql.sql('''SELECT `id` FROM `artists`;''')]
    # tags = sql.sql('''SELECT `id`, `tag` FROM `tags`;''')
    # tags = {row["id"]: row["tag"] for row in tags}
    # try:
    #     last_tag_id = max(list(tags.keys()))
    # except:
    #     last_tag_id = 0
    kws = [
        "うごイラ",
        "Ugoira",
        "動圖",
        "우고이라",
    ]
    for kw in kws:
        def job(kw, _rotation_key, _qs=None):
            def _job():
                qs = _qs
                rotation_key = _rotation_key
                while True:
                    try:
                        if not qs:
                            qs = dict(word=kw, offset=0, search_target="exact_match_for_tags")
                        result = apis[rotation_key].search_illust(**qs)
                        if "illusts" not in result:
                            raise Exception(result)
                        print("\rcrawl_tags: {}".format(qs), end="", flush=True)
                        data_arts = []
                        data_artists = []
                        # data_tags = []
                        # data_art_tag = []
                        for illust in result["illusts"]:
                            # if illust["user"]["id"] in artists_id:
                            #     continue
                            if sql.sql('''SELECT 0 FROM `artists` WHERE `id` = ?;''', (illust["user"]["id"],)):
                                continue
                            # for tag in illust["tags"]:
                            #     tag_id = -1
                            #     for k, v in tags.items():
                            #         if tag["name"] == v:
                            #             tag_id = k
                            #     if tag_id == -1:
                            #         last_tag_id += 1
                            #         tag_id = last_tag_id
                            #         tags[last_tag_id] = tag["name"]
                            #         data_tags.append((last_tag_id, tag["name"], tag["translated_name"]))
                            #     data_art_tag.append((illust["id"], tag_id))
                            # if illust["user"]["id"] not in artists_id:
                            data_artists.append((illust["user"]["id"],None))
                            # artists_id.append(illust["user"]["id"])
                            data_arts.append((
                                illust["id"],
                                # illust["title"],
                                # illust["type"],
                                # illust["image_urls"]["large"],
                                # illust["caption"],
                                # illust["restrict"],
                                illust["user"]["id"],
                                # illust["create_date"],
                                # illust["page_count"],
                                # illust["width"],
                                # illust["height"],
                                illust["sanity_level"],
                                illust["x_restrict"],
                                # json.dumps(illust["series"]),
                                # json.dumps(illust["meta_pages"])
                            ))
                        if data_arts:
                            sql.sql(
                                '''INSERT INTO `arts_crawl` VALUES (?,?,?,?,NULL)''',
                                tuple(data_arts)
                            )
                        if data_artists:
                            sql.sql(
                                '''INSERT INTO `artists` VALUES (?, ?)''',
                                tuple(data_artists)
                            )
                        # if data_tags:
                        #     sql.sql(
                        #         '''INSERT INTO `tags` VALUES (?, ?, ?);''',
                        #         tuple(data_tags)
                        #     )
                        # if data_art_tag:
                        #     sql.sql(
                        #         '''INSERT INTO `art_tag` VALUES (?,?)''',
                        #         tuple(data_art_tag)
                        #     )
                        if data_arts or data_artists:# or data_tags or data_art_tag:
                            sql.commit()
                        if test:
                            break
                        if "next_url" not in result:
                            break
                        qs = apis[rotation_key].parse_qs(result["next_url"])
                        if not qs:
                            break
                        elif "offset" in qs:
                            if int(qs["offset"]) >= 5000:
                                print("Offset must be no more than 5000", flush=True)
                                break
                        time.sleep(1/2)
                        result = None
                        data_arts = None
                        data_artists = None
                    except:
                        traceback.print_exc()
                        time.sleep(60)
                if test:
                    return
            return _job
        rk = rotate_key()
        tws[rk].add(job(kw, rk))
        if test:
            break
    for tw in tws:
        tw.wait()


def get_artists_by_x_restrict():
    _sql = '''
    SELECT `artists`.`id`
    FROM `arts_crawl`
    JOIN `artists`
    ON `arts_crawl`.`user_id` = `artists`.`id`
    WHERE `arts_crawl`.`downloaded` IS NULL
    AND `artists`.`followed` IS NULL
    AND `arts_crawl`.`x_restrict` = 1
    '''
    artists_not_followed = [row["id"] for row in sql.sql(_sql)]
    sql.sql(
        '''
        UPDATE `artists`
        SET `followed` = 1
        WHERE `id` IN ({});
        '''.format(_sql)
    )
    if not test:
        sql.sql(
            '''
            DELETE FROM `arts_crawl`;
            '''
        )
    return artists_not_followed


def get_illusts_by_artists():
    tws = []
    for token in tokens:
        tws.append(threadwrapper.ThreadWrapper(threading.Semaphore(1)))
    arts_id = [row["id"] for row in sql.sql('''SELECT `id` FROM `arts`;''')]
    artists = sql.sql(
        '''
        SELECT `id`
        FROM `artists`
        WHERE `followed` IS NOT NULL;
        --AND `id` NOT IN (
        --    SELECT DISTINCT `user_id`
        --    FROM `arts`
        --);
        '''
    )
    for i, row in enumerate(artists):
        def job(i, row, _rotation_key, _qs=None):
            def _job():
                artist = row["id"]
                qs = _qs
                rotation_key = _rotation_key
                while True:
                    try:
                        if not qs:
                            qs = dict(user_id=int(artist), offset=0)
                        print("\rget_illusts_by_artists: {}/{} [{}]".format(i, len(artists), int(qs["offset"])//30+1), end="", flush=True)
                        result = apis[rotation_key].user_illusts(**qs)
                        data_arts = []
                        if "illusts" not in result:
                            raise Exception(result)
                        for illust in result["illusts"]:
                            if illust["id"] in arts_id:
                                continue
                            arts_id.append(illust["id"])
                            data_arts.append((
                                illust["id"],
                                illust["title"],
                                illust["type"],
                                illust["image_urls"]["large"],
                                illust["caption"],
                                illust["restrict"],
                                illust["user"]["id"],
                                illust["create_date"],
                                illust["page_count"],
                                illust["width"],
                                illust["height"],
                                illust["sanity_level"],
                                illust["x_restrict"],
                                json.dumps(illust["series"]),
                                json.dumps(illust["meta_pages"])
                            ))
                        if data_arts:
                            sql.sql(
                                '''INSERT INTO `arts` VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,NULL,NULL);''',
                                tuple(data_arts)
                            )
                            sql.commit()
                        if test:
                            break
                        if "next_url" not in result:
                            break
                        qs = apis[rotation_key].parse_qs(result["next_url"])
                        if not qs:
                            break
                        elif "offset" in qs:
                            if int(qs["offset"]) >= 5000:
                                print("Offset must be no more than 5000", flush=True)
                                break
                        result = None
                        data_arts = None
                        time.sleep(1/2)
                    except:
                        traceback.print_exc()
                        time.sleep(60)
                        # return
                if test:
                    return
            return _job
        rk = rotate_key()
        tws[rk].add(job(i, row, rk))
        if test:
            break
    for tw in tws:
        tw.wait()


def get_illusts_ugoira_metadata(deleted_ids=[]):
    tws = []
    for token in tokens:
        tws.append(threadwrapper.ThreadWrapper(threading.Semaphore(1)))
    arts = sql.sql(
        '''
        SELECT `id`, `type`
        FROM `arts`
        WHERE `downloaded` IS NULL
        AND `ugoira_metadata` IS NULL
        AND `x_restrict` = 1
        AND `type` = 'ugoira'
        LIMIT {};
        '''.format(len(tokens))
    )
    if not arts:
        return True
    for i, art in enumerate(arts):
        if art["id"] in deleted_ids:
            continue
        def job(i, art, rotation_key):
            def _job():
                try:
                    print("get_illusts_ugoira_metadata: {}/{} [{}]".format(i+1, len(arts), art["id"]), flush=True)
                    if art["type"] != "ugoira":
                        print("tbd: type is not ugoira", end="", flush=True)
                        return
                    ugoira_metadata = apis[rotation_key].ugoira_metadata(art["id"])
                    if "error" in ugoira_metadata:
                        if "deleted" in ugoira_metadata["error"]["user_message"]:
                            deleted_ids.append(art["id"])
                            return
                        raise Exception(ugoira_metadata)
                    sql.sql(
                        '''
                        UPDATE `arts`
                        SET `ugoira_metadata` = ?
                        WHERE `id` = ?;
                        ''',
                        (json.dumps(ugoira_metadata), art["id"])
                    )
                    ugoira_metadata = None
                    sql.commit()
                    if test:
                        return
                    time.sleep(1/2)
                except:
                    traceback.print_exc()
                    time.sleep(60)
                    # return
            return _job
        rk = rotate_key()
        tws[rk].add(job(i, art, rk))
        if test:
            break
    for tw in tws:
        tw.wait()


def download_ugoira():
    concurrent = 2**1
    tw = threadwrapper.ThreadWrapper(threading.Semaphore(concurrent))
    arts = sql.sql(
        '''
        SELECT `user_id`, `id`, `ugoira_metadata`
        FROM `arts`
        WHERE `downloaded` IS NULL
        AND `ugoira_metadata` IS NOT NULL
        AND `x_restrict` = 1
        AND `type` = 'ugoira'
        LIMIT {};
        '''.format(concurrent)
    )
    if not arts:
        return True
    arts_stat = sql.sql(
        '''
        SELECT COUNT(*) AS `ct`
        FROM `arts`
        WHERE `ugoira_metadata` IS NOT NULL
        AND `x_restrict` = 1
        AND `type` = 'ugoira'
        GROUP BY `downloaded`
        ORDER BY `downloaded`;
        '''
    )
    for i, art in enumerate(arts):
        def job(i, art, rotation_key):
            def _job():
                try:
                    # print("\rdownload_ugoira: {}/{} [{}]".format(i+1, len(arts), art["id"]), end="")
                    print("download_ugoira: {}/{} [{}]".format(arts_stat[1]["ct"]+i, arts_stat[0]["ct"]+arts_stat[1]["ct"], art["id"]), flush=True)
                    try:
                        ugoira_metadata = json.loads(art["ugoira_metadata"])["ugoira_metadata"]
                    except:
                        print("tbd: cannot parse json", flush=True)
                        return
                    ori_zip_url = list(ugoira_metadata["zip_urls"].items())[0][1]
                    ugoira_metadata = None
                    zip_url = ori_zip_url.replace("600x600", "1920x1080")
                    _save_dir = os.path.join(save_dir, str(art["user_id"]), str(art["id"]))
                    try:
                        os.makedirs(_save_dir)
                    except:
                        pass
                    try:
                        apis[rotation_key].download(url=zip_url, path=_save_dir, name="ugoira_metadata.zip")
                    except:
                        apis[rotation_key].download(url=ori_zip_url, path=_save_dir, name="ugoira_metadata.zip")
                    sql.sql(
                        '''
                        UPDATE `arts`
                        SET `downloaded` = 1
                        WHERE `id` = ?;
                        ''',
                        (art["id"],)
                    )
                    if test:
                        return
                    # time.sleep(1)
                except:
                    traceback.print_exc()
                    # time.sleep(60)
                    return
            return _job
        rk = rotate_key()
        tw.add(job(i, art, rk))
        if test:
            break
    tw.wait()


def convert_ugoira_to_gif(signals, error_zips=[], existed_art_id=[], cache=[]):
    base_dir = os.listdir(save_dir)
    if not base_dir:
        return
    not_existed = {}
    if all(signals):
        concurrent = 2**0
    else:
        concurrent = 2**2
    cache_max = concurrent+2**7
    if not cache:
        for i, user_id in enumerate(base_dir):
            fp = os.path.join(save_dir, user_id)
            if not os.path.isdir(fp):
                continue
            art_ids = os.listdir(fp)
            if len(cache) >= cache_max:
                break
            for j, art_id in enumerate(art_ids):
                if art_id == "gifs":
                    continue
                # if test:
                if int(art_id) in existed_art_id:
                    continue
                elif int(art_id) in error_zips:
                    continue
                elif len(cache) >= cache_max:
                    break
                fp2 = os.path.join(fp, "gifs", "{}.gif".format(art_id))
                if os.path.exists(fp2):
                    existed_art_id.append(int(art_id))
                else:
                    if int(user_id) not in not_existed:
                        not_existed[int(user_id)] = 0
                    not_existed[int(user_id)] += 1
                    if len(cache) <= cache_max:
                        cache.append(int(art_id))
            if i%100 == 0 or len(base_dir)-1 == i:
                print("\rprogress: {:05d}/{:05d} [{:05d}]    ".format(i, len(base_dir), len(art_ids)), end="", flush=True)
    arts = [cache.pop(0) for i in range(0, min(concurrent, len(cache)))]
    arts = sql.sql(
        '''
        SELECT `user_id`, `id`, `ugoira_metadata`
        FROM `arts`
        WHERE `downloaded` IS NOT NULL
        AND `ugoira_metadata` IS NOT NULL
        AND `x_restrict` = 1
        AND `type` = 'ugoira'
        AND `id` IN ({});
        '''.format(",".join(["?"]*len(arts))),
        tuple(arts)
    )
    if not arts:
        print("\nerror_zips", error_zips, flush=True)
        time.sleep(10)
        return
    if test:
        print(not_existed, flush=True)
        exit()
    tw = threadwrapper.ThreadWrapper(threading.Semaphore(concurrent))
    def create_palette(frames):
        w, h = frames[0].size[0] // 8, frames[0].size[1] // 8
        master = Image.new("RGB", (w, h * len(frames)))
        for index, image in enumerate(frames):
            image2 = Image.new("RGB", image.size)
            image2.paste(image)
            image2.resize((w, h), Image.NEAREST)
            master.paste(image2, (0, h * index))
            image2.close()
        master = master.convert("P", dither=False, palette=Image.ADAPTIVE, colors=256)
        for index, image in enumerate(frames):
            image = image.convert("P", dither=False, palette=master.palette)
            frames[index] = image
        return frames, master
    for i, art in enumerate(arts):
        def job(i, art):
            def _job():
                try:
                    # print("\rconvert_ugoira_to_gif: {}/{} [{}]".format(i+1, len(arts), art["id"], end="")
                    print("convert_ugoira_to_gif: {}/{} [{}] [{}]".format(len(existed_art_id)+len(error_zips)+i+cache_max-len(cache), len(existed_art_id)+len(arts)+cache_max, len(error_zips), art["id"]), flush=True)
                    try:
                        ugoira_metadata = json.loads(art["ugoira_metadata"])["ugoira_metadata"]
                    except:
                        print("tbd: cannot parse json", flush=True)
                        return 
                    frames = [frame["delay"] for frame in ugoira_metadata["frames"]]
                    duration = sum(frames) / len(frames)
                    ugoira_metadata = None
                    frames = None
                    _save_dir = os.path.join(save_dir, str(art["user_id"]))
                    zip_fp = os.path.join(_save_dir, str(art["id"]), "ugoira_metadata.zip")
                    try:
                        if not os.path.exists(zip_fp):
                            sql.sql('''UPDATE `arts` SET `downloaded` = NULL WHERE `id` = ?;''', (art["id"],))
                            return
                        zip_fo = zipfile.ZipFile(zip_fp, "r")
                    except zipfile.BadZipFile:
                        if open(zip_fp, "rb").read().find(b"\x50\x4b\x05\x06") == -1:
                            os.remove(zip_fp)
                            sql.sql('''UPDATE `arts` SET `downloaded` = NULL WHERE `id` = ?;''', (art["id"],))
                            return
                    except:
                        error_zips.append(art["id"])
                        return
                    tmp_imgs = []
                    for fn in zip_fo.namelist():
                        tmp_imgs.append(Image.open(io.BytesIO(zip_fo.read(fn))))
                    zip_fo.close()
                    zip_fo = None
                    gifs_dir = os.path.join(_save_dir, "gifs")
                    try:
                        os.makedirs(gifs_dir)
                    except:
                        pass
                    gif_fp = os.path.join(gifs_dir, "{}.gif".format(art["id"]))
                    tmp_imgs, master = create_palette(tmp_imgs)
                    try:
                        tmp_imgs[0].save(
                            gif_fp,
                            save_all=True,
                            append_images=tmp_imgs[1:],
                            optimize=False,
                            duration=duration,
                            loop=0,
                            include_color_table=True,
                            palette=master.palette.getdata()
                        )
                    except:
                        os.remove(gif_fp)
                        raise Exception("cannot create gif:", gif_fp)
                    for _ in tmp_imgs:
                        _.close()
                    master.close()
                    tmp_imgs = None
                    master = None
                    if test:
                        return
                    time.sleep(1)
                except:
                    traceback.print_exc()
                    return
                    # time.sleep(60)
            return _job
        tw.add(job(i, art))
    tw.wait()
# test=True
# convert_ugoira_to_gif()


def download():
    terminate = False
    terminated = []
    exploded = []
    signals = [False, False]
    def get_metadata_job():
        print("get_metadata_job started", flush=True)
        while not terminate:
            try:
                ...
                # continue
                signals[0] = get_illusts_ugoira_metadata()
                if signals[0]:
                    time.sleep(10)
                    signals[0] = False
                # start = time.time()
                # print(time.time()-start)
                # break
            except:
                exploded.append("get_metadata_job")
                traceback.print_exc()
                break
            finally:
                time.sleep(1)
        terminated.append("get_metadata_job")
        return "get_metadata_job"
    def download_job():
        print("download_job started", flush=True)
        while not terminate:
            try:
                ...
                # continue
                signals[1] = download_ugoira()
                if signals[1]:
                    time.sleep(10)
                    signals[1] = False
                # start = time.time()
                # print(time.time()-start)
                # break
            except:
                exploded.append("download_job")
                traceback.print_exc()
                break
            finally:
                time.sleep(1)
        terminated.append("download_job")
        return "download_job"
    def convert_job():
        print("convert_job started", flush=True)
        while not terminate:
            try:
                ...
                # continue
                convert_ugoira_to_gif(signals)
                # start = time.time()
                # print(time.time()-start)
                # break
            except:
                exploded.append("convert_job")
                traceback.print_exc()
                break
            finally:
                time.sleep(1)
        terminated.append("convert_job")
        return "convert_job"
    p1 = threading.Thread(target=get_metadata_job)
    p1.daemon = True
    p1.start()
    p2 = threading.Thread(target=download_job)
    p2.daemon = True
    p2.start()
    p3 = threading.Thread(target=convert_job)
    p3.daemon = True
    p3.start()
    input("stop?")
    input("stop?")
    input("stop?")
    terminate = True
    while len(terminated) != 3:
        print("\r", terminated, end="", flush=True)
        time.sleep(1)
    _cache()
    sql.commit()
    sql.stop()


def crawl():
    try:
        while True:
            crawl_tags()
            get_artists_by_x_restrict()
            get_illusts_by_artists()
            for i in range(0, 60*60):
                print("\rnext crawl: {}m {}s".format(i//60, i%60), end="", flush=True)
                time.sleep(1)
    except KeyboardInterrupt:
        pass
    sql.commit()
    sql.stop()


def _cache():
    cache_fp = os.path.join(save_dir, "..", "cache.json")
    structure = {}
    followed_artist_id = [row["id"] for row in sql.sql('''SELECT `id` FROM `artists` WHERE `followed` IS NOT NULL;''')]
    _save_dir = os.listdir(save_dir)
    for i, user_id in enumerate(_save_dir):
        fp = os.path.join(save_dir, user_id)
        if not os.path.isdir(fp):
            continue
        structure[user_id] = {}
        for art_id in os.listdir(fp):
            if art_id == "gifs":
                continue
            fp2 = os.path.join(fp, art_id)
            structure[user_id][art_id] = omnitools.file_size(fp2)
        print("\r", i+1, len(_save_dir), end="", flush=True)
    open(cache_fp, "wb").write(json.dumps([followed_artist_id, structure]).encode())


if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.argv.append(input("input work mode: "))
    if sys.argv[1] == "crawl":
        crawl()
    elif sys.argv[1] == "download":
        download()
    elif sys.argv[1] == "cache":
        _cache()
    else:
        raise Exception("invalid mode: {}".format(sys.argv[1]))

