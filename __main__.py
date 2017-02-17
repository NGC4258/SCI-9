from datetime import datetime, timedelta, date                                 
from dateutil import tz
import requests
import re
import os
import logging
import time
import sys
import multiprocessing as mp
import psycopg2
from psycopg2 import extras
from psycopg2.extensions import AsIs

class ToolsForCrawler(object):

    def __init__(self, appId, appSecret, appTokenFile):
        self.appToken = self.__fetch_token(appId, appSecret, appTokenFile)

    def __fetch_token(self, appId, appSecret, appTokenFile):

        def get_from_local():
            with open(appTokenFile, "r") as tokenFile:
                return tokenFile.read()
            
        if os.path.isfile(appTokenFile):
            return get_from_local()

        def get_from_fb():
            params = {"grant_type": "client_credentials",
                      "client_id": appId,
                      "client_secret": appSecret
            }
            request = requests.request("GET",
                                       ("https://graph.facebook.com/"
                                       "oauth/access_token?"),
                                       params=params
            )
            for i in request:
                token = i.split("=")[1]
            with open(appTokenFile, "w") as tokenFile:
                tokenFile.write(token)
            return token
        
        return get_from_fb()

    def get_date_time(self, dt):
        from_zone = tz.gettz("UTC")
        to_zone = tz.gettz("Asia/Taipei")
        utc = datetime.strptime(dt, "%Y-%m-%dT%H:%M:%S+0000")
        utc = utc.replace(tzinfo=from_zone)
        newTime = datetime.strptime(str(utc.astimezone(to_zone)),
                                    "%Y-%m-%d %H:%M:%S+08:00")
        return newTime.strftime("%Y-%m-%d %H:%M:%S")

class PostCrawler(ToolsForCrawler):

    def __init__(self, appId, appSecret, appTokenFile):
        super(PostCrawler, self).__init__(appId, appSecret, appTokenFile)

    def get(self, target):
        fanResp = self.__request_to_fb(target, "id,name")
        if fanResp.get("id"):
            startTime, stopTime = self.__determine_time_range(2)
            processingDate = date.today()

            def fields_string(category):
                return ("{0}.until({1}).since({2}).limit(50){3}id,"
                        "message,story,link,name,caption,description,"
                        "type,created_time,updated_time,picture,icon,"
                        "source,status_type,object_id,to,message_tags,"
                        "shares{4}".format(category,
                                           stopTime,
                                           startTime,
                                           "{",
                                           "}")
                )

            postResp = self.__request_to_fb(target, fields_string("post"))
            feedResp = {}
            if postResp.get("posts"):
                return self.__collects_data(postResp,
                                            fanResp.get("id"),
                                            fanResp.get("name"),
                                            "posts",
                                            processingDate
                )
            else:
                feedResp = self.__request_to_fb(target, fields_string("feed"))
            if feedResp.get("feed"):
                return self.__collects_data(feedResp,
                                            fanResp.get("id"),
                                            fanResp.get("name"),
                                            "feed",
                                            processingDate
                )
        if fanResp.get("error"):
            return fanResp
        return []

    def __request_to_fb(self, target, fieldsStr):
        params = {"access_token": self.appToken,
                  "fields": fieldsStr
        }
        string = "https://graph.facebook.com/v2.6/{}".format(target)
        return requests.request("GET", string, params=params).json()

    def __summary_request_to_fb(self, target, fieldsStr):
        params = {"access_token": self.appToken}
        string = ("https://graph.facebook.com/v2.6/{0}/{1}"
                  "?fields=id&summary=true".format(target, fieldsStr)
        )
        return requests.request("GET", string, params=params).json()

    def __determine_time_range(self, hourRange):
        stopTime = datetime.utcnow()
        startTime = stopTime - timedelta(hours=hourRange)
        return startTime.strftime("%Y-%m-%d %H:%S:%M"), stopTime

    def __collects_data(self, resp, fanID, fanName, category, processingDate):
        results = []
        resultsAppend = results.append
        for datum in resp[category].get("data"):
            createdTime = self.__get_date_time(datum.get("created_time"))
            if self.__not_same_month(processingDate, createdTime):
                continue
            toID, toName = self.__get_to(datum.get("to"))
            resultsAppend({"id": datum.get("id", "Null"),
                           "from_id": fanID,
                           "from_name": fanName,
                           "message": datum.get("message", "Null"),
                           "story": datum.get("story", "Null"),
                           "link": datum.get("link", "Null"),
                           "name": datum.get("name", "Null"),
                           "caption": datum.get("caption", "Null"),
                           "description": datum.get("description", "Null"),
                           "type": datum.get("type", "Null"),
                           "created_time": createdTime,
                           "updated_time": self.__get_date_time(
                                           datum.get("updated_time")),
                           "shares": self.__get_shares(datum.get("shares")),
                           "likes": self.__get_total_count(datum.get("id"),
                                                           "likes"),
                           "comments": self.__get_total_count(datum.get("id"),
                                                          "comments"),
                           "picture": datum.get("picture", "Null"),
                           "icon": datum.get("icon", "Null"),
                           "source": datum.get("source", "Null"),
                           "status_type": datum.get("status_type", "Null"),
                           "object_id": datum.get("object_id", "Null"),
                           "application_id": datum.get("application_id", "Null"),
                           "application_name": datum.get("application_name",
                                                         "Null"),
                           "by_text": "Null",
                           "by_href": "Null",
                           "by_redirect": "Null",
                           "to_id": toID,
                           "to_name": toName,
                           "message_tags_count": self.__get_message_tags(
                                                 datum.get("message_tags")),
                           "updatetime": datetime.now().strftime("%Y-%m-%d "
                                                                 "%H:%M:%S"),
                           "searchkeyword": "Null",
                           "page_id": fanID}
        )
        return results

    def __get_date_time(self, dt):
        return super(PostCrawler, self).get_date_time(dt)

    def __not_same_month(self, processingDate, createdTime):
        if (datetime.strptime(createdTime, '%Y-%m-%d %H:%M:%S').strftime('%Y-%m')
            != processingDate.strftime('%Y-%m')):
            return True
        return False

    def __get_shares(self, shares):
        if shares:
            return shares.get("count")
        return 0

    def __get_message_tags(self, messageTags):
        if messageTags:
            return len(messageTags)
        return 0

    def __get_total_count(self, target, what):
        summary = self.__summary_request_to_fb(target, what)
        if summary.get("summary"):
            return summary["summary"].get("total_count", 0)
        return 0

    def __get_to(self, to):
        if to:
            return to["data"][0].get("id"), to["data"][0].get("name")
        return None, None

class CommentCrawler(ToolsForCrawler):

    def __init__(self, appId, appSecret, appTokenFile):
        super(CommentCrawler, self).__init__(appId, appSecret, appTokenFile)

    def get(self, target):
        fieldsStr = ("id,created_time,"
                     "comments.limit(50).order(reverse_chronological)"
                     "{id,from,message,created_time,like_count,comment_count}"
        )
        params = {"access_token": self.appToken,
                  "fields": fieldsStr
        }
        queryStr = "https://graph.facebook.com/v2.6/{}".format(target)        
        resp = requests.request("GET", queryStr, params=params).json()
        if resp.get("comments"):
            postID = resp.get("id")
            postCreatedTime = self.__get_date_time(resp.get("created_time"))
            pageID = postID.split("_")[0]
            return self.__collects_data(resp.get("comments"),
                                 pageID,
                                 postID,
                                 postCreatedTime
            )
        if resp.get("error"):
            return resp
        return []

    def __collects_data(self, comments, pageID, postID, postCreatedTime):
        results = []
        resultsAppend = results.append
        while True:
            for datum in comments.get("data"):
                resultsAppend({"id": datum.get("id", "Null"),
                               "from_id": datum["from"].get("id", "Null"),
                               "from_name": datum["from"].get("name", "Null"),
                               "message": datum.get("message", "Null"),
                               "created_time": self.__get_date_time(
                                               datum.get("created_time")),
                               "likes": datum.get("like_count", "Null"),
                               "comments": datum.get("comment_count", "Null"),
                               "page_id": pageID,
                               "post_id": postID,
                               "post_created_time": postCreatedTime,
                               "updatetime": self.__get_update_time(),
                               "app_id": "Null"}
                )
            if comments['paging'].get('next'):
                comments = requests.request("GET",
                                            comments["paging"].get("next")
                ).json()
            else:
                break
        return results

    def __get_date_time(self, dt):
        return super(CommentCrawler, self).get_date_time(dt)

    def __get_update_time(self):
        return datetime.now().strftime('%Y-%m-%d %H:%M:%S')

class PgSQLRecorder(object):

    def __init__(self, config):
        self.con = psycopg2.connect(config)
        self.cur = self.con.cursor(cursor_factory=extras.RealDictCursor)

    def close(self):
        if self.con:
            self.cur.close()
            self.con.close()

    def record(self, tableName, data, crawlerNamed):
        if crawlerNamed == "post":
            for count in data:
                self.cur.execute((
                    "INSERT INTO %(table)s (id, from_id, from_name, message, "
                    "story, link, name, caption, description, type, created_time, "
                    "updated_time, shares, likes, comments, picture, icon, source,"
                    " status_type, object_id, application_id, application_name, "
                    "by_text, by_href, by_redirect, to_id, to_name, "
                    "message_tags_count, updatetime, searchkeyword, page_id) "
                    "VALUES (%(id)s,%(from_id)s,%(from_name)s,%(message)s,"
                    "%(story)s,%(link)s,%(name)s,%(caption)s,%(description)s,"
                    "%(type)s,%(created_time)s,%(updated_time)s,%(shares)s,"
                    "%(likes)s,%(comments)s,%(picture)s,%(icon)s,%(source)s,"
                    "%(status_type)s,%(object_id)s,%(application_id)s,"
                    "%(application_name)s,%(by_text)s,%(by_href)s,%(by_redirect)s,"
                    "%(to_id)s,%(to_name)s,%(message_tags_count)s,%(updatetime)s,"
                    "%(searchkeyword)s,%(page_id)s);"),
                    {"table": AsIs(tableName),
                     "id": count.get("id"),
                     "from_id": count.get("from_id"),
                     "from_name": count.get("from_name"),
                     "message": count.get("message"),
                     "story": count.get("story"),
                     "link": count.get("link"),
                     "name": count.get("name"),
                     "caption": count.get("caption"),
                     "description": count.get("description"),
                     "type": count.get("type"),
                     "created_time": count.get("created_time"),
                     "updated_time": count.get("updated_time"),
                     "shares": count.get("shares"),
                     "likes": count.get("likes"),
                     "comments": count.get("comments"),
                     "picture": count.get("picture"),
                     "icon": count.get("icon"),
                     "source": count.get("source"),
                     "status_type": count.get("status_type"),
                     "object_id": count.get("object_id"),
                     "application_id": count.get("application_id"),
                     "application_name": count.get("application_name"),
                     "by_text": count.get("by_text"),
                     "by_href": count.get("by_href"),
                     "by_redirect": count.get("by_redirect"),
                     "to_id": count.get("to_id"),
                     "to_name": count.get("to_name"),
                     "message_tags_count": count.get("message_tags_count"),
                     "updatetime": count.get("updatetime"),
                     "searchkeyword": count.get("searchkeyword"),
                     "page_id": count.get("page_id")})
        if crawlerNamed == "comment":
            for count in data:
                self.cur.execute((
                    "INSERT INTO %(table)s (id, from_id, from_name, message, "
                    "created_time, likes, comments, page_id, post_id, "
                    "post_created_time, updatetime, app_id) VALUES (%(id)s,"
                    "%(from_id)s,%(from_name)s,%(message)s,%(created_time)s,"
                    "%(likes)s,%(comments)s,%(page_id)s,%(post_id)s,"
                    "%(post_created_time)s,%(updatetime)s,%(app_id)s);"),
                    {"table": AsIs(tableName),
                     "id": count.get("id"),
                     "from_id": count.get("from_id"),
                     "from_name": count.get("from_name"),
                     "message": count.get("message"),
                     "created_time": count.get("created_time"),
                     "likes": count.get("likes"),
                     "comments": count.get("comments"),
                     "page_id": count.get("page_id"),
                     "post_id": count.get("post_id"),
                     "post_created_time": count.get("post_created_time"),
                     "updatetime": count.get("updatetime"),
                     "app_id": count.get("app_id")})
        self.con.commit()

    def create_post_table(self, tableName):
        self.cur.execute(("CREATE TABLE IF NOT EXISTS %(table)s"
                         "(id text, from_id bigint, from_name text, message text, "
                         "story text, link text, name text, caption text, "
                         "description text, type text, created_time text, "
                         "updated_time text, shares bigint, likes bigint, "
                         "comments bigint, picture text, icon text, source text, "
                         "status_type text, object_id text, application_id text, "
                         "application_name text, by_text text, by_href text, "
                         "by_redirect text, to_id text, to_name text, "
                         "message_tags_count bigint, updatetime text, "
                         "searchkeyword text, page_id text);"),
                         {"table": AsIs(tableName)}
        )
        self.cur.execute(("CREATE OR REPLACE RULE key_update AS ON INSERT TO "
                         "%(table)s WHERE (EXISTS ( SELECT 1 FROM %(table)s "
                         "WHERE %(table)s.id::text = new.id::text)) "
                         "DO INSTEAD UPDATE %(table)s SET from_id = new.from_id, "
                         "from_name = new.from_name, message = new.message, "
                         "story = new.story, link = new.link, name = new.name, "
                         "caption = new.caption, description = new.description, "
                         "type = new.type, created_time = new.created_time, "
                         "updated_time = new.updated_time, shares = new.shares, "
                         "likes = new.likes, comments = new.comments, "
                         "picture = new.picture, icon = new.icon, "
                         "source = new.source, status_type = new.status_type, "
                         "object_id = new.object_id, "
                         "application_id = new.application_id, "
                         "application_name = new.application_name, "
                         "by_text = new.by_text, by_href = new.by_href, "
                         "by_redirect = new.by_redirect, to_id = new.to_id, "
                         "to_name = new.to_name, "
                         "message_tags_count = new.message_tags_count, "
                         "updatetime = new.updatetime, "
                         "searchkeyword = new.searchkeyword, "
                         "page_id = new.page_id WHERE "
                         "%(table)s.id::text = new.id::text;"),
                         {"table": AsIs(tableName)}
        )
        self.cur.execute(("SELECT * FROM pg_indexes WHERE tablename = "
                          "'%(table)s';"),
                         {"table": AsIs(tableName)}
        )
        indexResp = self.cur.fetchall()
        if not indexResp:
            self.cur.execute("CREATE INDEX ON %(table)s (id);",
                            {"table": AsIs(tableName)}
            )
        self.con.commit()

    def create_comment_table(self, tableName):
        self.cur.execute(("CREATE TABLE IF NOT EXISTS %(table)s(id text, "
                          "from_id text, from_name text, message text, "
                          "created_time text, likes int, comments int, "
                          "page_id bigint, post_id text, post_created_time text, "
                          "updatetime text, app_id text );"),
                          {"table": AsIs(tableName)}
        )
        self.cur.execute(("CREATE OR REPLACE RULE key_update AS ON INSERT TO "
                          "%(table)s WHERE (EXISTS ( SELECT 1 FROM %(table)s "
                          "WHERE %(table)s.id::text = new.id::text)) "
                          "DO INSTEAD UPDATE %(table)s SET from_id = new.from_id, "
                          "from_name = new.from_name, message = new.message, "
                          "created_time = new.created_time, likes = new.likes, "
                          "comments = new.comments, page_id = new.page_id, "
                          "post_id = new.post_id, "
                          "post_created_time = new.post_created_time, "
                          "updatetime = new.updatetime WHERE "
                          "%(table)s.id::text = new.id::text;"),
                          {"table": AsIs(tableName)}
        )
        self.cur.execute(("SELECT * FROM pg_indexes WHERE tablename = "
                          "'%(table)s';"),
                         {"table": AsIs(tableName)}
        )
        indexResp = self.cur.fetchall()
        if not indexResp:
            self.cur.execute("CREATE INDEX ON %(table)s (id);",
                            {"table": AsIs(tableName)}
            )
        self.con.commit()

    def get_posts_id_upd(self, tableName):
        self.cur.execute("SELECT id, updated_time from %(table)s;",
                                {"table": AsIs(tableName)}
        )
        data = self.cur.fetchall()
        self.con.commit()
        return data

def check_out_config(fileName):
    config = {}
    configUpdate = config.update
    with open(fileName, "r") as f:
        lines = f.readlines()
    for l in lines:
        if "#" in l or "=" not in l:
            continue
        temp = l.split("=")
        configUpdate({temp[0].strip(): temp[1].strip().strip("\n")})
    return config

def verify_config(config):
    processSetting = ("log_file", )
    storeDatabase = ("host", "port", "dbname", "user", "password")
    fbSetting = ("app_id", "app_secret", "app_token_file")
    verifyList = [processSetting, storeDatabase, fbSetting]
    for unit in verifyList:
        for i in unit:
            if not config.get(i):
                raise ValueError("No {} given".format(i))

def start():
    logger.info("*" * 18 + " Start FB crawling " + "*" * 18)
    return time.time()

def get_groups(storeTable, config, fileName="groups"):
    tasks = []
    tasksAppend = tasks.append
    groupsIDs = []
    groupsIDsAppend = groupsIDs.append
    with open(fileName, "r") as f:
        lines = f.readlines()
    t = []
    for l in lines:
        groupID = l.strip().strip("\n")
        groupsIDsAppend(groupID)
        t.append(groupID)
        if len(t) > 2:
            tasksAppend({"table": storeTable,
                        "category": "post",
                        "config": config,
                        "targets": t
                       }
            )
            t = []
    tasksAppend({"table": storeTable,
                "category": "post",
                "config": config,
                "targets": t
               }
    )
    logger.info("%s groups to working" % len(lines))
    return tasks, groupsIDs, len(lines)

def get_processes(limit=8):
    if mp.cpu_count() > limit:
        return limit
    return mp.cpu_count()

def run(task):
    if task.get("category") == "post":
        pCrawler = PostCrawler(config["app_id"],
                               config["app_secret"],
                               config["app_token_file"]
        )
    if task.get("category") == "comment":
        pCrawler = CommentCrawler(config["app_id"],
                                  config["app_secret"],
                                  config["app_token_file"]
        )
    pRecorder = PgSQLRecorder(task.get("config"))
    for target in task.get("targets", []):
        try:
            data = pCrawler.get(target)
        except Exception as e:
            logger.error("{0}:{1}".format(target, e))
            recordApply = False
            error = False
        else:
            recordApply = True
            error = False
            if len(data) == 0:
                recordApply = False
            if isinstance(data, dict) and data.get("error"):
                recordApply = False
                error = True
        if recordApply:
            try:
                pRecorder.record(task.get("table"), data, task.get("category"))

            except Exception as e:
                logger.error("{0}:{1}".format(target, e))
                pRecorder.con.rollback()
            else:
                logger.info("{0} ... done".format(target))
        else:
            if error:
                logger.warning("%s: %s" % (target, data["error"].get("message")))
            else:
                logger.info("{0} ... no data".format(target))
    pRecorder.close()

def get_posts(storeTable, config, posts, days=1):
    logger.info("%s posts ID, and filtering in %s days" % (len(posts), days))
    temp = []
    tempAppend = temp.append
    daysAgo = (datetime.today() - timedelta(days=days)).strftime("%Y-%m-%d")
    t = []
    for post in posts:
        updDate = datetime.strptime(post.get("updated_time"), 
                                    "%Y-%m-%d %H:%M:%S"
        )
        if updDate.strftime("%Y-%m-%d") > daysAgo:
            t.append(post.get("id"))
        if len(t) > 4:
            tempAppend({"table": storeTable,
                        "category": "comment",
                        "config": config,
                        "targets": t
                       }
            )
            t = []
    tempAppend({"table": storeTable,
                "category": "comment",
                "config": config,
                "targets": t
               }
    )
    if len(t) != 0:
        counts = len(temp) * 5  - ( 5 - len(t) )
    else:
        counts = len(temp) * 5
    logger.info("After filtering, %s posts ID for now" % counts)
    return temp, counts

def filter_posts_by_groups(allPosts, groupsIDs):
    posts = []
    postsAppend = posts.append
    for post in allPosts:
        groupsID = post.get("id").split("_")[0]
        if groupsID in groupsIDs:
            postsAppend({"id": post.get("id"),
                         "updated_time": post.get("updated_time")}
            )
    return posts

def stop():
    recorder.close()
    cStop = time.time()
    logger.info("Requests: {}".format(requestedCounts))
    logger.info("FB crawling stopped, total time taken: {} sec.".format(
                round(cStop - cStart, 2))
    )

if __name__=="__main__":
    path = os.path.split(os.path.realpath(__file__))[0]
    iniFile = os.path.join(path, "sci-9.ini")
    groupsFile = os.path.join(path, "groups")
    if iniFile:
        config = check_out_config(iniFile)
    else:
        raise ValueError("No initial file given.")
    verify_config(config)
    logging.getLogger('elasticsearch.trace').setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    fh = logging.FileHandler(config["log_file"])
    fh.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s[%(levelname)s] %(message)s")
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    processingDate = date.today().strftime("%Y_%m")
    dbConfig = ("host={0} port={1} dbname={2} user={3} password={4}"
                "".format(config["host"],
                          config["port"],
                          config["dbname"],
                          config["user"],
                          config["password"])
    )
    try:
        cStart = start()
        postTable = "fb_post_" + processingDate
        tasks, groupsID, counts = get_groups(postTable, 
                                             dbConfig, 
                                             groupsFile
        )
        requestedCounts = counts
        recorder = PgSQLRecorder(dbConfig)
        recorder.create_post_table(postTable)
        p = mp.Pool(get_processes(config.get("processes")))
        p.map(run, tasks)
        p.close()
        p.join()
        commentTable = "fb_comment_" + processingDate
        posts = filter_posts_by_groups(recorder.get_posts_id_upd(postTable),
                                       groupsID
        )
        tasks, counts = get_posts(commentTable,
                                  dbConfig,
                                  posts
        )
        requestedCounts = requestedCounts + counts * 3
        recorder.create_comment_table(commentTable)
        p = mp.Pool(get_processes(config.get("processes")))
        p.map(run, tasks)
        p.close()
        p.join()
        stop()
    except Exception as e:
        logger.error("Main error: " + str(e))
        raise