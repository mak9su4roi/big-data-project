import logging
from fastapi import FastAPI
from common.session import session
import pandas as pd
import os
from datetime import date
from string import Template
from time import time

app = FastAPI()
sessions = { n: {"prs": [n, "9042", "wiki"], "handle": None} for n in ["node"] }

Q1 = "SELECT DISTINCT domain FROM domain__pages;"
Q2 = "SELECT page FROM user_id__pages WHERE user_id = ?"
Q3 = "SELECT DOMAIN, COUNT(page) as page_num FROM domain__pages GROUP BY domain;"
Q4 = "SELECT page FROM page_id__page WHERE page_id = ?;"

def pandas_factory(colnames, rows):
    """Read the data returned by the driver into a pandas DataFrame"""
    print(len(colnames))
    return pd.DataFrame(rows, columns=colnames)

class Try:
    def __init__(self, val, default=None):
        self.__val = val
        self.__default = default
        self.__err = False

    def get(self):
        if self.__err:
            return self.__err
        if self.__val is None:
            return self.__default
        return self.__val

    def __and__(self, f):
        if self.__val is not None and self.__err == False:
            try:
                logging.error(f"YYYY: {self.__val}")
                self.__val = f(self.__val)
            except Exception as e:
                self.__err = str(e)
        return self

def cassandra_get_df(q: str, *args, last_attempt=False) -> pd.DataFrame | None:
    for k, meta in sessions.items():
        handle = meta["handle"] 
        try: 
            if handle is None and last_attempt:
                handle = session(*meta["prs"])
                sessions[k]["handle"] = handle
            elif handle is None:
                continue
            t=time()
            if args:
                prep_q = handle.prepare(q)
                res = pd.DataFrame(handle.execute(prep_q, args))
            else:
                res = pd.DataFrame(handle.execute(q))
            return res if len(res) else None
        except Exception as e:
            sessions[k]["handle"] = None
            continue
    if not last_attempt:
        return cassandra_get_df(q, *args, last_attempt=True)
    raise Exception("Connection Error")

@app.get("/domains/")
def q1():
    msg = f"ERR"
    return (Try([Q1], msg)
            & (lambda x: cassandra_get_df(*x))
            #& (lambda df: df.apply(lambda r: r.domain, axis=1))
    ).get()

@app.get("/user_id/{uid}")
def q2(uid: str):
    msg = f"User with uid {uid} created 0 pages"
    return (Try((Q2, uid), msg)
            & (lambda x: cassandra_get_df(*x))
            & (lambda df: df.apply(lambda r: r.page, axis=1))
    ).get()

@app.get("/domain-pages/")
def q3():
    msg = f"Nothing"
    return (Try([Q3], msg)
        & (lambda x: cassandra_get_df(*x))
        & (lambda df: df.apply(lambda r:{"domain": r.domain, "page_num": r.page_num}, axis=1))
    ).get()


@app.get("/page_id/{pid}")
def q4(pid: str):
    msg = f"There is no page associated wiht {pid}"
    return (Try((Q4, pid), msg)
            & (lambda x: cassandra_get_df(*x))
            & (lambda df: df.apply(lambda r: r.page, axis=1))
    ).get()

