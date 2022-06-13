from kafka import KafkaConsumer
from enum import Enum
import pandas as pd
import logging
import json
from common.session import session

from cassandra.cluster import Session, BatchStatement, PreparedStatement, ConnectionShutdown
from cassandra import ConsistencyLevel
import numpy as np

FRAME_SIZE = 2
TableTypes = Enum('TableTypes', 'Q1_Q3 Q2 Q4')

Q1_Q3_ins = (
    "INSERT INTO domain__pages (domain, page) VALUES (?,?)",
    ["domain", "page"],
    [np.dtype(str), np.dtype(str)]
)

Q2_ins = (
    "INSERT INTO user_id__pages (user_id, page) VALUES (?,?)",
    ["user_id", "page"],
    [np.dtype(str), np.dtype(str)]
)

Q4_ins = (
    "INSERT INTO page_id__page (page_id, page) VALUES (?,?)",
    ["page_id", "page"],
    [np.dtype(str), np.dtype(str)]
)

def brk_connect(name: str, port: str) -> KafkaConsumer:
    while True:
        try:
            producer = KafkaConsumer("wiki",
                                    bootstrap_servers=f"{name}:{port}",
                                    value_deserializer=lambda s: pd.Series(json.loads(s.decode("utf-8"))),
                                    consumer_timeout_ms=10000
            )
            logging.warning(f"Connected to {name}:{port}")
            return producer
        except Exception as err:
            logging.error(err)

def fstream(consumer: KafkaConsumer) -> list[pd.DataFrame, str]:
    mm_, slist = None, [] 
    for msg in consumer:
        msg = msg.value
        (DD, MM, YY), (hh, mm, _, _) = map(lambda s: s.split(":") if ":" in s else s.split("-"), msg.created_at.split("::"))
        if mm_ is None:
            DD_, MM_, YY_, hh_, mm_ = DD, MM, YY, hh, mm
        if mm_ != mm:
            yield pd.DataFrame(slist), f"tweets_{DD_}_{MM_}_{YY_}_{hh_}_{mm_}"
            DD_, MM_, YY_, hh_, mm_ = DD, MM, YY, hh, mm
            slist.clear()
        slist += [msg]
    yield pd.DataFrame(slist), f"tweets_{DD_}_{MM_}_{YY_}_{hh_}_{mm_}"


def cassandra_ins_df(s: Session, df: pd.DataFrame, 
        ins: tuple[str|PreparedStatement, list[str], list[np.dtype]]) -> None:
    match ins:
        case str(cmd), list(cols), list(types):
            df = df[cols].dropna()
            prep_cmd = s.prepare(cmd)
            for c, t in zip(cols, types):       
                df[c] = df[c].astype(t)
            logging.warning(f"InsertCMD: {cmd}")
        case prep_cmd, list(cols), list(types):
            pass

    batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
    df.apply(lambda row: batch.add(prep_cmd, [row[col] for col in cols]), 1)
    try:
        s.execute(batch)
    except ConnectionShutdown as e: 
        logging.error(f"FatalError: {str(e)}")
        exit()
    except Exception as e:
        logging.error(f"Error: {str(e)}")
        cassandra_ins_df(s,df.iloc[:df(len)//2],(prep_cmd, cols, types))
        cassandra_ins_df(s,df.iloc[df(len)//2:],(prep_cmd, cols, types))

def stream(consumer: KafkaConsumer):
    tns = {TableTypes.Q1_Q3: [], TableTypes.Q2: [], TableTypes.Q4: []}
    for msg in consumer:
        msg = msg.value
        print(msg)
        tns[TableTypes.Q1_Q3] += [msg[["domain", "page"]]]
        tns[TableTypes.Q2] += [msg[["user_id", "page"]]]
        tns[TableTypes.Q4] += [msg[["page_id", "page"]]]
        for tt in tns.keys():
            if len(tns[tt]) == FRAME_SIZE: yield tt, pd.DataFrame(tns[tt]), tns[tt].clear()
    for tt in tns.keys(): yield tt, pd.DataFrame(tns[tt]), tns[tt].clear()

def main():
    ss = session("node", "9042", "wiki")
    for tt, df, _ in stream(brk_connect("broker", "9092")):
        logging.error(tt.value)
        logging.error(df)
        match(tt):
            case TableTypes.Q1_Q3:
                cassandra_ins_df(ss, df, Q1_Q3_ins)
            case TableTypes.Q2:
                cassandra_ins_df(ss, df, Q2_ins)
            case TableTypes.Q4:
                cassandra_ins_df(ss, df, Q4_ins)

if __name__ == "__main__":
    main()