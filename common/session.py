from cassandra.cluster import Session

def session(address: str, port: int, keyspace: str) -> Session:
    from cassandra.cluster import Cluster
    return Cluster(contact_points=[address], port=port).connect(keyspace=keyspace)