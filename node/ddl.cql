CREATE KEYSPACE wiki WITH replication = {
    'class': 'SimpleStrategy', 
    'replication_factor': 1
};

USE wiki;

CREATE TABLE domain__pages ( domain text, page text, PRIMARY KEY(domain, page));

CREATE TABLE user_id__pages (user_id text, page text, PRIMARY KEY (user_id, page));

CREATE TABLE page_id__page (page_id text, page text, PRIMARY KEY (page_id));