# -*- coding: utf-8 -*-
"""Encapsulate mongodb related operations.

Basic Rules:
    * Collections hierarchical naming.
    * Documents are homogeneous in each collection.

Examples:
    db.session <--metadata=[
                            [_id=session_id,created_time,DBVersion,...]
                           ]
    db.session.resource <--resource_list=[
                            [_id=x,cluster_type,login_server],
                            [_id=y,cluster_type,login_server],
                            ...
                           ]
    db.session.resource.config <--static=[
                            [_id=x,hpc|grid|local|cloud,...],
                            [_id=y,hpc|grid|local|cloud,...],
                            ...
                           ]
    db.session.resource.workload <--dynamic=[
                            [timestamp,x,...],
                            [timestamp,x,...],
                            [timestamp,y,...],
                            ...
                           ]
    db.session.resource.bandwidth <--network=[
                            [timestamp,src=x,dst=y,meas],
                            [timestamp,src=y,dst=x,meas],
                            ...,
                           ]
    db.session.bundle.bundle_manager_uid
    db.session.bundle.bundle_manager_uid.bundle

Notes:
    In db, uid for each resource are converted by ip2id().

    Based on pymongo 2.8 API
"""
_DBVersion="1.0.0"

import datetime
import pymongo

import radical.utils as ru
from   radical.pilot.utils import DBConnectionInfo

def ip2id (ip) :
    return ip.replace('.', "_DOT_")

def id2ip (Id) :
    return Id.replace("_DOT_", '.')

class DBException(Exception):
    def __init__(self, msg, obj=None):
        Exception.__init__(self, msg)
        self._obj = obj

class Session():
    """This class encapsulates db access methods.
    """
    def __init__(self, db_url, db_name="AIMES_bundle"):
        url = ru.Url(db_url)
        if db_name:
            url.path = db_name

        mongo, db, dbname, _, _ = ru.mongodb_connect (url)

        self._client = mongo
        self._db     = db
        self._dbname = dbname
        self._dburl  = str(url)
        if url.username and url.password:
            self._dbauth = "{}:{}".format(url.username, url.password)
        else:
            self._dbauth = None

        self._session_id = None

        # shortcuts to collections
        #    db.session
        self._s   = None
        #    db.session.resource
        self._r = None
        #    db.session.resource.config
        self._rc   = None
        #    db.session.resource.workload
        self._rw   = None
        #    db.session.resource.bandwidth
        self._bw  = None
        #    db.session.bundle_manager
        self._bm  = None

    @staticmethod
    def new(sid, db_url, db_name="AIMES_bundle"):
        """Creates a new session (factory method).
        """
        creation_time = datetime.datetime.utcnow()

        dbs = Session(db_url, db_name)
        session_metadata = dbs._create(sid, creation_time)

        connection_info = DBConnectionInfo(
            session_id=sid,
            dbname=dbs._dbname,
            dbauth=dbs._dbauth,
            dburl=dbs._dburl
        )

        return (dbs, session_metadata, connection_info)

    def _create(self, sid, creation_time):
        """Creates a new session.
        """
        # make sure session doesn't exist
        if sid and self._db[sid].count() != 0:
            raise DBException ("Session {} already exists.".format(sid))

        self._session_id = sid

        self._s = self._db[sid]
        # more metadata are coming
        metadata = {"_id"       : sid,
                    "created"   : creation_time,
                    "version"   : _DBVersion}
        self._s.insert(metadata)

        # Namespaced subcollections separated by the . character are
        # syntactic suger, subcollections has no relationship with root
        # collections. Create the collection shortcuts
        self._r  = self._db["{}.resource".format(sid)]
        self._rc = self._db["{}.resource.config".format(sid)]
        self._rw = self._db["{}.resource.workload".format(sid)]
        self._bw = self._db["{}.resource.bandwidth".format(sid)]
        self._bm = self._db["{}.bundle_manager".format(sid)]

    @staticmethod
    def reconnect(sid, db_url, db_name="AIMES_bundle"):
        """Reconnects to an existing session.
        """
        dbs = Session(db_url=db_url, db_name=db_name)
        session_metadata = dbs._reconnect(sid)

        connection_info = DBConnectionInfo(
            session_id=sid,
            dbname=dbs._dbname,
            dbauth=dbs._dbauth,
            dburl=dbs._dburl
        )

        return (dbs, session_metadata, connection_info)

    def _reconnect(self, sid):
        """Reconnects to an existing session (private).
        """
        # make sure session exists
        if sid not in self._db.collection_names():
            raise DBException("DB session {} doesn't exist.".format(sid))

        self._s = self._db[sid]
        if self._s.find({"_id": sid}).count() != 1:
            raise DBException("DB session {} metadata doesn't exist.".format(sid))

        # self._s.update({"_id"  : sid},
        #                {"$set" : {"connected" : datetime.datetime.utcnow()}})

        self._session_id = sid

        # create the collection shortcuts
        self._r  = self._db["{}.resource".format(sid)]
        self._rc = self._db["{}.resource.config".format(sid)]
        self._rw = self._db["{}.resource.workload".format(sid)]
        self._bw = self._db["{}.resource.bandwidth".format(sid)]
        self._bm = self._db["{}.bundle_manager".format(sid)]

        # return session metadata
        return self._s.find_one({"_id": sid})

    @property
    def session_id(self):
        return self._session_id

    ####################################################################
    #                    Server-side methods                           #
    ####################################################################
    def add_resource_list(self, resource_list):
        """Add resource list to db.session.resource.

        The "login_server" field is used to uniquely identify
        each resource. Since mongodb _id can't contain '.', call
        ip2id() to replace '.' with '_DOT_'.
        """
        # TODO support merge with existing records
        docs = resource_list.values()
        for d in docs:
            d["_id"] = ip2id(d["login_server"])

        self._r.insert(docs)

    def update_resource_config(self, config):
        save_id = config["_id"]
        config["_id"] = ip2id(config["_id"])
        self._rc.update(
                {"_id" : config["_id"]}, config, upsert=True)
        config["_id"] = save_id

    def update_resource_workload(self, workload):
        self._rw.update(
                {"resource_id" : workload["resource_id"]}, workload, upsert=True)

    ####################################################################
    #                    Client-side methods                           #
    ####################################################################
    def register_bundle_manager(self, bm_id, ip_addr):
        self._bm_id = bm_id
        doc = {"_id" : self._bm_id, "ip_addr" : ip_addr}
        self._bm.update({"_id" : self._bm_id}, doc, upsert=True)

    def get_resource_list(self):
        resource_list = []
        for r in self._r.find():
            resource_list.append( id2ip(str(r['_id'])) )
        return resource_list

    def get_resource_config(self, resource_name):
        return self._rc.find_one({"_id": ip2id(resource_name)})

    def get_resource_workload(self, resource_name):
        return self._rw.find_one({"resource_id": resource_name})
