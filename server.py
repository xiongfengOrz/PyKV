from __future__ import (absolute_import)

from pykv.database import TinyDB
from pykv.queries import Query, QueryImpl, QueryOps

import zmq
from zmq.devices import ProcessDevice
import random

import json
import traceback
import sys, os, errno
from cStringIO import StringIO
import time

import cPickle as pickle
import copy_reg
import types
import shutil
import logging



def reduce_method(m):
    return (getattr, (m.__self__, m.__func__.__name__))

copy_reg.pickle(types.MethodType, reduce_method)


def router_dealer(client_uri, internal_uri):
    pd = ProcessDevice(zmq.QUEUE, zmq.ROUTER, zmq.DEALER)
    pd.bind_in(client_uri)
    pd.bind_out(internal_uri)
    pd.setsockopt_in(zmq.IDENTITY, 'ROUTER')
    pd.setsockopt_out(zmq.IDENTITY, 'DEALER')
    return pd

class Server(object):
    def __init__(self, db, client_uri, internal_uri, lock_uri, read_only=False):
        self.client_uri = client_uri
        self.internal_uri = internal_uri
        self.rep_uri = internal_uri.replace("*", "localhost")
        self.lock_uri = lock_uri
        self.auto_reload = zmq.NONBLOCK if read_only else 0
        self.db = db
        self.running = False

    def start(self):
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REP)
        self.socket.connect(self.rep_uri)
            
    def run(self):
        self.running = True
        while self.running:
            try:
                message = self.socket.recv(self.auto_reload)
            except zmq.ZMQError as e:
                pass
            logging.debug("Received request: %s" % message)
            try:
                message = json.loads(message)
                if message["mode"] == "exec":
                    # Make extra checks here
                    old_stdout = sys.stdout
                    stdout = sys.stdout = StringIO()
                    try:
                        try:
                            co = compile(message["command"], "<command-line>", "eval")
                        except SyntaxError:
                            co = compile(message["command"], "<command-line>", "exec")
                        ret = eval(co, globals()) 
                        
                    except:
                        output = sys.exc_info()
                    else:
                        output = ret
                    sys.stdout = old_stdout
                elif message["mode"] == "readall":
                    output = db
                elif message["mode"] == "lock":
                    output = {"locked": True, "uri": self.lock_uri}
                elif message["mode"] == "unlock":
                    output = {"locked": False}
                else:
                    try:
                        entry = globals()[message["db"]]
                        func = getattr(entry, message["func"])
                        print(func)
                    except AttributeError:
                        output = "Server not find this func {0}".format(message["func"])     
                    else: # to do : split this to simple funcs                     
                        try:
                            cond = None
                            if message.get("index",None):
                                query = Query(message["index"].pop(0))
                                operation = getattr(query, message["operation"].pop(0))
                                cond = operation(message["args"].pop(0))
                                while message["connection"]:
                                    if message["index"]:
                                        query = Query(message["index"].pop(0))
                                        operation = getattr(query, message["operation"].pop(0))                                
                                        cond = getattr(cond, message["connection"].pop(0))(operation(message["args"].pop(0)))
                                    else:
                                        cond = getattr(cond, message["connection"].pop(0))()
                                print(cond)
                            if message["func"] == "insert":
                                output = func(message["insert_item"])
                            elif message["func"] == "update":
                                query_op = QueryOps()
                                print(message["update_op"])
                                update_op = getattr(query_op, message["update_op"])
                                print("update_op: ", update_op)
                                output = func(update_op, cond)
                            else:
                                output = func(cond) if cond else func() # like db.__len__()
                        except AttributeError:
                            output = "Server not find this operation {0}".format(message["operation"])
                            raise
                        
                     
            except:
                output = traceback.format_exc()
                logging.error(traceback.print_exc())
                
            if type(output).__name__ in ['listiterator', 'dictionary-keyiterator']:
                output = list(output)
            try:
                output = json.dumps(output)
            except:
                output = str(output)
            self.socket.send(output)
            
            if message["mode"] == "lock":
                self.normal_socket = self.socket
                self.socket = zmq.Context().socket(zmq.REP)
                self.socket.bind(self.lock_uri.replace("localhost", "*"))
                logging.debug("Locked and listening on %s" % self.lock_uri)
            elif message["mode"] == "unlock":
                self.socket.close()
                self.socket = self.normal_socket
                logging.debug("Unlocked")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("db_filename", default="/tmp/db.pkl", nargs='?')
    parser.add_argument("client_uri", default="tcp://*:5559", nargs='?')
    parser.add_argument("internal_uri", default="tcp://*:5560", nargs='?')
    parser.add_argument("lock_uri", default="tcp://*:5558", nargs='?')
    parser.add_argument("-v", "--verbosity", action="count", default=0)
    args = parser.parse_args()
    if args.verbosity == 1:
        logging.basicConfig(level=logging.INFO)
    elif args.verbosity >= 2:
        logging.basicConfig(level=logging.DEBUG)

    logging.info("Starting server: client_uri=%s internal_uri=%s lock_uri=%s"
                 % (args.client_uri, args.internal_uri, args.lock_uri))
    router = router_dealer(args.client_uri, args.internal_uri)
    db = TinyDB('db.json')
    query = Query()
    server = Server(db, args.client_uri, args.internal_uri, args.lock_uri)
    router.start()
    server.start()
    server.run()
 
