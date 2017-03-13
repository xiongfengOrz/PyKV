import zmq
import json
import traceback
import logging
import argparse

from pykv.queriesinfo import QueryInfo, Query


class Client(object):
    def __init__(self, db, socket):
        self.db = db
        self._socket = socket

    def __len__(self):
        return self._send(func = "__len__") 
    
    def __delitem__(self, element):
        '''
        type(element) : dict
        ''' 
        if not isinstance(element, dict):
            raise ValueError('Del element is not a dictionary')        
        
        queryinfo = QueryInfo(entry = element.keys(), op = ["__eq__"], args = element.values())
        return self.remove(queryinfo)
    
    def __contains__(self, element):
        '''
        type(element) : dict
        '''
        if not isinstance(element, dict):
            raise ValueError('Contains element is not a dictionary')  
                
        queryinfo = QueryInfo(entry = element.keys(), op = ["__eq__"], args = element.values())
        return True if self.search(queryinfo) else False
    
    def __setitem__(self, key, value):
        self.insert(dict(key = value))     
        
    def insert(self, element):
        '''
        :type element: list
        :param element: for multi insert, it is a list of dict
        '''
        if not isinstance(element, list):
            raise ValueError('Element is not a list')

        logging.warning("Running %s %s %s", "insert",
                        self.db, element)
        
        return self._send(func = "insert", insert_item = element)
    
    def remove(self, queryinfo):
        if type(queryinfo) != QueryInfo:
            raise ValueError('Remove args is not QueryInfo')   
        
        logging.warning("Running %s %s %s %s %s", queryinfo.entry,
                        "remove", queryinfo.op, queryinfo.args, queryinfo.connection)  
        
        return self._send(func = "remove", index = queryinfo.entry, operation = queryinfo.op,
                             args = queryinfo.args, connection = queryinfo.connection)  
    
    def update(self, update_op, queryinfo):
        if type(queryinfo) != QueryInfo:
            raise ValueError('Update args is not QueryInfo') 
        if update_op not in ["decrement", "delete", "increment"]:
            raise ValueError('Update args is not reasonal update_op') 
        
        logging.warning("Running %s %s %s %s %s %s", queryinfo.entry,
                        "update", queryinfo.op, queryinfo.args, queryinfo.connection, update_op)  
        
        return self._send(func = "update", index = queryinfo.entry, operation = queryinfo.op, 
                        args = queryinfo.args, connection = queryinfo.connection, update_op = update_op)         
    
    def search(self, queryinfo):
        if type(queryinfo) != QueryInfo:
            raise ValueError('Search args is not QueryInfo')
    
        logging.warning("Running %s %s %s %s %s", queryinfo.entry,
                        "search", queryinfo.op, queryinfo.args, queryinfo.connection)  
        
        return self._send(func = "search", index = queryinfo.entry, operation = queryinfo.op,
                     args = queryinfo.args, connection = queryinfo.connection)
        
    

    def _send(self, func=None, index=None, operation=None, 
              args=None, connection=None, update_op=None, insert_item=None, **kwargs):
        
        kwargs = kwargs if kwargs is not None else {}
        if insert_item:
            message = json.dumps({"mode": "run",
                                 "db": self.db,
                                 "func": func,
                                 "insert_item": insert_item})            
        else:
            message = json.dumps({"mode": "run",
                              "db": self.db,
                              "func": func,
                              "index": index,
                              "operation": operation,
                              "args": args,
                              "connection": connection,
                              "update_op": update_op,
                              "kwargs": kwargs})
      
        self._socket.send(message)
        answer = self._socket.recv()
        answer = json.loads(answer)
        return answer

def client_factory(db, uri="tcp://localhost:5559"):
    context = zmq.Context()
    logging.debug("Connecting to server on %s" % uri)
    socket = context.socket(zmq.REQ)
    socket.connect(uri)
    return Client(db, socket)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("in_uri", default="tcp://*:5559", nargs='?')
    parser.add_argument("-v", "--verbosity", action="count", default=0)
    args = parser.parse_args()
    if args.verbosity >= 1:
        logging.basicConfig(level=logging.DEBUG)

    client = client_factory("db")
    
    quer1 = Query()
    print(client.search(quer1.name == "he"))

    

