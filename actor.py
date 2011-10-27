import logging
import types
from zmq.core import Context
from zmq.utils import jsonapi as json
from zmq import PUSH, PULL


class Actor(object):
    def __init__(self, data_in_url, copy_in=False):
        self._ctx = Context.instance()
        self._copy_in = copy_in
        self._in_sock = self._ctx.socket(PULL)
        self._in_sock.bind(data_in_url)

    def data_received(self, metadata, data):
        pass

    def run(self):
        while True:
            try:
                parts = self._in_sock.recv_multipart(copy=self._copy_in)
                # load the metadata
                try:
                    if not self._copy_in:
                        metadata = json.loads(parts[0].bytes)
                    else:
                        metadata = json.loads(parts[0])
                except:
                    logging.exception('%r: error parsing metadata', self)
                    continue
                # make sure it's a dict
                if type(metadata) != types.DictType:
                    logging.error('%r: metadata is not a dict', self)
                    continue
                payload = parts[1:]
                # a memoryview will probably be more useful to
                # handlers than a Message...
                if not self._copy_in:
                    payload = [m.buffer for m in payload]

                #get the action and corresponding handler
                action = metadata.get('action', None)
                if action is None:
                    logging.error('%r: cannot handle msg: no action found', self)
                    continue
                handler = getattr(self, 'handle_%s' % action, None)
                if handler is None:
                    logging.error('%r: no handler found for action "%s"', self, action)
                    continue
                #try calling the handler
                try:
                    out_metadata, out_payload = handler(metadata, payload)
                except:
                    logging.exception('%r: handler for action "%s" failed', self, action)
                    continue
                # see if we have the next hop available and send it the result
                hops = metadata.get('hops')
                if hops is not None and type(hops) is types.ListType and len(hops) > 0:
                    logging.debug('%r: next hops %r', self, hops)
                    next_hop = hops[0]
                    out_metadata['hops'] = hops[1:]
                    try:
                        encoded_metadata = json.dumps(out_metadata)
                    except:
                        logging.exception('%r: encoding metadata failed')
                        continue
                    parts = [encoded_metadata]
                    if type(out_payload) is types.ListType or type(out_payload) is types.TupleType:
                        parts.extend(out_payload)
                    else:
                        parts.append(out_payload)
                    out_sock = self._ctx.socket(PUSH)
                    out_sock.connect(next_hop)
                    out_sock.send_multipart(parts)
                    # by default zmq will flush the socket queue
                    # before closing it entirely
                    out_sock.close()
                else: #no next hops
                    logging.debug('%r: no next hops', self)
                
            except: #top level try block
                logging.exception('%r: some strange thing happened', self)
                continue
            
            
                
