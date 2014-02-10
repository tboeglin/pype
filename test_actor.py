from actor import Actor
import logging
import sys

logging.basicConfig()

if len(sys.argv) < 2:
    print 'need a zmq url to bind to as an argument'
    sys.exit()
port = sys.argv[1]
print 'will bind to', port

class TestActor(Actor):
    def handle_ndarray(self, metadata, data):
        print 'got ndarray with metadata', metadata
        return dict(a=3, b=4, action='ndarray'), 'no result'

t = TestActor(port, copy_in=True)
