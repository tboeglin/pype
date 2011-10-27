from zmq.utils import jsonapi as json
import numpy

def decode_ndarray(metadata, data):
    """Convert the `data` to a numpy array using shape and dtype from
    `metadata`.  Return None if something fails"""
    #sanity checks
    if 'shape' not in metadata.keys():
        logging.error('decode_ndarray: we need at least a shape')
        return None
    if 'dtype' not in metadata.keys():
        logging.warning('decode_ndarray: no "dtype" in metadata, defaulting to float')
    ary = None
    try:
        ary = numpy.frombuffer(buffer(data), dtype=metadata['dtype']).reshape(metadata['shape'])
    except:
        logging.exception('decode_ndarray: could not reconstruct numpy array')
    finally:
        return ary
        

def encode_ndarray(ary):
    metadata = dict()
    metadata['dtype'] = ary.dtype.name
    metadata['shape'] = ary.shape

    return (metadata, ary.data)
