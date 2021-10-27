import grpc
import json
from keyviserver.proto import index_pb2, index_pb2_grpc


class Index(object):
    """
    KeyviServer index client.
    """

    def __init__(self, host, port=7586, **kwargs):
        self.host = host
        self.port = port
        self.channel = grpc.insecure_channel(host + ":" + str(port))
        self.stub = index_pb2_grpc.IndexStub(self.channel)
        try:
            self.info()
        except grpc.RpcError:
            raise ConnectionError('Failed to connect')

    def __setitem__(self, key, value):
        self.set(key, value)

    def __getitem__(self, key):
        value = self.get(key)
        if value is None:
            raise KeyError(key)
        return value

    def info(self):
        response = self.stub.Info(index_pb2.InfoRequest())
        return response.info

    def set(self, key, value):
        if not isinstance(value, (str, bytes)):
            value = json.dumps(value)
        self.stub.Set(index_pb2.SetRequest(key=key, value=value))

    def mset(self, key_value_dict):
        for key in key_value_dict:
            value = key_value_dict[key]
            if not isinstance(value, (str, bytes)):
                key_value_dict[key] = json.dumps(value)

        self.stub.MSet(index_pb2.MSetRequest(key_values=key_value_dict))

    def get(self, key):
        response = self.stub.Get(index_pb2.GetRequest(key=key))
        return json.loads(response.value) if response.value else None

    def get_fuzzy(self, key, max_edit_distance=3, min_exact_prefix=2):
        response = self.stub.GetFuzzy(index_pb2.GetFuzzyRequest(key=key, max_edit_distance=max_edit_distance, min_exact_prefix=min_exact_prefix))
        return response.matches

    def get_near(self, key, min_exact_prefix=2, greedy=False):
        response = self.stub.GetNear(index_pb2.GetNearRequest(key=key, min_exact_prefix=min_exact_prefix, greedy=greedy))
        return response.matches

    def flush(self, asynchronous=False):
        self.stub.Flush(index_pb2.FlushRequest(asynchronous=asynchronous))

    def force_merge(self, max_segments=1):
        self.stub.ForceMerge(index_pb2.ForceMergeRequest(max_segments=max_segments))
