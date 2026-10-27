"""Exercise real adapter transport routing, including deliberately expired connections."""
import json
import unittest
from unittest.mock import patch
from vectorbench.backends.adapters import Qdrant


class Connection:
    def __init__(self, events, fail=False): self.events,self.expired,self.fail=events,True,fail
    def close(self): self.events.append("close"); self.expired=False
    def request(self,method,path,body,headers):
        self.events.append("send")
        if self.expired or self.fail: raise BrokenPipeError("injected stale socket")
        assert isinstance(body,bytes)
    def getresponse(self): return self
    status=200
    def read(self): return b'{"result":"ok"}'


class TransportTests(unittest.TestCase):
    def test_write_serializes_then_closes_stale_socket_before_one_send(self):
        adapter=Qdrant(0); events=[]; adapter.connection=Connection(events)
        original=json.dumps
        def dumps(*args,**kwargs): events.append("serialize"); return original(*args,**kwargs)
        with patch("vectorbench.backends.adapters.json.dumps",side_effect=dumps):
            self.assertEqual(adapter.request("PUT","/collections/items/points?wait=true",{"x":1}),{"result":"ok"})
        self.assertEqual(events,["serialize","close","send"])

    def test_failed_write_is_never_replayed(self):
        adapter=Qdrant(0); events=[]; adapter.connection=Connection(events,fail=True)
        with self.assertRaises(BrokenPipeError):
            adapter.request_encoded("PUT","/collections/items/points?wait=true",b'{}')
        self.assertEqual(events,["close","send"])


if __name__=="__main__": unittest.main()
