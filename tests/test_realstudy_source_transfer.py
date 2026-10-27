"""Replay truncated HTTP responses, not live source or DB qualification."""
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch
from vectorbench.datasets.source import download_range


class TransferTests(unittest.TestCase):
    def test_truncated_get_retries_separately_and_retains_first_failure(self):
        source = {"url":"https://example.org/data","resolution":{"host":"example.org","addresses":["127.0.0.1"]},
                  "etag":'"pinned"',"content_length":1000}
        calls=[]
        with tempfile.TemporaryDirectory() as d:
            def curl(command, **kwargs):
                calls.append(command)
                out=Path(command[command.index("--output")+1]); head=Path(command[command.index("--dump-header")+1])
                head.write_text('HTTP/1.1 206 Partial Content\r\nContent-Range: bytes 20-119/1000\r\nETag: "pinned"\r\n\r\n')
                out.write_bytes(b'x'*(20 if len(calls)==1 else 100))
                return subprocess.CompletedProcess(command,18 if len(calls)==1 else 0,b'',b'partial' if len(calls)==1 else b'')
            with patch("vectorbench.datasets.source.subprocess.run",side_effect=curl),patch("vectorbench.datasets.source.time.sleep"):
                selected=download_range(source,20,100,Path(d)/"piece")
            self.assertEqual(len(calls),2)
            self.assertEqual(selected.read_bytes(),b'x'*100)
            self.assertEqual((Path(d)/"piece.attempt-1").stat().st_size,20)
            self.assertTrue((Path(d)/"piece.attempt-1.json").exists())

    def test_changed_etag_is_never_hidden_by_retry(self):
        source={"url":"https://example.org/data","resolution":{"host":"example.org","addresses":["127.0.0.1"]},
                "etag":'"pinned"',"content_length":1000}
        with tempfile.TemporaryDirectory() as d:
            def curl(command,**kwargs):
                Path(command[command.index("--output")+1]).write_bytes(b'x'*100)
                Path(command[command.index("--dump-header")+1]).write_text('HTTP/1.1 206 Partial Content\r\nContent-Range: bytes 20-119/1000\r\nETag: "changed"\r\n\r\n')
                return subprocess.CompletedProcess(command,0,b'',b'')
            with patch("vectorbench.datasets.source.subprocess.run",side_effect=curl) as request:
                with self.assertRaisesRegex(ValueError,"identity/range changed"):
                    download_range(source,20,100,Path(d)/"piece")
                self.assertEqual(request.call_count,1)


if __name__=="__main__": unittest.main()
