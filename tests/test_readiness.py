from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from vectorbench.experiments.pilot.run import Server


class FinalPostmasterReadiness(unittest.TestCase):
    def test_temporary_init_server_must_not_admit_copy_client(self):
        phase = {"name": "bash", "reads": 0}

        def docker(*args, **kwargs):
            if "/proc/1/comm" in args:
                phase["reads"] += 1
                if phase["reads"] > 1:
                    phase["name"] = "postgres"
                return phase["name"]
            if "pg_isready" in args:
                if phase["name"] != "postgres":
                    raise AssertionError("pg_isready can falsely accept the temporary init server")
                return "accepting connections"
            raise AssertionError(args)

        with tempfile.TemporaryDirectory() as directory:
            server = Server("postgres", Path(directory))
            with patch("vectorbench.experiments.pilot.run.docker", side_effect=docker), \
                 patch("vectorbench.experiments.pilot.run.Postgres", return_value=object()), patch("vectorbench.experiments.pilot.run.time.sleep"):
                server.connect()
            self.assertEqual(phase["reads"], 2)
            self.assertTrue((Path(directory) / "readiness.json").exists())


if __name__ == "__main__":
    unittest.main()
