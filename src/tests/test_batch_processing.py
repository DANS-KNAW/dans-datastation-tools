import time
from datetime import datetime

from datastation.common.batch_processing import BatchProcessor


class TestBatchProcessor:

    def test_process_pids(self, capsys):
        batch_processor = BatchProcessor()
        pids = ["1", "2", "3"]
        callback = lambda pid: print(pid)
        batch_processor.process_pids(pids, callback)
        captured = capsys.readouterr()
        assert captured.out == "1\n2\n3\n"

    def test_process_pids_with_wait(self, capsys):
        batch_processor = BatchProcessor(wait=0.1)
        pids = ["1", "2", "3"]
        callback = lambda pid: print(pid)
        start_time = datetime.now()
        batch_processor.process_pids(pids, callback)
        end_time = datetime.now()
        captured = capsys.readouterr()
        assert captured.out == "1\n2\n3\n"
        assert (end_time - start_time).total_seconds() >= 0.2

    def test_process_pids_with_wait_on_iterator(self, capsys):
        def as_is(rec):
            time.sleep(0.1)
            return rec
        batch_processor = BatchProcessor(wait=0.1)
        pids = map(as_is, ["1", "2", "3"])
        callback = lambda pid: print(pid)
        start_time = datetime.now()
        batch_processor.process_pids(pids, callback)
        end_time = datetime.now()
        captured = capsys.readouterr()
        assert captured.out == "1\n2\n3\n"
        assert (end_time - start_time).total_seconds() >= 0.5
