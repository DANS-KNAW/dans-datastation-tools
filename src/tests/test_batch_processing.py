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

    def test_process_pids_with_wait_on_iterator(self, capsys):
        batch_processor = BatchProcessor(wait=0.1)

        def as_is(rec):
            time.sleep(0.1)
            print(f"lazy-{rec}")
            return rec
        pids = map(as_is, ["1", "2", "3"])
        callback = lambda pid: print(pid)
        start_time = datetime.now()
        batch_processor.process_pids(pids, callback)
        end_time = datetime.now()
        captured = capsys.readouterr()
        # as_is is called alternated with callback
        assert captured.out == "lazy-1\n1\nlazy-2\n2\nlazy-3\n3\n"
        assert (end_time - start_time).total_seconds() >= 0.5

    def test_process_pids_with_wait_on_list(self, capsys):
        def as_is(rec):
            time.sleep(0.1)
            print(f"lazy-{rec}")
            return rec
        batch_processor = BatchProcessor(wait=0.1)
        pids = [as_is(rec) for rec in  ["1", "2", "3"]]
        callback = lambda pid: print(pid)
        start_time = datetime.now()
        batch_processor.process_pids(pids, callback)
        end_time = datetime.now()
        captured = capsys.readouterr()
        # as_is is called repeatedly before callback is called repeatedly
        # this illustrates the "don't" of search_api.search.return
        assert captured.out == "lazy-1\nlazy-2\nlazy-3\n1\n2\n3\n"
        assert (end_time - start_time).total_seconds() >= 0.2
