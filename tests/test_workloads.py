from catalogue import Archive
import workloads
from tests.common import MemoryCase


class WorkloadTests(MemoryCase):
    def test_finite_workloads_match_specification(self):
        for case in workloads.CASES:
            receipt = self.archive.ingest_batch(case, workloads.events(case, 1))
            workloads.verify(self.archive, receipt, case, 1)

    def test_workload_validation(self):
        for value in (0, -1, True, 0.5):
            with self.assertRaises(ValueError):
                list(workloads.events('late-veto', value))
        with self.assertRaises(ValueError):
            list(workloads.events('missing', 1))
