from datetime import timedelta
from unittest import TestCase

import pandas as pd

from qutils.monkey_patch import timedelta_to_human


class TestMonkeyPatch(TestCase):
    def test_timedelta_to_human(self):
        for td in timedelta(days=1, seconds=3900), pd.to_timedelta('1d1h5m'):
            self.assertEqual('1.05 days', timedelta_to_human(td, precision=2))
            self.assertEqual('1.0 day', timedelta_to_human(td, precision=1))
        for td in timedelta(days=-1, seconds=-3900), pd.to_timedelta('-1d1h5m'):
            self.assertEqual('1.05 days ago', timedelta_to_human(td, precision=2))
            self.assertEqual('1.0 day ago', timedelta_to_human(td, precision=1))
