import os
import sqlite3
import sys
import tempfile
import unittest
from contextlib import closing
from unittest.mock import Mock

import pandas as pd
import requests

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from etl.api_sync import sync_api


class SyncTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = os.path.join(self.tmp.name, 'state.sqlite3')
        self.df = pd.DataFrame([
            {'fecha': '2026-09-01', 'cba_nea': 20, 'cbt_nea': 40},
            {'fecha': '2026-08-01', 'cba_nea': 10, 'cbt_nea': 30},
        ])

    def state(self):
        with closing(sqlite3.connect(self.path)) as db:
            return db.execute('SELECT mes, estado FROM envios ORDER BY mes').fetchall()

    def test_success_is_persisted_and_ordered(self):
        response = Mock(status_code=201, text='Se inserto cbt en ECV.Se inserto cbt en EPH.')
        session = Mock()
        session.post.return_value = response
        sync_api(self.df, self.path, session)
        sync_api(self.df, self.path, session)
        self.assertEqual([call.kwargs['data']['mes'] for call in session.post.call_args_list], [8, 9])
        self.assertEqual(self.state(), [('2026-08', 'enviado'), ('2026-09', 'enviado')])

    def test_tls_error_can_be_retried(self):
        session = Mock()
        session.post.side_effect = requests.exceptions.SSLError('TLS')
        with self.assertRaises(RuntimeError):
            sync_api(self.df, self.path, session)
        self.assertEqual(self.state()[0][1], 'pendiente')
        session.post.side_effect = None
        session.post.return_value = Mock(status_code=201, text='Se inserto cbt en ECV.Se inserto cbt en EPH.')
        sync_api(self.df, self.path, session)
        self.assertEqual(self.state()[0][1], 'enviado')

    def test_timeout_requires_review(self):
        session = Mock()
        session.post.side_effect = requests.exceptions.Timeout('timeout')
        with self.assertRaises(RuntimeError):
            sync_api(self.df, self.path, session)
        self.assertEqual(self.state()[0][1], 'revision')
        session.reset_mock()
        with self.assertRaises(RuntimeError):
            sync_api(self.df, self.path, session)
        session.post.assert_not_called()


if __name__ == '__main__':
    unittest.main()
