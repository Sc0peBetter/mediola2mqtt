"""Tests for _send_gateway_request — HTTP status handling and logging."""

import logging
import unittest
from unittest import mock

from tests._helper import load_module


class _FakeResponse:
    def __init__(self, status_code, text=''):
        self.status_code = status_code
        self.text = text
        self.ok = 200 <= status_code < 400


class SendGatewayRequestTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.m = load_module()

    def test_2xx_response_returns_true(self):
        with mock.patch.object(self.m.requests, 'get',
                               return_value=_FakeResponse(200, 'OK')):
            self.assertTrue(self.m._send_gateway_request(
                {"XC_FNC": "Send2"}, 'mediola1'))

    def test_4xx_response_returns_false_and_warns(self):
        with mock.patch.object(self.m.requests, 'get',
                               return_value=_FakeResponse(404, 'not found')):
            with self.assertLogs(self.m.logger, level='WARNING') as captured:
                self.assertFalse(self.m._send_gateway_request(
                    {"XC_FNC": "Send2"}, 'mediola1'))
        self.assertEqual(captured.records[0].levelno, logging.WARNING)
        self.assertIn('404', captured.output[0])

    def test_5xx_response_returns_false_and_errors(self):
        with mock.patch.object(self.m.requests, 'get',
                               return_value=_FakeResponse(500, 'boom')):
            with self.assertLogs(self.m.logger, level='WARNING') as captured:
                self.assertFalse(self.m._send_gateway_request(
                    {"XC_FNC": "Send2"}, 'mediola1'))
        self.assertEqual(captured.records[0].levelno, logging.ERROR)
        self.assertIn('500', captured.output[0])

    def test_request_exception_returns_false_and_errors(self):
        with mock.patch.object(self.m.requests, 'get',
                               side_effect=self.m.requests.ConnectionError(
                                   'no route')):
            with self.assertLogs(self.m.logger, level='WARNING') as captured:
                self.assertFalse(self.m._send_gateway_request(
                    {"XC_FNC": "Send2"}, 'mediola1'))
        self.assertEqual(captured.records[0].levelno, logging.ERROR)
        self.assertIn('no route', captured.output[0])

    def test_unknown_mediolaid_returns_false_and_errors(self):
        # No HTTP call expected because host resolution fails first.
        with mock.patch.object(self.m.requests, 'get') as mocked:
            with self.assertLogs(self.m.logger, level='WARNING') as captured:
                self.assertFalse(self.m._send_gateway_request(
                    {"XC_FNC": "Send2"}, 'does-not-exist'))
            mocked.assert_not_called()
        self.assertEqual(captured.records[0].levelno, logging.ERROR)

    def test_non_2xx_does_not_leak_xc_pass(self):
        """Redaction must still apply to the body+payload log line."""
        # mediola1 has no password; inject one to verify redaction.
        with mock.patch.object(self.m, 'get_mediola',
                               return_value={'host': '192.0.2.10',
                                             'password': 'super-secret'}):
            with mock.patch.object(self.m.requests, 'get',
                                   return_value=_FakeResponse(403, 'nope')):
                with self.assertLogs(self.m.logger, level='WARNING') as cap:
                    self.assertFalse(self.m._send_gateway_request(
                        {"XC_FNC": "Send2"}, 'mediola1'))
        self.assertNotIn('super-secret', cap.output[0])
        self.assertIn('***', cap.output[0])


if __name__ == '__main__':
    unittest.main()
