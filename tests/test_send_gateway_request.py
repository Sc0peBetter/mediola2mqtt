"""Tests for _send_gateway_request — HTTP status handling."""

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

    def test_4xx_response_returns_false(self):
        with mock.patch.object(self.m.requests, 'get',
                               return_value=_FakeResponse(404, 'not found')):
            self.assertFalse(self.m._send_gateway_request(
                {"XC_FNC": "Send2"}, 'mediola1'))

    def test_5xx_response_returns_false(self):
        with mock.patch.object(self.m.requests, 'get',
                               return_value=_FakeResponse(500, 'boom')):
            self.assertFalse(self.m._send_gateway_request(
                {"XC_FNC": "Send2"}, 'mediola1'))

    def test_request_exception_returns_false(self):
        with mock.patch.object(self.m.requests, 'get',
                               side_effect=self.m.requests.ConnectionError(
                                   'no route')):
            self.assertFalse(self.m._send_gateway_request(
                {"XC_FNC": "Send2"}, 'mediola1'))

    def test_unknown_mediolaid_returns_false(self):
        # No HTTP call expected because host resolution fails first.
        with mock.patch.object(self.m.requests, 'get') as mocked:
            self.assertFalse(self.m._send_gateway_request(
                {"XC_FNC": "Send2"}, 'does-not-exist'))
            mocked.assert_not_called()


if __name__ == '__main__':
    unittest.main()
