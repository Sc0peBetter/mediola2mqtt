"""Tests for IT switch address handling — non-numeric address must not crash."""

import unittest
from unittest import mock

from tests._helper import load_module


class ITSwitchAddressTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.m = load_module()

    def test_numeric_it_address_builds_payload(self):
        # A01 is a valid family/device pair: family A -> 0, device 01 -> 0.
        payload = self.m._build_switch_payload(
            cfg={'type': 'IT'}, dtype='IT', adr='A01', msg_payload=b'ON')
        self.assertEqual(payload, {
            'XC_FNC': 'SendSC', 'type': 'IT', 'data': '00E',
        })

    def test_non_numeric_it_address_returns_none(self):
        # The fallback path int(adr[1:]) used to raise ValueError and crash
        # the whole MQTT callback. It must now log and return None.
        result = self.m._build_switch_payload(
            cfg={'type': 'IT'}, dtype='IT', adr='AXX', msg_payload=b'ON')
        self.assertIsNone(result)

    def test_non_numeric_off_command_returns_none(self):
        result = self.m._build_switch_payload(
            cfg={'type': 'IT'}, dtype='IT', adr='ABC', msg_payload=b'OFF')
        self.assertIsNone(result)

    def test_on_message_with_non_numeric_address_does_not_raise(self):
        """End-to-end: an MQTT message targeting a malformed IT switch must
        not propagate ValueError out of the callback thread."""
        # The cfg in the index has no on_value, forcing the fallback path.
        # Inject a switch with a malformed address into the index.
        self.m._switch_command_index[('mediola1', 'IT', 'ZZZ')] = {
            'type': 'IT', 'mediola': 'mediola1',
        }

        class _M:
            topic = 'mediola/switches/mediola1/IT_ZZZ/set'
            qos = 0
            payload = b'ON'

        try:
            with mock.patch.object(self.m, '_send_gateway_request',
                                   return_value=True) as send:
                self.m.on_message(None, None, _M())
                # Gateway request must NOT have been sent for malformed addr.
                send.assert_not_called()
        finally:
            del self.m._switch_command_index[('mediola1', 'IT', 'ZZZ')]


if __name__ == '__main__':
    unittest.main()
