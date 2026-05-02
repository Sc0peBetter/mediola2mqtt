"""Tests for _parse_command_topic — category extraction and validation."""

import unittest

from tests._helper import load_module


class ParseCommandTopicTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.m = load_module()

    def test_blinds_set_topic(self):
        result = self.m._parse_command_topic(
            'mediola/blinds/mediola2/ER_01/set')
        self.assertEqual(result, ('blinds', 'mediola2', 'ER', '01', False))

    def test_switches_set_topic(self):
        result = self.m._parse_command_topic(
            'mediola/switches/mediola1/IT_A01/set')
        self.assertEqual(result, ('switches', 'mediola1', 'IT', 'A01', False))

    def test_position_topic(self):
        result = self.m._parse_command_topic(
            'mediola/blinds/mediola2/ER_01/position/set')
        self.assertEqual(result, ('blinds', 'mediola2', 'ER', '01', True))

    def test_unknown_category_is_still_returned(self):
        # The parser returns the category as-is so the dispatcher can decide.
        result = self.m._parse_command_topic(
            'mediola/widgets/mediola1/IT_A01/set')
        self.assertEqual(result, ('widgets', 'mediola1', 'IT', 'A01', False))

    def test_underscore_in_mediolaid_is_safe(self):
        result = self.m._parse_command_topic(
            'mediola/blinds/my_house/RT_5a25d5/set')
        self.assertEqual(result, ('blinds', 'my_house', 'RT', '5a25d5', False))

    def test_multi_segment_base_topic(self):
        result = self.m._parse_command_topic(
            'home/floor1/mediola/blinds/mediola2/ER_01/set')
        self.assertEqual(result, ('blinds', 'mediola2', 'ER', '01', False))

    def test_missing_set_suffix_returns_none(self):
        self.assertIsNone(self.m._parse_command_topic(
            'mediola/blinds/mediola2/ER_01'))

    def test_topic_too_short_returns_none(self):
        self.assertIsNone(self.m._parse_command_topic('blinds/ER_01/set'))

    def test_position_topic_too_short_returns_none(self):
        self.assertIsNone(self.m._parse_command_topic(
            'blinds/mediola2/ER_01/position/set'))

    def test_malformed_device_segment_returns_none(self):
        # No underscore in device segment.
        self.assertIsNone(self.m._parse_command_topic(
            'mediola/blinds/mediola2/ER01/set'))

    def test_empty_dtype_or_adr_returns_none(self):
        self.assertIsNone(self.m._parse_command_topic(
            'mediola/blinds/mediola2/_01/set'))
        self.assertIsNone(self.m._parse_command_topic(
            'mediola/blinds/mediola2/ER_/set'))


class OnMessageRoutingTests(unittest.TestCase):
    """Verify that on_message routes strictly by category and won't fire the
    wrong device type when a malformed topic happens to match dtype/adr."""

    @classmethod
    def setUpClass(cls):
        cls.m = load_module()

    def setUp(self):
        self.blind_calls = []
        self.switch_calls = []
        self.position_calls = []

        self._orig_blind = self.m._handle_blind_command
        self._orig_switch = self.m._handle_switch_command
        self._orig_position = self.m.handle_blind_position

        self.m._handle_blind_command = lambda *a, **kw: self.blind_calls.append(a)
        self.m._handle_switch_command = lambda *a, **kw: self.switch_calls.append(a)
        self.m.handle_blind_position = lambda *a, **kw: self.position_calls.append(a)

    def tearDown(self):
        self.m._handle_blind_command = self._orig_blind
        self.m._handle_switch_command = self._orig_switch
        self.m.handle_blind_position = self._orig_position

    def _msg(self, topic, payload=b'open'):
        class _M:
            pass
        m = _M()
        m.topic = topic
        m.qos = 0
        m.payload = payload
        return m

    def test_blind_topic_only_invokes_blind_handler(self):
        self.m.on_message(None, None,
                          self._msg('mediola/blinds/mediola2/ER_01/set'))
        self.assertEqual(len(self.blind_calls), 1)
        self.assertEqual(self.switch_calls, [])

    def test_switch_topic_only_invokes_switch_handler(self):
        self.m.on_message(None, None,
                          self._msg('mediola/switches/mediola1/IT_A01/set',
                                    payload=b'ON'))
        self.assertEqual(len(self.switch_calls), 1)
        self.assertEqual(self.blind_calls, [])

    def test_unknown_category_invokes_no_handler(self):
        # An attacker (or buggy publisher) crafts a topic with a bogus
        # category but valid dtype/adr — must NOT be dispatched.
        self.m.on_message(None, None,
                          self._msg('mediola/widgets/mediola2/ER_01/set'))
        self.assertEqual(self.blind_calls, [])
        self.assertEqual(self.switch_calls, [])
        self.assertEqual(self.position_calls, [])


if __name__ == '__main__':
    unittest.main()
