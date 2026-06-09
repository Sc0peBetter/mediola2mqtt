"""Regression tests for callback robustness, the blind movement engine,
status packet handling, and position-state restore."""

import threading
import unittest
from unittest import mock

from tests._helper import load_module


class _Msg:
    def __init__(self, topic, payload):
        self.topic = topic
        self.qos = 0
        self.payload = payload


class PositionPayloadTests(unittest.TestCase):
    """Malicious or junk MQTT payloads must never raise out of a callback."""

    @classmethod
    def setUpClass(cls):
        cls.m = load_module()

    def test_invalid_position_payloads_do_not_raise(self):
        for payload in ('inf', '-inf', 'nan', '⅓', '', 'abc', '1e999'):
            with mock.patch.object(self.m, 'send_blind_command') as send:
                with self.subTest(payload=payload):
                    self.m.handle_blind_position('ER', '01', 'mediola2',
                                                 payload)
                    send.assert_not_called()

    def test_huge_position_is_clamped_not_crashing(self):
        # Parses fine, clamps to 100; blind has no travel_time so the
        # command is rejected after clamping without a gateway call.
        with mock.patch.object(self.m, 'send_blind_command') as send:
            self.m.handle_blind_position('ER', '01', 'mediola2', '99999')
            send.assert_not_called()

    def test_on_message_with_invalid_position_does_not_raise(self):
        self.m.on_message(None, None, _Msg(
            'mediola/blinds/mediola2/ER_01/position/set', b'inf'))

    def test_on_message_swallows_handler_exceptions(self):
        original = self.m._handle_blind_command

        def _boom(*args, **kwargs):
            raise RuntimeError('handler exploded')

        self.m._handle_blind_command = _boom
        try:
            with self.assertLogs(self.m.logger, level='ERROR'):
                self.m.on_message(None, None, _Msg(
                    'mediola/blinds/mediola2/ER_01/set', b'open'))
        finally:
            self.m._handle_blind_command = original


class BlindPayloadTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.m = load_module()

    def test_er_tilt_unicode_numeric_returns_none(self):
        # '⅓'.isnumeric() is True but int('⅓') raises; this used to crash
        # the MQTT network thread.
        with self.assertLogs(self.m.logger, level='WARNING'):
            result = self.m._build_blind_payload(
                cfg={}, dtype='ER', adr='01',
                msg_payload='⅓'.encode(), spayload='⅓')
        self.assertIsNone(result)

    def test_er_tilt_positive_and_zero(self):
        up = self.m._build_blind_payload(
            cfg={}, dtype='ER', adr='01', msg_payload=b'5', spayload='5')
        self.assertEqual(up['data'], '010A')
        down = self.m._build_blind_payload(
            cfg={}, dtype='ER', adr='01', msg_payload=b'0', spayload='0')
        self.assertEqual(down['data'], '010B')

    def test_er_named_command(self):
        result = self.m._build_blind_payload(
            cfg={}, dtype='ER', adr='01', msg_payload=b'open',
            spayload='open')
        self.assertEqual(result['data'], '0101')


class SwitchPayloadValidationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.m = load_module()

    def test_family_code_out_of_range_returns_none(self):
        for adr in ('101', 'Q01', 'Z05'):
            with self.subTest(adr=adr):
                with self.assertLogs(self.m.logger, level='WARNING'):
                    result = self.m._build_switch_payload(
                        cfg={'type': 'IT'}, dtype='IT', adr=adr,
                        msg_payload=b'ON')
                self.assertIsNone(result)

    def test_device_code_out_of_range_returns_none(self):
        for adr in ('A00', 'A17', 'A99'):
            with self.subTest(adr=adr):
                with self.assertLogs(self.m.logger, level='WARNING'):
                    result = self.m._build_switch_payload(
                        cfg={'type': 'IT'}, dtype='IT', adr=adr,
                        msg_payload=b'ON')
                self.assertIsNone(result)

    def test_upper_bound_address_encodes(self):
        result = self.m._build_switch_payload(
            cfg={'type': 'IT'}, dtype='IT', adr='P16', msg_payload=b'ON')
        self.assertEqual(result['data'], 'FFE')


class MovementEngineTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.m = load_module()

    def setUp(self):
        self._publishes = []
        self._patch = mock.patch.object(
            self.m, '_safe_publish',
            side_effect=lambda t, p, retain=False:
                self._publishes.append((t, p, retain)))
        self._patch.start()

    def tearDown(self):
        for ident in list(self.m.blind_movements) + list(self.m.blind_timers):
            self.m._clear_movement_state(ident)
        with self.m.blind_state_lock:
            self.m.blind_positions.clear()
        self._patch.stop()

    def test_calc_position_midway_opening(self):
        mov = {'start_time': 0.0, 'start_pos': 0, 'target_pos': 100,
               'travel_time': 10}
        with mock.patch.object(self.m.time, 'monotonic', return_value=5.0):
            self.assertEqual(self.m._calc_position_from_movement(mov), 50)

    def test_calc_position_midway_closing(self):
        mov = {'start_time': 0.0, 'start_pos': 100, 'target_pos': 0,
               'travel_time': 10}
        with mock.patch.object(self.m.time, 'monotonic', return_value=2.5):
            self.assertEqual(self.m._calc_position_from_movement(mov), 75)

    def test_calc_position_clamps_at_target(self):
        mov = {'start_time': 0.0, 'start_pos': 0, 'target_pos': 50,
               'travel_time': 10}
        with mock.patch.object(self.m.time, 'monotonic', return_value=999.0):
            self.assertEqual(self.m._calc_position_from_movement(mov), 50)

    def test_zero_duration_returns_target(self):
        mov = {'start_time': 0.0, 'start_pos': 40, 'target_pos': 40,
               'travel_time': 0}
        self.assertEqual(self.m._calc_position_from_movement(mov), 40)

    def test_begin_movement_backdates_start_time(self):
        # The gateway command went out 5s before _begin_movement ran (slow
        # HTTP): the position estimate and the finish timer must account
        # for the elapsed time.
        ident = 'test_backdate'
        t0 = self.m.time.monotonic() - 5.0
        gen = self.m._begin_movement(ident, 'topic/pos', 0, 100, 10,
                                     send_stop_at_end=False, start_time=t0)
        self.assertIsNotNone(gen)
        try:
            pos = self.m.get_current_blind_position(ident)
            self.assertGreaterEqual(pos, 45)
            self.assertLessEqual(pos, 60)
            with self.m.blind_state_lock:
                remaining = self.m.blind_timers[ident].interval
            self.assertLess(remaining, 6.0)
        finally:
            self.m._clear_movement_state(ident)

    def test_superseding_movement_invalidates_old_generation(self):
        ident = 'test_supersede'
        gen1 = self.m._begin_movement(ident, 'topic/pos', 0, 100, 60,
                                      send_stop_at_end=False)
        current = self.m.get_current_blind_position(ident)
        self.m._clear_movement_state(ident, snapshot_pos=current)
        # New target differs from `current` so a fresh movement starts.
        gen2 = self.m._begin_movement(ident, 'topic/pos', current, 50,
                                      60, send_stop_at_end=False)
        try:
            self.assertIsNotNone(gen1)
            self.assertIsNotNone(gen2)
            self.assertNotEqual(gen1, gen2)
            with self.m.blind_state_lock:
                self.assertEqual(self.m.blind_movements[ident]['gen'], gen2)
        finally:
            self.m._clear_movement_state(ident)


class StatusPacketTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.m = load_module()

    def setUp(self):
        self._publishes = []
        self._patch = mock.patch.object(
            self.m, '_safe_publish',
            side_effect=lambda t, p, retain=False:
                self._publishes.append((t, p, retain)))
        self._patch.start()

    def tearDown(self):
        self._patch.stop()

    def test_er_status_packet_publishes_state(self):
        handled = self.m.handle_packet_v4(
            b'{"type": "ER", "data": "010A"}', 'mediola2')
        self.assertTrue(handled)
        self.assertEqual(self._publishes, [
            ('mediola/blinds/mediola2/ER_01/state', 'opening', True)])

    def test_unmapped_state_is_not_published(self):
        handled = self.m.handle_packet_v4(
            b'{"type": "ER", "data": "01FF"}', 'mediola2')
        self.assertFalse(handled)
        self.assertEqual(self._publishes, [])

    def test_handle_blind_returns_no_topic_for_unmapped_state(self):
        topic, payload, retain = self.m.handle_blind(
            'ER', '01', 'ff', 'mediola2')
        self.assertFalse(topic)

    def test_malformed_packet_is_rejected(self):
        for raw in (b'garbage', b'[]', b'{"type": "ER"}',
                    b'{"type": "ER", "data": 5}'):
            with self.subTest(raw=raw):
                self.assertFalse(self.m.handle_packet_v4(raw, 'mediola2'))

    def test_v6_er_packet_publishes_state(self):
        handled = self.m.handle_packet_v6(
            b'{"type": "ER", "adr": "01", "state": "0A"}', 'mediola2')
        self.assertTrue(handled)
        self.assertEqual(self._publishes[0][1], 'opening')


class ErAddressNormalizationTests(unittest.TestCase):
    def test_unpadded_config_address_matches_status_packets(self):
        yaml_text = """\
mediola:
  - host: 192.0.2.10
    id: mediola1

mqtt:
  host: localhost
  port: 1883
  discovery_prefix: homeassistant
  topic: mediola
  debug: false

blinds:
  - type: ER
    adr: "5"
    name: Unpadded
    mediola: mediola1
"""
        m = load_module(yaml_text)
        # Commands keep the literal config address (used in topics)...
        self.assertIn(('mediola1', 'ER', '5'), m._blind_command_index)
        # ...but status lookups use the zero-padded decimal form that
        # incoming packets are normalized to.
        self.assertIn(('mediola1', 'ER', '05'), m._blind_status_index)


class ITAddressConfigTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.m = load_module()

    def test_malformed_on_value_returns_none(self):
        # Used to raise ValueError during _build_indexes at import time,
        # crash-looping the add-on on a config typo.
        for bad in ('ZZZZZZZZ', 'XY1'):
            with self.subTest(on_value=bad):
                with self.assertLogs(self.m.logger, level='WARNING'):
                    self.assertIsNone(self.m.get_IT_address(bad))

    def test_resolve_switch_address_skips_malformed_value(self):
        with self.assertLogs(self.m.logger, level='WARNING'):
            adr = self.m._resolve_switch_address(
                {'type': 'IT', 'on_value': 'ZZZZZZZZ'})
        self.assertIsNone(adr)


class PositionRestoreTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.m = load_module()

    def setUp(self):
        with self.m.blind_state_lock:
            self.m.blind_positions.clear()
            self.m.blind_movements.clear()

    def test_retained_position_seeds_state(self):
        self.m.on_message(None, None, _Msg(
            'mediola/blinds/mediola2/ER_01/position', b'42'))
        self.assertEqual(
            self.m.get_current_blind_position('mediola2_ER_01'), 42)

    def test_live_state_wins_over_retained_echo(self):
        with self.m.blind_state_lock:
            self.m.blind_positions['mediola2_ER_01'] = 80
        self.m._seed_blind_position('mediola2', 'ER', '01', '42')
        self.assertEqual(
            self.m.get_current_blind_position('mediola2_ER_01'), 80)

    def test_active_movement_blocks_seeding(self):
        with self.m.blind_state_lock:
            self.m.blind_movements['mediola2_ER_01'] = {
                'start_time': self.m.time.monotonic(), 'start_pos': 0,
                'target_pos': 100, 'travel_time': 60, 'gen': 99999}
        try:
            self.m._seed_blind_position('mediola2', 'ER', '01', '42')
            with self.m.blind_state_lock:
                self.assertNotIn('mediola2_ER_01', self.m.blind_positions)
        finally:
            with self.m.blind_state_lock:
                self.m.blind_movements.clear()

    def test_invalid_retained_payload_is_ignored(self):
        with self.assertLogs(self.m.logger, level='WARNING'):
            self.m._seed_blind_position('mediola2', 'ER', '01', 'inf')
        with self.m.blind_state_lock:
            self.assertNotIn('mediola2_ER_01', self.m.blind_positions)


class CommandDispatchTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.m = load_module()

    def test_dispatch_runs_inline_without_worker(self):
        calls = []
        self.m._dispatch_command(lambda a, b: calls.append((a, b)), 1, 2)
        self.assertEqual(calls, [(1, 2)])

    def test_dispatch_enqueues_when_worker_running(self):
        calls = []
        done = threading.Event()
        self.m._command_worker_started.set()
        worker = threading.Thread(target=self.m._command_worker, daemon=True)
        worker.start()
        try:
            self.m._dispatch_command(
                lambda: (calls.append('ran'), done.set()))
            self.assertTrue(done.wait(timeout=5))
            self.assertEqual(calls, ['ran'])
        finally:
            self.m._command_worker_started.clear()
            self.m._command_queue.put(None)
            worker.join(timeout=5)


if __name__ == '__main__':
    unittest.main()
