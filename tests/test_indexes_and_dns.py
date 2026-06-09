"""Tests for the lookup indexes and DNS cache TTL."""

import time
import unittest
from unittest import mock

from tests._helper import load_module


class BuildIndexesTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.m = load_module()

    def test_blind_command_index_populated(self):
        cfg = self.m._blind_command_index.get(('mediola2', 'ER', '01'))
        self.assertIsNotNone(cfg)
        self.assertEqual(cfg['type'], 'ER')

    def test_blind_status_index_uses_packet_type(self):
        # ER blind -> ER status packet
        self.assertIn(('mediola2', 'ER', '01'), self.m._blind_status_index)
        # RT blind -> R2 status packet (lowercase address)
        self.assertIn(('mediola1', 'R2', '5a25d5'), self.m._blind_status_index)

    def test_button_index_lowercases_address(self):
        # button adr was '3D5E00' in config; status packets arrive lowercased.
        self.assertIn(('mediola1', 'IT', '3d5e00'), self.m._button_index)

    def test_switch_index_includes_explicit_adr(self):
        self.assertIn(('mediola1', 'IT', 'A01'), self.m._switch_command_index)


class DnsCacheTtlTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.m = load_module()

    def setUp(self):
        with self.m._mediola_host_ip_cache_lock:
            self.m._mediola_host_ip_cache.clear()
            self.m._dns_force_refresh_last.clear()

    def test_cache_is_used_within_ttl(self):
        with mock.patch.object(self.m.socket, 'gethostbyname',
                               return_value='1.2.3.4') as resolver:
            self.assertEqual(self.m._resolve_host_ip('host.example'), '1.2.3.4')
            self.assertEqual(self.m._resolve_host_ip('host.example'), '1.2.3.4')
            self.assertEqual(resolver.call_count, 1)

    def test_cache_expires_after_ttl(self):
        with mock.patch.object(self.m.socket, 'gethostbyname',
                               side_effect=['1.2.3.4', '5.6.7.8']) as resolver:
            self.assertEqual(self.m._resolve_host_ip('host.example'), '1.2.3.4')
            # Force-age the cache entry past the TTL.
            with self.m._mediola_host_ip_cache_lock:
                ip, _ = self.m._mediola_host_ip_cache['host.example']
                self.m._mediola_host_ip_cache['host.example'] = (
                    ip, time.monotonic() - (self.m.DNS_CACHE_TTL + 10))
            self.assertEqual(self.m._resolve_host_ip('host.example'), '5.6.7.8')
            self.assertEqual(resolver.call_count, 2)

    def test_force_refresh_bypasses_cache(self):
        with mock.patch.object(self.m.socket, 'gethostbyname',
                               side_effect=['1.2.3.4', '5.6.7.8']) as resolver:
            self.assertEqual(self.m._resolve_host_ip('host.example'), '1.2.3.4')
            self.assertEqual(self.m._resolve_host_ip('host.example',
                                                    force_refresh=True),
                             '5.6.7.8')
            self.assertEqual(resolver.call_count, 2)

    def test_failed_lookup_invalidates_cache(self):
        with mock.patch.object(self.m.socket, 'gethostbyname',
                               return_value='1.2.3.4'):
            self.assertEqual(self.m._resolve_host_ip('host.example'), '1.2.3.4')
        with mock.patch.object(self.m.socket, 'gethostbyname',
                               side_effect=self.m.socket.gaierror('nope')):
            # Force a failure that bypasses the cache.
            self.assertIsNone(self.m._resolve_host_ip('host.example',
                                                     force_refresh=True))
        # Cache entry should have been dropped on failure.
        self.assertNotIn('host.example', self.m._mediola_host_ip_cache)

    def test_force_refresh_is_rate_limited(self):
        with mock.patch.object(self.m.socket, 'gethostbyname',
                               side_effect=['1.2.3.4', '5.6.7.8']) as resolver:
            self.assertEqual(
                self.m._resolve_host_ip('host.example', force_refresh=True),
                '1.2.3.4')
            # Within the cooldown the cached value is served, no DNS hit.
            self.assertEqual(
                self.m._resolve_host_ip('host.example', force_refresh=True),
                '1.2.3.4')
            self.assertEqual(resolver.call_count, 1)
            # After the cooldown a forced refresh resolves again.
            with self.m._mediola_host_ip_cache_lock:
                self.m._dns_force_refresh_last['host.example'] -= (
                    self.m.DNS_FORCE_REFRESH_COOLDOWN + 1)
            self.assertEqual(
                self.m._resolve_host_ip('host.example', force_refresh=True),
                '5.6.7.8')
            self.assertEqual(resolver.call_count, 2)


class GetMediolaidByAddressTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.m = load_module()

    def setUp(self):
        with self.m._mediola_host_ip_cache_lock:
            self.m._mediola_host_ip_cache.clear()
            self.m._dns_force_refresh_last.clear()

    def test_known_source_returns_matching_id(self):
        # Hosts in the test config are IP literals; gethostbyname echoes them.
        with mock.patch.object(self.m.socket, 'gethostbyname',
                               side_effect=lambda h: h):
            self.assertEqual(
                self.m.get_mediolaid_by_address(('192.0.2.20', 1902)),
                'mediola2')

    def test_unknown_source_returns_none(self):
        # Spoofed/unknown senders must NOT fall back to a default id —
        # the UDP listener is unauthenticated.
        with mock.patch.object(self.m.socket, 'gethostbyname',
                               side_effect=lambda h: h):
            self.assertIsNone(
                self.m.get_mediolaid_by_address(('203.0.113.99', 1902)))


if __name__ == '__main__':
    unittest.main()
