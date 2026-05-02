#!/usr/bin/env python
# (c) 2021 Andreas Böhler
# License: Apache 2.0


import paho.mqtt.client as mqtt
import socket
import json
import yaml
import logging
import os
import sys
import requests
import threading
import time
import itertools
import signal

# Module logger. A NullHandler keeps tests / library imports silent until the
# script's main() configures the root logger via basicConfig, at which point
# records propagate to stderr/stdout as usual.
logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

if os.path.exists('/data/options.json'):
    print('Running in hass.io add-on mode')
    with open('/data/options.json', 'r') as fp:
        config = json.load(fp)
elif os.path.exists('/config/mediola2mqtt.yaml'):
    print('Running in legacy add-on mode')
    with open('/config/mediola2mqtt.yaml', 'r') as fp:
        config = yaml.safe_load(fp)
elif os.path.exists('mediola2mqtt.yaml'):
    print('Running in local mode')
    with open('mediola2mqtt.yaml', 'r') as fp:
        config = yaml.safe_load(fp)
else:
    print('Configuration file not found, exiting.')
    sys.exit(1)

# Default HTTP timeout (seconds) for requests to the Mediola gateway. Without
# this, a hung gateway can block MQTT callback threads indefinitely.
HTTP_TIMEOUT = 10

# UDP receive buffer size. v6 status payloads can exceed 1 KB.
UDP_BUF_SIZE = 8192

# Lock protecting blind_positions / blind_timers / blind_movements /
# blind_progress_timers, since they are mutated from threading.Timer threads
# as well as the MQTT callback thread.
blind_state_lock = threading.RLock()

# Track blind positions (identifier -> 0-100) and active movement timers
blind_positions = {}
blind_timers = {}
# Track active movements: identifier -> {'start_time', 'start_pos',
# 'target_pos', 'travel_time', 'gen'}
blind_movements = {}
# Periodic position update timers for smoother percentage feedback in HA
blind_progress_timers = {}

# Monotonic generation counter so timer callbacks can detect when their
# movement has been superseded by a newer command and skip mutating state.
_movement_gen = itertools.count(1)

# Coordinated shutdown flag for the main UDP loop.
_shutdown_event = threading.Event()

# Lookup indexes for O(1) device dispatch. Built by _build_indexes() after
# the config has been loaded. Keys are tuples: (mediolaid, dtype, adr)
# (case preserved for command paths; lowercased for status paths since
# status packets carry lowercase addresses).
_blind_command_index = {}   # (mediolaid, dtype, adr) -> blind cfg
_blind_status_index = {}    # (mediolaid, packet_type, adr.lower()) -> blind cfg
_switch_command_index = {}  # (mediolaid, dtype, adr) -> switch cfg
_button_index = {}          # (mediolaid, packet_type, adr.lower()) -> button cfg

# Populated by main() so functions can reference it as a module global. Tests
# that import the module without running main() simply don't touch it.
mqttc = None


def _redact_payload(payload):
    """Return a copy of payload with XC_PASS scrubbed for safe logging."""
    if isinstance(payload, dict) and 'XC_PASS' in payload:
        return {**payload, 'XC_PASS': '***'}
    return payload


def get_mediola(mediolaid):
    """Return the Mediola config dict for the given id, or None if not found."""
    if isinstance(config['mediola'], list):
        for m in config['mediola']:
            if m.get('id') == mediolaid:
                return m
        return None
    return config['mediola']


def apply_mediola_password(mediolaid, payload):
    """Resolve gateway host and stamp the password (if any) onto payload.
    Returns the host string, or '' if no matching Mediola was found."""
    m = get_mediola(mediolaid)
    if not m:
        return ''
    if m.get('password'):
        payload['XC_PASS'] = m['password']
    return m.get('host', '')


def _send_gateway_request(payload, mediolaid):
    """Send payload to the resolved Mediola gateway. Returns True on success."""
    host = apply_mediola_password(mediolaid, payload)
    if not host:
        logger.error("Could not find matching Mediola for id=%r", mediolaid)
        return False
    url = 'http://' + host + '/command'
    try:
        response = requests.get(url, params=payload,
                                headers={'Connection': 'close'},
                                timeout=HTTP_TIMEOUT)
    except requests.RequestException as err:
        logger.error("Error sending command to Mediola %s: %s", host, err)
        return False
    if not response.ok:
        # Truncate body so a misbehaving gateway can't flood the log.
        body = (response.text or '')[:500]
        # 5xx is a server fault, 4xx is a client/protocol fault — both fail
        # the command, but a 5xx is more actionable for ops.
        log_level = (logging.ERROR if response.status_code >= 500
                     else logging.WARNING)
        logger.log(log_level,
                   "Mediola %s returned HTTP %s for %s: %s",
                   host, response.status_code,
                   _redact_payload(payload), body)
        return False
    if config['mqtt'].get('debug'):
        logger.debug("Send Mediola: %s --> %s",
                     _redact_payload(payload), response.text)
    return True


def get_blind_identifier(mediolaid, dtype, adr):
    return mediolaid + '_' + dtype + '_' + adr


def _calc_position_from_movement(mov):
    """Pure calculation of current estimated position given a movement dict."""
    elapsed = time.monotonic() - mov['start_time']
    total_duration = mov['travel_time'] * abs(mov['target_pos'] - mov['start_pos']) / 100.0
    if total_duration <= 0:
        return mov['target_pos']
    progress = min(elapsed / total_duration, 1.0)
    if mov['target_pos'] > mov['start_pos']:
        return round(mov['start_pos'] + progress * (mov['target_pos'] - mov['start_pos']))
    return round(mov['start_pos'] - progress * (mov['start_pos'] - mov['target_pos']))


def get_current_blind_position(identifier):
    """Calculate the current position based on elapsed movement time."""
    with blind_state_lock:
        mov = blind_movements.get(identifier)
        if mov is not None:
            return _calc_position_from_movement(mov)
        return blind_positions.get(identifier, 100)


def _safe_publish(topic, payload, retain=False):
    """Wrap mqttc.publish to avoid losing the worker thread on transient errors."""
    try:
        mqttc.publish(topic, payload=payload, retain=retain)
    except Exception as err:
        print(f"Error publishing to {topic}: {err}")


def start_blind_progress_updates(identifier, pos_topic, gen):
    """Publish estimated blind position periodically while a timed movement is active.

    Each callback chain is tagged with a generation id; if the active
    movement's generation changes, the old chain ends naturally without
    further publishes and without overwriting the new chain's timer entry.
    """
    def _publish_progress(ident=identifier, topic=pos_topic, my_gen=gen):
        with blind_state_lock:
            mov = blind_movements.get(ident)
            if mov is None or mov.get('gen') != my_gen:
                return
            pos = _calc_position_from_movement(mov)
        _safe_publish(topic, str(pos), retain=True)
        next_timer = threading.Timer(1.0, _publish_progress)
        with blind_state_lock:
            mov = blind_movements.get(ident)
            if mov is None or mov.get('gen') != my_gen:
                return
            blind_progress_timers[ident] = next_timer
        next_timer.start()

    initial = threading.Timer(1.0, _publish_progress)
    with blind_state_lock:
        existing = blind_progress_timers.pop(identifier, None)
        blind_progress_timers[identifier] = initial
    if existing is not None:
        existing.cancel()
    initial.start()


def send_blind_command(blind_cfg, adr, command, mediolaid):
    """Send open/close/stop command for a blind via HTTP. Returns True on success."""
    dtype = blind_cfg.get('type')
    if dtype == 'IR':
        key = command + '_value'
        if key not in blind_cfg:
            print(f"Missing {key} for IR blind: {adr}")
            return False
        payload = {"XC_FNC": "Send2", "type": "CODE", "ir": "01", "code": blind_cfg[key]}
    elif dtype == 'RT':
        prefix = {'open': '20', 'close': '40', 'stop': '10'}.get(command)
        if prefix is None:
            return False
        payload = {"XC_FNC": "SendSC", "type": "RT", "data": prefix + adr}
    elif dtype == 'ER':
        suffix = {'open': '01', 'close': '00', 'stop': '02'}.get(command)
        if suffix is None:
            return False
        try:
            adr_hex = format(int(adr), "02x")
        except (ValueError, TypeError):
            print(f"Invalid ER address: {adr}")
            return False
        payload = {"XC_FNC": "SendSC", "type": "ER", "data": adr_hex + suffix}
    else:
        return False

    return _send_gateway_request(payload, mediolaid)


def _clear_movement_state(identifier, snapshot_pos=None):
    """Cancel timers and reset state for `identifier`.

    Returns the movement dict that was popped (or None) so callers can decide
    whether to send a 'stop' to the gateway.
    """
    with blind_state_lock:
        existing_timer = blind_timers.pop(identifier, None)
        existing_progress = blind_progress_timers.pop(identifier, None)
        mov = blind_movements.pop(identifier, None)
        if snapshot_pos is not None:
            blind_positions[identifier] = snapshot_pos
    if existing_timer is not None:
        existing_timer.cancel()
    if existing_progress is not None:
        existing_progress.cancel()
    return mov


def _begin_movement(identifier, pos_topic, current, target, travel_time,
                    send_stop_at_end, blind_cfg=None, adr=None, mediolaid=None):
    """Register movement state, kick off progress updates, and schedule the
    finish timer. Caller must have already cleared previous state and (where
    appropriate) sent the gateway command that physically starts the blind.
    """
    if target == current:
        return None
    duration = travel_time * abs(target - current) / 100.0
    if duration <= 0:
        return None

    gen = next(_movement_gen)
    with blind_state_lock:
        blind_movements[identifier] = {
            'start_time': time.monotonic(),
            'start_pos': current,
            'target_pos': target,
            'travel_time': travel_time,
            'gen': gen,
        }
    start_blind_progress_updates(identifier, pos_topic, gen)

    def _finish(ident=identifier, fp=target, pt=pos_topic, my_gen=gen,
                stop_at_end=send_stop_at_end, bcfg=blind_cfg, c=adr,
                m=mediolaid):
        with blind_state_lock:
            mov = blind_movements.get(ident)
            if mov is None or mov.get('gen') != my_gen:
                # Superseded by a newer movement; do nothing.
                return
            blind_positions[ident] = fp
            blind_movements.pop(ident, None)
            progress_timer = blind_progress_timers.pop(ident, None)
            blind_timers.pop(ident, None)
        if progress_timer is not None:
            progress_timer.cancel()
        if stop_at_end and bcfg is not None and c is not None and m is not None:
            send_blind_command(bcfg, c, 'stop', m)
        _safe_publish(pt, str(fp), retain=True)

    t = threading.Timer(duration, _finish)
    with blind_state_lock:
        blind_timers[identifier] = t
    t.start()
    return gen


def handle_blind_position(dtype, adr, mediolaid, spayload):
    """Handle a set_position command (0-100) using travel_time-based timing."""
    try:
        target = int(float(spayload))
        target = max(0, min(100, target))
    except (ValueError, TypeError):
        print(f"Invalid position value: {spayload}")
        return

    cfg = _blind_command_index.get((mediolaid, dtype, adr))
    if cfg is None:
        return

    travel_time = cfg.get('travel_time', 0)
    if not travel_time:
        print(f"No travel_time configured for blind: {adr}")
        return

    # IR blinds without stop_value cannot honor an intermediate target;
    # the timed movement would never be terminated.
    if cfg.get('type') == 'IR' and 'stop_value' not in cfg:
        print(f"Cannot set position for IR blind without stop_value: {adr}")
        return

    identifier = get_blind_identifier(mediolaid, dtype, adr)
    topic_identifier = dtype + '_' + adr
    pos_topic = (config['mqtt']['topic'] + '/blinds/' + mediolaid + '/'
                 + topic_identifier + '/position')
    current = get_current_blind_position(identifier)

    # If a movement is in progress and the user requests the currently
    # estimated position as the target, treat it as 'stop here'.
    if target == current:
        with blind_state_lock:
            mov_active = identifier in blind_movements
        if mov_active:
            _clear_movement_state(identifier, snapshot_pos=current)
            send_blind_command(cfg, adr, 'stop', mediolaid)
            _safe_publish(pos_topic, str(current), retain=True)
        return

    _clear_movement_state(identifier, snapshot_pos=current)
    direction = 'open' if target > current else 'close'
    if not send_blind_command(cfg, adr, direction, mediolaid):
        return
    _begin_movement(identifier, pos_topic, current, target, travel_time,
                    send_stop_at_end=True, blind_cfg=cfg, adr=adr,
                    mediolaid=mediolaid)


# Define MQTT event callbacks
def on_connect(client, userdata, flags, rc):
    connect_statuses = {
        0: "Connected",
        1: "incorrect protocol version",
        2: "invalid client ID",
        3: "server unavailable",
        4: "bad username or password",
        5: "not authorised"
    }
    if rc != 0:
        print("MQTT: " + connect_statuses.get(rc, "Unknown error"))
    else:
        setup_discovery()


def on_disconnect(client, userdata, rc):
    if rc != 0:
        print("Unexpected disconnection")
    else:
        print("Disconnected")


def _parse_command_topic(topic):
    """Parse a command topic into (category, mediolaid, dtype, adr, is_position).

    Returns None if the topic doesn't match an expected command shape.
    Expected shapes (base topic may itself contain '/'):
      <base>/<category>/<mediolaid>/<dtype>_<adr>/set
      <base>/<category>/<mediolaid>/<dtype>_<adr>/position/set
    Parses structurally so that '_' inside <mediolaid> or <base> is safe.
    """
    parts = topic.split('/')
    if len(parts) < 5 or parts[-1] != 'set':
        return None
    if parts[-2] == 'position':
        if len(parts) < 6:
            return None
        category = parts[-5]
        mediolaid = parts[-4]
        device_segment = parts[-3]
        is_position = True
    else:
        category = parts[-4]
        mediolaid = parts[-3]
        device_segment = parts[-2]
        is_position = False
    if not category:
        return None
    if '_' not in device_segment:
        return None
    dtype, _, adr = device_segment.partition('_')
    if not dtype or not adr:
        return None
    return category, mediolaid, dtype, adr, is_position


def _build_blind_payload(cfg, dtype, adr, msg_payload, spayload):
    """Return the gateway payload for a blind command, or None on validation
    failure. Logs the reason for failure."""
    er_opcodes = {
        b'open': '01', b'up': '01', b'on': '01',
        b'close': '00', b'down': '00', b'off': '00',
        b'stop': '02',
        b'upstep': '03', b'downstep': '04',
        b'manumode': '05', b'automode': '06', b'togglemode': '07',
        b'longup': '08', b'longdown': '09',
        b'doubleup': '0A', b'doubledown': '0B',
        b'learn': '0C', b'onpulsemove': '0D', b'offpulsemove': '0E',
        b'asclose': '0F', b'asmove': '10',
    }
    rt_opcodes = {
        b'open': '20', b'up': '20', b'on': '20',
        b'close': '40', b'down': '40', b'off': '40',
        b'stop': '10',
    }

    if dtype == 'IR':
        if msg_payload in (b'open', b'up', b'on'):
            if 'open_value' not in cfg:
                print("Missing open_value for IR blind: " + adr)
                return None
            data = cfg['open_value']
        elif msg_payload in (b'close', b'down', b'off'):
            if 'close_value' not in cfg:
                print("Missing close_value for IR blind: " + adr)
                return None
            data = cfg['close_value']
        elif msg_payload == b'stop':
            if 'stop_value' not in cfg:
                print("Missing stop_value for IR blind: " + adr)
                return None
            data = cfg['stop_value']
        else:
            print("Wrong command for IR blind: " + str(msg_payload))
            return None
        return {"XC_FNC": "Send2", "type": "CODE", "ir": "01", "code": data}
    if dtype == 'RT':
        if msg_payload not in rt_opcodes:
            print("Wrong command for RT blind: " + str(msg_payload))
            return None
        return {"XC_FNC": "SendSC", "type": dtype,
                "data": rt_opcodes[msg_payload] + adr}
    if dtype == 'ER':
        try:
            adr_hex = format(int(adr), "02x")
        except (ValueError, TypeError):
            print(f"Invalid ER address: {adr}")
            return None
        if msg_payload in er_opcodes:
            data = adr_hex + er_opcodes[msg_payload]
        elif spayload.isnumeric():
            # tilt - encoded as double-tap up/down
            data = adr_hex + ("0A" if int(spayload) > 0 else "0B")
        else:
            print("Wrong command: " + str(msg_payload))
            return None
        return {"XC_FNC": "SendSC", "type": dtype, "data": data}
    return None


def _build_switch_payload(cfg, dtype, adr, msg_payload):
    """Return the gateway payload for a switch command, or None on validation
    failure. Logs the reason for failure."""
    if dtype not in ('IT', 'IR'):
        return None

    if msg_payload == b'ON':
        value_key = 'on_value'
        it_suffix = 'E'
    elif msg_payload == b'OFF':
        value_key = 'off_value'
        it_suffix = '6'
    else:
        print("Wrong command")
        return None

    if value_key in cfg:
        data = cfg[value_key]
    elif dtype == 'IT' and len(adr) == 3:
        # Old family_code + device_code, A01 - P16. The address is supplied
        # by the user / topic, so a malformed value must not crash the
        # callback thread.
        try:
            device_code = int(adr[1:]) - 1
        except ValueError:
            logger.warning(
                "Invalid IT address (non-numeric device code): %r", adr)
            return None
        data = (format((ord(adr[0].upper()) - 65), 'X')
                + format(device_code, 'X')
                + it_suffix)
    else:
        print(f"Missing {value_key} and unknown type/address: {dtype}/{adr}")
        return None

    if dtype == 'IT':
        return {"XC_FNC": "SendSC", "type": dtype, "data": data}
    return {"XC_FNC": "Send2", "type": "CODE", "ir": "01", "code": data}


def _handle_blind_command(category, mediolaid, dtype, adr, msg_payload, spayload):
    cfg = _blind_command_index.get((mediolaid, dtype, adr))
    if cfg is None:
        print(f"No blind matches command: {mediolaid}/{dtype}/{adr}")
        return

    payload = _build_blind_payload(cfg, dtype, adr, msg_payload, spayload)
    if payload is None:
        return

    if not _send_gateway_request(payload, mediolaid):
        # Gateway rejected the command — leave position state untouched so
        # we don't desync from the physical blind.
        return

    if msg_payload in (b'open', b'up', b'on'):
        blind_command = 'open'
    elif msg_payload in (b'close', b'down', b'off'):
        blind_command = 'close'
    elif msg_payload == b'stop':
        blind_command = 'stop'
    else:
        blind_command = None

    travel_time = cfg.get('travel_time', 0)
    if travel_time and blind_command in ('open', 'close', 'stop'):
        identifier_key = get_blind_identifier(mediolaid, dtype, adr)
        topic_identifier = dtype + '_' + adr
        pos_topic = (config['mqtt']['topic'] + '/blinds/' + mediolaid
                     + '/' + topic_identifier + '/position')
        current = get_current_blind_position(identifier_key)
        _clear_movement_state(identifier_key, snapshot_pos=current)

        if blind_command == 'stop':
            _safe_publish(pos_topic, str(current), retain=True)
        else:
            final_pos = 100 if blind_command == 'open' else 0
            if final_pos == current:
                _safe_publish(pos_topic, str(current), retain=True)
            else:
                # Full open/close: gateway runs blind to physical stop, so
                # we don't send another 'stop' when the timer fires.
                _begin_movement(identifier_key, pos_topic, current,
                                final_pos, travel_time,
                                send_stop_at_end=False)


def _handle_switch_command(category, mediolaid, dtype, adr, msg_payload):
    cfg = _switch_command_index.get((mediolaid, dtype, adr))
    if cfg is None:
        print(f"No switch matches command: {mediolaid}/{dtype}/{adr}")
        return

    payload = _build_switch_payload(cfg, dtype, adr, msg_payload)
    if payload is None:
        return

    if not _send_gateway_request(payload, mediolaid):
        # Switches publish optimistically; just log the failure.
        print(f"Gateway rejected switch command: {mediolaid}/{dtype}/{adr}")


def on_message(client, obj, msg):
    spayload = msg.payload.decode(errors='replace')
    print("Msg: " + msg.topic + " " + str(msg.qos) + " " + str(msg.payload))

    parsed = _parse_command_topic(msg.topic)
    if parsed is None:
        print(f"Ignoring unrecognized topic: {msg.topic}")
        return
    category, mediolaid, dtype, adr, is_position = parsed

    if is_position:
        if category != 'blinds':
            print(f"Ignoring position command for non-blind category: "
                  f"{category} ({msg.topic})")
            return
        handle_blind_position(dtype, adr, mediolaid, spayload)
        return

    if category == 'blinds':
        _handle_blind_command(category, mediolaid, dtype, adr,
                              msg.payload, spayload)
    elif category == 'switches':
        _handle_switch_command(category, mediolaid, dtype, adr, msg.payload)
    else:
        print(f"Ignoring command with unknown category: "
              f"{category} ({msg.topic})")
        return


def on_publish(client, obj, mid):
    print("Pub: " + str(mid))


def on_subscribe(client, obj, mid, granted_qos):
    print("Subscribed: " + str(mid) + " " + str(granted_qos))


def on_log(client, obj, level, string):
    print(string)


def _resolve_mediolaid_and_host(item):
    """Return (mediolaid, host) for a configured device, or (None, None)."""
    if isinstance(config['mediola'], list):
        mediolaid = item.get('mediola')
    else:
        mediolaid = 'mediola'
    m = get_mediola(mediolaid)
    if not m:
        return None, None
    return mediolaid, m.get('host', '')


def _resolve_switch_address(cfg):
    """Determine the address (string) for a switch config, or None."""
    if 'adr' in cfg:
        return cfg['adr']
    stype = cfg.get('type')
    if stype == 'IT' and 'on_value' in cfg:
        return get_IT_address(cfg['on_value'])
    if stype == 'IR' and 'name' in cfg:
        return get_IR_address(cfg['name'])
    return None


def _resolve_blind_address(cfg):
    """Determine the address (string) for a blind config, or None."""
    if 'adr' in cfg:
        return cfg['adr']
    if cfg.get('type') == 'IR' and 'name' in cfg:
        return get_IR_address(cfg['name'])
    return None


def setup_discovery():
    if 'buttons' in config:
        # Buttons are configured as MQTT device triggers
        for ii, cfg in enumerate(config['buttons']):
            if 'type' not in cfg or 'adr' not in cfg:
                print(f"Skipping button[{ii}]: missing type or adr")
                continue
            identifier = cfg['type'] + '_' + cfg['adr']
            mediolaid, host = _resolve_mediolaid_and_host(cfg)
            if not host:
                print('Error: Could not find matching Mediola!')
                continue
            deviceid = "mediola_buttons_" + host.replace(".", "")
            dtopic = config['mqtt']['discovery_prefix'] + '/device_automation/' + \
                     mediolaid + '_' + identifier + '/config'
            topic = config['mqtt']['topic'] + '/buttons/' + mediolaid + '/' + identifier
            payload = {
                "automation_type": "trigger",
                "topic": topic,
                "type": "button_short_press",
                "subtype": "button_1",
                "device": {
                    "identifiers": deviceid,
                    "manufacturer": "Mediola",
                    "name": "Mediola Button",
                },
            }
            _safe_publish(dtopic, json.dumps(payload), retain=True)

    if 'switches' in config:
        for ii, cfg in enumerate(config['switches']):
            stype = cfg.get('type')
            if stype is None:
                print(f"Skipping switch[{ii}]: missing type")
                continue
            adr = _resolve_switch_address(cfg)
            if adr is None:
                print(f"Skipping switch[{ii}]: cannot determine address")
                continue
            identifier = stype + '_' + adr
            mediolaid, host = _resolve_mediolaid_and_host(cfg)
            if not host:
                print('Error: Could not find matching Mediola!')
                continue
            deviceid = "mediola_switches_" + host.replace(".", "")
            dtopic = config['mqtt']['discovery_prefix'] + '/switch/' + \
                     mediolaid + '_' + identifier + '/config'
            topic = config['mqtt']['topic'] + '/switches/' + mediolaid + '/' + identifier
            name = cfg.get('name', "Mediola Switch")
            payload = {
                "command_topic": topic + "/set",
                "payload_on": "ON",
                "payload_off": "OFF",
                "optimistic": True,
                "unique_id": mediolaid + '_' + identifier,
                "name": name,
                "device": {
                    "identifiers": deviceid,
                    "manufacturer": "Mediola",
                    "name": "Mediola Switch",
                },
            }
            mqttc.subscribe(topic + "/set")
            _safe_publish(dtopic, json.dumps(payload), retain=True)

    if 'blinds' in config:
        for ii, cfg in enumerate(config['blinds']):
            btype = cfg.get('type')
            if btype is None:
                print(f"Skipping blind[{ii}]: missing type")
                continue
            adr = _resolve_blind_address(cfg)
            if adr is None:
                print(f"Skipping blind[{ii}]: cannot determine address")
                continue
            identifier = btype + '_' + adr
            mediolaid, host = _resolve_mediolaid_and_host(cfg)
            if not host:
                print('Error: Could not find matching Mediola!')
                continue
            deviceid = "mediola_blinds_" + host.replace(".", "")
            dtopic = config['mqtt']['discovery_prefix'] + '/cover/' + \
                     mediolaid + '_' + identifier + '/config'
            topic = config['mqtt']['topic'] + '/blinds/' + mediolaid + '/' + identifier
            name = cfg.get('name', "Mediola Blind")
            payload = {
                "command_topic": topic + "/set",
                "payload_open": "open",
                "payload_close": "close",
                "optimistic": True,
                "device_class": "blind",
                "unique_id": mediolaid + '_' + identifier,
                "name": name,
                "device": {
                    "identifiers": deviceid,
                    "manufacturer": "Mediola",
                    "name": "Mediola Blind",
                },
            }
            stop_supported = btype != 'IR' or 'stop_value' in cfg
            if stop_supported:
                payload["payload_stop"] = "stop"
            # Position control requires both travel_time AND a working stop;
            # otherwise the timed movement could never be terminated.
            if 'travel_time' in cfg and stop_supported:
                payload['set_position_topic'] = topic + '/position/set'
                payload['position_topic'] = topic + '/position'
            # apply template if defined and override values
            if 'template' in cfg:
                template = cfg['template']
                if 'templates' in config:
                    template_found = False
                    for tpl in config['templates']:
                        if tpl.get('tpl_name') != template:
                            continue
                        template_found = True
                        tpl_pl = dict(tpl)
                        tpl_pl.pop('tpl_name', None)
                        for tpl_key, tpl_value in tpl_pl.items():
                            payload[tpl_key] = tpl_value
                        if 'tilt_command_topic' not in payload and \
                           ('tilt_opened_value' in payload or 'tilt_closed_value' in payload):
                            payload['tilt_command_topic'] = payload['command_topic']
                        break
                    if not template_found:
                        print(f"Missing template: {template}")
                else:
                    print(f"Missing section 'templates' to resolve template: {template}")
            if btype == 'ER' or btype == 'RT':
                payload["state_topic"] = topic + "/state"
            mqttc.subscribe(topic + "/set")
            if 'set_position_topic' in payload:
                mqttc.subscribe(topic + '/position/set')
            _safe_publish(dtopic, json.dumps(payload), retain=True)


def handle_button(packet_type, address, state, mediolaid):
    cfg = _button_index.get((mediolaid, packet_type, address))
    if cfg is None:
        return False, False, False
    cfg_adr = cfg['adr']
    cfg_type = cfg['type']
    identifier = cfg_type + '_' + cfg_adr
    topic = (config['mqtt']['topic'] + '/buttons/' + mediolaid + '/'
             + identifier)
    return topic, state, False


def handle_blind(packet_type, address, state, mediolaid):
    cfg = _blind_status_index.get((mediolaid, packet_type, address))
    if cfg is None:
        return False, False, True
    blind_type = cfg.get('type')
    cfg_adr = _resolve_blind_address(cfg)
    if cfg_adr is None:
        return False, False, True
    identifier = blind_type + '_' + cfg_adr
    topic = (config['mqtt']['topic'] + '/blinds/' + mediolaid + '/'
             + identifier + '/state')
    payload = 'unknown'
    if packet_type == 'ER':
        if state in ('01', '0e'):
            payload = 'open'
        elif state in ('02', '0f'):
            payload = 'closed'
        elif state in ('08', '0a'):
            payload = 'opening'
        elif state in ('09', '0b'):
            payload = 'closing'
        elif state in ('0d', '05'):
            payload = 'stopped'
    elif packet_type == 'R2':
        # For RT/R2 many gateways report coarse state values only.
        # 00:00 is commonly idle/stopped.
        if state in ('00:00', '00'):
            payload = 'stopped'
    return topic, payload, True


# DNS cache TTL in seconds. A bounded TTL avoids serving stale host->IP
# mappings indefinitely after DHCP renewals or gateway failover.
DNS_CACHE_TTL = 300

_mediola_host_ip_cache = {}  # host -> (ipaddr, monotonic_timestamp)
_mediola_host_ip_cache_lock = threading.Lock()


def _resolve_host_ip(host, force_refresh=False):
    """Cached gethostbyname so we don't hit DNS for every UDP packet.

    Entries expire after DNS_CACHE_TTL seconds. Pass force_refresh=True
    to bypass the cache (e.g. after a connection failure).
    """
    now = time.monotonic()
    if not force_refresh:
        with _mediola_host_ip_cache_lock:
            entry = _mediola_host_ip_cache.get(host)
            if entry is not None and (now - entry[1]) < DNS_CACHE_TTL:
                return entry[0]
    try:
        ipaddr = socket.gethostbyname(host)
    except socket.gaierror as err:
        print(f"DNS lookup failed for {host}: {err}")
        # Drop the stale entry on failure so the next call re-resolves.
        with _mediola_host_ip_cache_lock:
            _mediola_host_ip_cache.pop(host, None)
        return None
    with _mediola_host_ip_cache_lock:
        _mediola_host_ip_cache[host] = (ipaddr, now)
    return ipaddr


def get_mediolaid_by_address(addr):
    if not isinstance(config['mediola'], list):
        return 'mediola'
    src_ip = addr[0]
    # First pass uses the cache. If no cached IP matches, refresh once and
    # try again so a renewed DHCP lease doesn't strand us forever.
    for entry in config['mediola']:
        host = entry.get('host')
        if not host:
            continue
        ipaddr = _resolve_host_ip(host)
        if ipaddr is not None and src_ip == ipaddr:
            return entry.get('id', 'mediola')
    for entry in config['mediola']:
        host = entry.get('host')
        if not host:
            continue
        ipaddr = _resolve_host_ip(host, force_refresh=True)
        if ipaddr is not None and src_ip == ipaddr:
            return entry.get('id', 'mediola')
    return 'mediola'


def handle_packet_v4(data, addr):
    try:
        data_dict = json.loads(data)
    except (ValueError, TypeError):
        return False
    if not isinstance(data_dict, dict):
        return False

    try:
        packet_type = data_dict['type']
        raw_data = data_dict['data']
        if not isinstance(raw_data, str) or len(raw_data) < 2:
            return False
        button_addr = raw_data[0:-2].lower()
        state = raw_data[-2:].lower()
    except (KeyError, TypeError, AttributeError):
        return False

    mediolaid = get_mediolaid_by_address(addr)
    topic, payload, retain = handle_button(packet_type, button_addr, state, mediolaid)
    if not topic:
        # ER addresses are 1-byte hex; RT/R2 use the full hex-string prefix.
        try:
            if packet_type == 'ER':
                if len(raw_data) < 4:
                    return False
                blind_addr = format(int(raw_data[0:2], 16), '02d')
            else:
                blind_addr = button_addr
        except ValueError:
            return False
        topic, payload, retain = handle_blind(packet_type, blind_addr, state, mediolaid)

    if topic and payload:
        _safe_publish(topic, payload, retain=retain)
        return True
    return False


def handle_packet_v6(data, addr):
    try:
        data_dict = json.loads(data)
    except (ValueError, TypeError):
        return False
    if not isinstance(data_dict, dict):
        return False

    try:
        packet_type = data_dict['type']
        address = data_dict['adr'].lower()
        raw_state = data_dict['state'].lower()
    except (KeyError, TypeError, AttributeError):
        return False

    state = raw_state[-2:] if packet_type == 'ER' else raw_state
    mediolaid = get_mediolaid_by_address(addr)
    topic, payload, retain = handle_button(packet_type, address, state, mediolaid)
    if not topic:
        try:
            blind_address = format(int(address, 16), '02d') if packet_type == 'ER' else address
        except ValueError:
            return False
        topic, payload, retain = handle_blind(packet_type, blind_address, state, mediolaid)

    if topic and payload:
        _safe_publish(topic, payload, retain=retain)
        return True
    return False


# calculate switch address from on_value for IT switches
def get_IT_address(on_value):
    # ITT-1500 new self-learning code
    if len(on_value) == 8:
        # 26bit address, (2 bit command), 4 bit channel
        return format(int(on_value, 16) & 0xFFFFFFC7, "08x")
    # familiy-code, device-code
    elif len(on_value) == 3:
        family_code = chr(int(on_value[0], 16) + 65)
        device_code = format(int(on_value[1], 16) + 1, "02")
        return family_code + device_code
    else:
        return "0"


# calculate switch "address" from name for IR switches
def get_IR_address(name):
    adr = name.lower()
    adr = ''.join(e for e in adr if e.isalnum() and e.isascii())
    return adr


def _config_mediola_id_for(cfg):
    """Return the mediola id this config entry belongs to, or None."""
    if isinstance(config['mediola'], list):
        return cfg.get('mediola')
    return 'mediola'


def _build_indexes():
    """Index device configs for O(1) dispatch.

    Called at startup (and whenever the config changes) so that incoming
    MQTT messages and UDP status packets can find their matching cfg entry
    without scanning the full config every time.
    """
    _blind_command_index.clear()
    _blind_status_index.clear()
    _switch_command_index.clear()
    _button_index.clear()

    for cfg in config.get('blinds', []):
        btype = cfg.get('type')
        if not btype:
            continue
        cadr = _resolve_blind_address(cfg)
        if cadr is None:
            continue
        mid = _config_mediola_id_for(cfg)
        if mid is None:
            continue
        _blind_command_index.setdefault((mid, btype, cadr), cfg)
        # Status packets: ER blinds receive ER packets, RT blinds receive R2.
        if btype == 'ER':
            _blind_status_index.setdefault((mid, 'ER', cadr.lower()), cfg)
        elif btype == 'RT':
            _blind_status_index.setdefault((mid, 'R2', cadr.lower()), cfg)

    for cfg in config.get('switches', []):
        stype = cfg.get('type')
        if not stype:
            continue
        cadr = _resolve_switch_address(cfg)
        if cadr is None:
            continue
        mid = _config_mediola_id_for(cfg)
        if mid is None:
            continue
        _switch_command_index.setdefault((mid, stype, cadr), cfg)

    for cfg in config.get('buttons', []):
        ctype = cfg.get('type')
        cadr = cfg.get('adr')
        if not ctype or cadr is None:
            continue
        mid = _config_mediola_id_for(cfg)
        if mid is None:
            continue
        _button_index.setdefault((mid, ctype, cadr.lower()), cfg)


# Build dispatch indexes once the config is fully loaded so that helper
# functions (including those triggered by tests that import the module
# without running main()) can rely on them being populated.
_build_indexes()


def _shutdown_handler(signum, frame):
    print(f'Received signal {signum}, shutting down.')
    _shutdown_event.set()


def main():
    """Set up MQTT and run the UDP listen loop. Runs until shutdown."""
    global mqttc

    # Configure the root logger only when run as a script. basicConfig is a
    # no-op if the root logger already has handlers (e.g. set up by an
    # embedding host), so this is safe to call unconditionally.
    log_level = logging.DEBUG if config['mqtt'].get('debug') else logging.INFO
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s %(levelname)s %(name)s: %(message)s',
    )

    # Rebuild indexes in case the config was reloaded between import and run.
    _build_indexes()

    mqttc = mqtt.Client()
    mqttc.on_connect = on_connect
    mqttc.on_subscribe = on_subscribe
    mqttc.on_disconnect = on_disconnect
    mqttc.on_message = on_message

    if config['mqtt'].get('debug'):
        print("Debugging messages enabled")
        mqttc.on_log = on_log
        mqttc.on_publish = on_publish

    if config['mqtt'].get('username') and config['mqtt'].get('password'):
        mqttc.username_pw_set(config['mqtt']['username'],
                              config['mqtt']['password'])
    try:
        mqttc.connect(config['mqtt']['host'], config['mqtt']['port'], 60)
    except (OSError, ConnectionError) as err:
        print(f'Error connecting to MQTT ({err}), will now quit.')
        sys.exit(1)
    mqttc.loop_start()

    listen_port = 1902
    if 'general' in config and 'port' in config['general']:
        listen_port = config['general']['port']

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(('', listen_port))
    sock.settimeout(1.0)

    # Install signal handlers so SIGTERM (systemd, Docker stop) runs cleanup.
    try:
        signal.signal(signal.SIGTERM, _shutdown_handler)
        signal.signal(signal.SIGINT, _shutdown_handler)
    except ValueError:
        # signal.signal only works in the main thread; guard for embedded use.
        pass

    try:
        while not _shutdown_event.is_set():
            try:
                data, addr = sock.recvfrom(UDP_BUF_SIZE)
            except socket.timeout:
                continue
            except OSError as err:
                print(f"Socket error: {err}")
                break

            if config['mqtt'].get('debug'):
                print('Received message: %s' % data)
            _safe_publish(config['mqtt']['topic'], data, retain=False)

            # For the v4 (and probably v5) gateways, the status packet starts
            # with '{XC_EVT}', but for the v6 it starts with 'STA:'.
            if data.startswith(b'{XC_EVT}'):
                data = data.replace(b'{XC_EVT}', b'')
                if not handle_packet_v4(data, addr):
                    if config['mqtt'].get('debug'):
                        print('Error handling v4 packet: %s' % data)
            elif data.startswith(b'STA:'):
                data = data.replace(b'STA:', b'')
                if not handle_packet_v6(data, addr):
                    if config['mqtt'].get('debug'):
                        print('Error handling v6 packet: %s' % data)
    finally:
        print('Shutting down.')
        with blind_state_lock:
            for t in list(blind_timers.values()):
                t.cancel()
            for t in list(blind_progress_timers.values()):
                t.cancel()
            blind_timers.clear()
            blind_progress_timers.clear()
            blind_movements.clear()
        try:
            sock.close()
        except OSError:
            pass
        mqttc.loop_stop()
        try:
            mqttc.disconnect()
        except Exception:
            pass


if __name__ == '__main__':
    main()
