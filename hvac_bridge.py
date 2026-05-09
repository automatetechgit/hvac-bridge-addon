#!/usr/bin/env python3
"""
HVAC Bridge — HAI Omnistat2 Church Thermostat Serial → MQTT
Replaces Node-RED "Thermostat MQTT" tab.
"""

import json, logging, os, struct, sys, time, threading
from dataclasses import dataclass, field
from typing import Optional

import serial
import paho.mqtt.client as mqtt

# ── Configuration ────────────────────────────────────────────────
MQTT_HOST = os.environ.get("MQTT_HOST", "192.168.30.75")
MQTT_PORT = int(os.environ.get("MQTT_PORT", "1883"))
POLL_INTERVAL = int(os.environ.get("POLL_INTERVAL", "10"))
SERIAL_PORTS = {"/dev/ttyUSB1": None, "/dev/ttyUSB2": None}
BAUD = 300
WATCHDOG_STALE_SEC = 30
MQTT_CLIENT_ID = "hvac-bridge-ridge"

# ── Logging ──────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("hvac")

# ── OmniF Temperature Lookup Table (index → °F) ─────────────────
OMNIF = [-40.0,-39.1,-38.2,-37.3,-36.4,-35.5,-34.6,-33.7,-32.8,-31.9,
-31.0,-30.1,-29.2,-28.3,-27.4,-26.5,-25.6,-24.7,-23.8,-22.9,-22.0,-21.1,
-20.2,-19.3,-18.4,-17.5,-16.6,-15.7,-14.4,-13.9,-13.0,-12.1,-11.2,-10.3,
-9.4,-8.5,-7.6,-6.7,-5.8,-4.9,-4.0,-3.1,-2.2,-1.3,-0.4,0.5,1.4,2.3,3.2,
4.1,5.0,5.9,6.8,7.7,8.6,9.5,10.4,11.3,12.2,13.1,14.0,14.9,15.8,16.7,17.6,
18.5,19.4,20.3,21.2,22.1,23.0,23.9,24.8,25.7,26.6,27.5,28.4,29.3,30.2,31.1,
32.0,32.9,33.8,34.7,35.6,36.5,37.4,38.3,39.2,40.1,41.0,41.9,42.8,43.7,44.6,
45.5,46.4,47.3,48.2,49.1,50.0,50.9,51.8,52.7,53.6,54.5,55.4,56.3,57.2,58.1,
59.0,59.9,60.8,61.7,62.6,63.5,64.4,65.3,66.2,67.1,68.0,68.9,69.8,70.7,71.6,
72.5,73.4,74.3,75.2,76.1,77.0,77.9,78.8,79.7,80.6,81.5,82.4,83.3,84.2,85.1,
86.0,86.9,87.8,88.7,89.6,90.5,91.4,92.3,93.2,94.1,95.0,95.9,96.8,97.7,98.6,
99.5,100.4,101.3,102.2,103.1,104.0,104.9,105.8,106.7,107.6,108.5,109.4,110.3,
111.2,112.1,113.0,113.9,114.8,115.7,116.6,117.5,118.4,119.3,120.2,121.1,122.0,
122.9,123.8,124.7,125.6,126.5,127.4,127.3,129.2,130.1,131.0,131.9,132.8,133.7,
134.6,135.5,136.4,137.3,138.2,139.1,140.0,140.9,141.8,142.7,143.6,144.5,145.4,
146.3,147.2,148.1,149.0,149.9,150.8,151.7,152.6,153.5,154.4,155.3,156.2,157.1,
158.0,158.9,159.8,160.7,161.6,162.5,163.4,164.3,165.2,166.1,167.0,167.9,168.8,
169.7,170.6,171.5,172.4,173.3,174.2,175.1,176.0,176.9,177.8,178.7,179.6,180.5,
181.4,182.3,183.2,184.1,185.0,185.9,186.8,187.7,188.6,189.5]

MODES = {0: "off", 1: "heat", 2: "cool", 3: "auto", 4: "em_heat"}
FANS  = {0: "auto", 1: "on", 2: "cycle"}
HOLDS = {0: "off", 1: "on", 2: "vacation"}
MODE_MAP = {"off":0, "heat":1, "cool":2, "auto":3, "em_heat":4}
FAN_MAP  = {"auto":0, "on":1, "cycle":2}

# ── Thermostat Configuration ────────────────────────────────────
THERMOSTATS = [
    {"address":1, "name":"entrance",         "location":"Entrance",             "port":"/dev/ttyUSB1"},
    {"address":2, "name":"conference_rm",    "location":"Conference Room",      "port":"/dev/ttyUSB1"},
    {"address":3, "name":"sanctuary_e",      "location":"Sanctuary E",          "port":"/dev/ttyUSB2"},
    {"address":4, "name":"sanctuary_w",      "location":"Sanctuary W",          "port":"/dev/ttyUSB2"},
    {"address":5, "name":"admin_office",     "location":"Admin Office",         "port":"/dev/ttyUSB2"},
    {"address":6, "name":"fellowship_rear",  "location":"Fellowship Hall Rear", "port":"/dev/ttyUSB1"},
    {"address":7, "name":"fellowship_front", "location":"Fellowship Hall Front","port":"/dev/ttyUSB2"},
]

TSTAT_BY_ADDR = {t["address"]: t for t in THERMOSTATS}
TSTAT_BY_NAME = {t["name"]: t for t in THERMOSTATS}

# ── Protocol Helpers ─────────────────────────────────────────────

def omni_to_f(idx: int) -> Optional[float]:
    if idx is None or idx < 0 or idx >= len(OMNIF):
        return None
    return round(OMNIF[idx], 1)

def f_to_omni(temp_f: float) -> int:
    best, best_diff = 0, 9999
    for i, v in enumerate(OMNIF):
        d = abs(v - temp_f)
        if d < best_diff:
            best_diff = d
            best = i
    return best

def checksum(data: bytes) -> int:
    return sum(data) & 0xFF

def make_group1_req(addr: int) -> bytes:
    pkt = bytes([addr, 0x02])
    return pkt + bytes([checksum(pkt)])

def make_humidity_req(addr: int) -> bytes:
    pkt = bytes([addr, 0x20, 0xA2, 0x01])
    return pkt + bytes([checksum(pkt)])

def make_write_cmd(addr: int, reg: int, value: int) -> bytes:
    pkt = bytes([addr, 0x21, reg, value])
    return pkt + bytes([checksum(pkt)])

# ── Frame Parser ─────────────────────────────────────────────────

def parse_frame(data: bytes):
    """Try to extract one frame from a buffer. Returns (frame_bytes, rest_bytes) or (None, data)."""
    if len(data) < 2:
        return None, data
    # Find start: address byte with bit 7 set
    start = -1
    for i, b in enumerate(data):
        if (b & 0x80) and (b & 0x7F) in TSTAT_BY_ADDR:
            start = i
            break
    if start == -1:
        return None, b""  # No valid start found — discard buffer
    data = data[start:]
    if len(data) < 2:
        return None, data
    addr = data[0] & 0x7F
    dlm  = data[1]
    msg_type = dlm & 0x0F
    data_len = (dlm >> 4) & 0x0F
    frame_len = 2 + data_len + 1  # hdr + payload + cs
    if len(data) < frame_len:
        return None, data  # Need more bytes
    frame = data[:frame_len]
    rest  = data[frame_len:]
    # Verify checksum
    if checksum(frame[:-1]) != frame[-1]:
        return None, b""  # Checksum fail — discard
    return frame, rest

def parse_group1(frame: bytes, tstat: dict, humidity: Optional[int] = None) -> dict:
    """Parse a Group 1 response (msg_type=3, data_len=6) into a state dict."""
    cool_sp  = omni_to_f(frame[2])
    heat_sp  = omni_to_f(frame[3])
    mode     = MODES.get(frame[4], str(frame[4]))
    fan      = FANS.get(frame[5], str(frame[5]))
    hold     = HOLDS.get(frame[6], str(frame[6]))
    cur_temp = omni_to_f(frame[7])
    return {
        "current_temp": cur_temp,
        "cool_setpoint": cool_sp,
        "heat_setpoint": heat_sp,
        "mode": mode,
        "fan": fan,
        "hold": hold,
        "humidity": humidity,
        "last_seen": time.time(),
    }

# ── Serial Manager ───────────────────────────────────────────────

class SerialManager:
    def __init__(self, port: str):
        self.port = port
        self._ser: Optional[serial.Serial] = None
        self._lock = threading.Lock()
        self._buffer = b""

    def open(self):
        self.close()
        try:
            self._ser = serial.Serial(
                port=self.port, baudrate=BAUD, bytesize=8,
                parity="N", stopbits=1, timeout=0.5
            )
            log.info(f"Serial {self.port} opened at {BAUD} baud")
            return True
        except Exception as e:
            log.error(f"Failed to open {self.port}: {e}")
            self._ser = None
            return False

    def close(self):
        with self._lock:
            if self._ser and self._ser.is_open:
                try:
                    self._ser.close()
                except:
                    pass
            self._ser = None
            self._buffer = b""

    def is_open(self) -> bool:
        return self._ser is not None and self._ser.is_open

    def send(self, data: bytes):
        if not self.is_open():
            return False
        try:
            with self._lock:
                self._ser.write(data)
            return True
        except Exception as e:
            log.warning(f"Serial write error on {self.port}: {e}")
            self.close()
            return False

    def read_frames(self):
        """Read available data and extract complete frames. Returns list of (frame_bytes, addr)."""
        if not self.is_open():
            return []
        try:
            with self._lock:
                raw = self._ser.read(256)
            if not raw:
                return []
            self._buffer += raw
        except Exception as e:
            log.warning(f"Serial read error on {self.port}: {e}")
            self.close()
            return []

        frames = []
        while True:
            frame, self._buffer = parse_frame(self._buffer)
            if frame is None:
                break
            addr = frame[0] & 0x7F
            frames.append((frame, addr))
        return frames

# ── MQTT Manager ─────────────────────────────────────────────────

class MQTTManager:
    def __init__(self):
        self.client = mqtt.Client(client_id=MQTT_CLIENT_ID)
        self.client.on_connect = self._on_connect
        self.client.on_message = self._on_message
        self._connected = False
        self._state = {}  # addr -> state dict

    def _on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            log.info("MQTT connected")
            self._connected = True
            # Subscribe to command topics
            for t in THERMOSTATS:
                prefix = f"church/thermostat/{t['name']}/set/#"
                client.subscribe(prefix)
            # Send discovery
            self.send_discovery()
        else:
            log.error(f"MQTT connect failed: rc={rc}")
            self._connected = False

    def _on_message(self, client, userdata, msg):
        """Handle incoming MQTT commands (temp/mode/fan changes from HA)."""
        parts = msg.topic.split("/")
        if len(parts) < 4:
            return
        tstat_name = parts[2]
        command = parts[-1]
        tstat = TSTAT_BY_NAME.get(tstat_name)
        if not tstat:
            log.warning(f"Unknown thermostat: {tstat_name}")
            return
        addr = tstat["address"]
        payload = msg.payload.decode().strip()

        if command in ("temp", "cool", "heat"):
            val = round(float(payload))
            if val < 50 or val > 99:
                log.warning(f"Bad setpoint {val} for {tstat_name}")
                return
            current_mode = self._state.get(addr, {}).get("mode", "cool")
            if command == "temp":
                reg = 0x3C if current_mode == "heat" else 0x3B
            else:
                reg = 0x3C if command == "heat" else 0x3B
            data_val = f_to_omni(float(val))
            pkt = make_write_cmd(addr, reg, data_val)
            _route_cmd(pkt, tstat["port"])
            log.info(f"CMD: {tstat_name} setpoint→{val}°F")

        elif command == "mode":
            data_val = MODE_MAP.get(payload)
            if data_val is None:
                log.warning(f"Bad mode '{payload}' for {tstat_name}")
                return
            pkt = make_write_cmd(addr, 0x3D, data_val)
            _route_cmd(pkt, tstat["port"])
            log.info(f"CMD: {tstat_name} mode→{payload}")

        elif command == "fan":
            data_val = FAN_MAP.get(payload)
            if data_val is None:
                log.warning(f"Bad fan '{payload}' for {tstat_name}")
                return
            pkt = make_write_cmd(addr, 0x3E, data_val)
            _route_cmd(pkt, tstat["port"])
            log.info(f"CMD: {tstat_name} fan→{payload}")

    def connect(self):
        try:
            self.client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
            self.client.loop_start()
        except Exception as e:
            log.error(f"MQTT connection error: {e}")

    def is_connected(self) -> bool:
        return self._connected

    def publish_state(self, tstat: dict, state: dict):
        self._state[tstat["address"]] = state
        topic = f"church/thermostat/{tstat['name']}/state"
        payload = json.dumps({
            "current_temp": state["current_temp"],
            "cool_setpoint": state["cool_setpoint"],
            "heat_setpoint": state["heat_setpoint"],
            "mode": state["mode"],
            "fan": state["fan"],
            "humidity": state["humidity"],
        })
        self.client.publish(topic, payload, qos=0, retain=True)

    def send_discovery(self):
        for t in THERMOSTATS:
            topic = f"homeassistant/climate/church_{t['name']}/config"
            dp = {
                "name": t["location"],
                "unique_id": f"church_hvac_tstat_{t['address']}_v4",
                "device_class": "temperature",
                "current_temperature_topic": f"church/thermostat/{t['name']}/state",
                "current_temperature_template": "{{ value_json.current_temp }}",
                "temperature_state_topic": f"church/thermostat/{t['name']}/state",
                "temperature_state_template": "{% if value_json.mode == 'cool' %}{{ value_json.cool_setpoint }}{% else %}{{ value_json.heat_setpoint }}{% endif %}",
                "temperature_command_topic": f"church/thermostat/{t['name']}/set/temp",
                "mode_state_topic": f"church/thermostat/{t['name']}/state",
                "mode_state_template": "{{ value_json.mode }}",
                "mode_command_topic": f"church/thermostat/{t['name']}/set/mode",
                "modes": ["off","heat","cool","auto"],
                "fan_mode_state_topic": f"church/thermostat/{t['name']}/state",
                "fan_mode_state_template": "{{ value_json.fan }}",
                "fan_mode_command_topic": f"church/thermostat/{t['name']}/set/fan",
                "fan_modes": ["auto","on","cycle"],
                "current_humidity_topic": f"church/thermostat/{t['name']}/state",
                "current_humidity_template": "{{ value_json.humidity }}",
                "temperature_unit": "F",
                "min_temp": 50, "max_temp": 99, "temp_step": 1,
                "device": {
                    "identifiers": [f"church_hvac_system_{t['address']}"],
                    "name": t["location"],
                    "model": "RC-2000 Omnistat2",
                    "manufacturer": "HAI",
                    "via_device": "church_hvac_bridge",
                }
            }
            self.client.publish(topic, json.dumps(dp), qos=1, retain=True)
            log.info(f"Discovery sent for {t['location']}")


# ── Global command router (MQTT → serial) ────────────────────────
_serial_by_port: dict[str, SerialManager] = {}
def _route_cmd(pkt: bytes, port: str):
    mgr = _serial_by_port.get(port)
    if mgr:
        mgr.send(pkt)

# ── Poller ───────────────────────────────────────────────────────

def build_poller(config_ts: list):
    """Build a poll schedule: list of (delay_seconds, request_bytes, port)."""
    schedule = []
    gap = 0.5
    for t in config_ts:
        schedule.append((gap, make_group1_req(t["address"]), t["port"]))
        gap += 0.5
        schedule.append((gap, make_humidity_req(t["address"]), t["port"]))
        gap += 0.5
    return schedule

def main():
    global _serial_by_port

    log.info("=" * 50)
    log.info("HVAC Bridge starting")
    log.info(f"MQTT: {MQTT_HOST}:{MQTT_PORT}")
    log.info(f"Stats: {len(THERMOSTATS)} thermostats on {len(SERIAL_PORTS)} serial ports")
    log.info("=" * 50)

    # Open serial ports
    for port_name in SERIAL_PORTS:
        sm = SerialManager(port_name)
        if sm.open():
            SERIAL_PORTS[port_name] = True
        _serial_by_port[port_name] = sm

    # MQTT
    mqtt_mgr = MQTTManager()
    mqtt_mgr.connect()

    # Poll schedule
    poll_schedule = build_poller(THERMOSTATS)
    cycle_duration = poll_schedule[-1][0] + 0.5 if poll_schedule else 10.0
    poll_cycle_start = time.time()

    # State tracking
    humidity_store: dict[int, int] = {}
    last_data_time = time.time()

    # Main loop
    last_discovery_time = 0
    while True:
        now = time.time()

        # ── Poll cycle ──
        elapsed = now - poll_cycle_start
        if elapsed >= cycle_duration:
            poll_cycle_start = now
            elapsed = 0

        for delay, request_bytes, port in poll_schedule:
            if abs(elapsed - delay) < 0.25:
                sm = _serial_by_port.get(port)
                if sm:
                    sm.send(request_bytes)

        # ── Read frames from each port ──
        for port_name, sm in _serial_by_port.items():
            frames = sm.read_frames()
            for frame, addr in frames:
                tstat = TSTAT_BY_ADDR.get(addr)
                if not tstat:
                    continue
                dlm = frame[1]
                msg_type = dlm & 0x0F
                data_len = (dlm >> 4) & 0x0F

                if msg_type == 2:  # Register read response (humidity)
                    reg = frame[2]
                    if reg == 0xA2:
                        humidity_store[addr] = frame[3]
                        log.debug(f"Humidity {tstat['name']}: {frame[3]}%")

                elif msg_type == 3 and data_len == 6:  # Group 1 status
                    humidity = humidity_store.get(addr)
                    state = parse_group1(frame, tstat, humidity)
                    mqtt_mgr.publish_state(tstat, state)
                    last_data_time = now
                    log.info(f"{tstat['location']}: {state['current_temp']}°F [{state['mode']}] SP={state['cool_setpoint']}°C H={humidity}%")

                elif msg_type == 0:
                    log.debug(f"ACK from {tstat['name']}")
                elif msg_type == 1:
                    log.debug(f"NAK from {tstat['name']}")

        # ── Re-send discovery every hour ──
        if mqtt_mgr.is_connected() and now - last_discovery_time > 3600:
            mqtt_mgr.send_discovery()
            last_discovery_time = now

        # ── Watchdog: reconnect stale serial ──
        if now - last_data_time > WATCHDOG_STALE_SEC:
            log.warning(f"Watchdog: no data for {int(now - last_data_time)}s — resetting serial ports")
            for port_name, sm in _serial_by_port.items():
                sm.close()
                time.sleep(1)
                sm.open()
            last_data_time = now

        # ── Watchdog: reconnect stale MQTT ──
        if not mqtt_mgr.is_connected():
            log.warning("MQTT disconnected — reconnecting")
            mqtt_mgr.connect()

        time.sleep(0.1)

if __name__ == "__main__":
    main()
