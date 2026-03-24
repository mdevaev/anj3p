#!/usr/bin/env python3


import asyncio
import multiprocessing
import operator
import re
import time
import logging

import aiohttp.web

import pymodbus
from pymodbus.client.serial import ModbusSerialClient as ModbusClient
from pymodbus.pdu.register_message import ReadHoldingRegistersResponse


# =====
class _InvalidModbusResponseError(Exception):
    pass


def _uint16(regs: list[int], offset: int) -> int:
    return regs[offset]


def _sint16(regs: list[int], offset: int) -> int:
    raw = regs[offset]
    return ((raw - 65536) if raw >= 32768 else raw)


def _uint32(regs: list[int], offset: int) -> int:
    return ((regs[offset] << 16) + regs[offset + 1])


_WORKING_CODES = {
    0: "Power On",
    1: "Standby",
    2: "On-Grid",
    3: "Off-Grid",
    4: "Bypass",
    5: "Charging",
    6: "Fault",
}

_WARNING_BITS = {
    0:  "Zero crossing loss of grid power",
    1:  "Grid waveform abnormal",
    2:  "Grid over voltage",
    3:  "Grid low voltage",
    4:  "Grid over frequency",
    5:  "Grid low frequency",
    6:  "PV low voltage",
    7:  "Over temperature",
    8:  "Battery low voltage",
    9:  "Battery is not connected",
    10: "Overload",
    11: "Battery Eq charging",
    12: "Battery is discharged at a low voltage and it has not been charged back to the recovery point",
    13: "Output power derating",
    14: "Fan blocked",
    15: "PV energy is too low to be used",
    16: "Parallel communication interrupted",
    17: "Output mode of Single and Parallel systems is inconsistent",
    18: "Battery voltage difference of parallel system is too large",
}

_FAULT_BITS = {
    # 1:  "(?) Inverter over temp",
    2:  "DCDC over temp",
    3:  "Battery over voltage",
    # 4:  "(?) PV over temp",       
    5:  "Output short circuited",
    6:  "Inverter over voltage",
    7:  "Output over load",   
    8:  "Bus over voltage",
    9:  "Bus soft start timeouted",
    10: "PV over current",
    11: "PV over voltage",
    12: "Battery over current",
    13: "Inverter over current",
    14: "Bus low voltage",
    # 15: "(?) Inverter failed",
    16: "Inverter DC component is too high",
    18: "The zero bias of output current is too large",
    19: "The zero bias of inverter current is too large",
    20: "The zero bias of battery current is too large",
    21: "The zero bias of PV current is too large",
    22: "Inverter low voltage",
    23: "Inverter negative power protection",
    24: "The host in the parallel system is lost",
    25: "Synchronization signal abnormal in the parallel system",
    27: "Para versions are incompatible",
}

_REGISTERS = list(sorted({
    # 1xx
    100: (_uint32, 1,   "unit_fault_mask"),
    108: (_uint32, 1,   "unit_warning_mask"),
    # 2xx
    201: (_uint16, 1,   "unit_working_mask"),
    202: (_sint16, 10,  "grid_voltage"),
    203: (_sint16, 100, "grid_freq"),
    204: (_sint16, 1,   "grid_power"),
    205: (_sint16, 10,  "inverter_voltage"),
    206: (_sint16, 10,  "inverter_current"),
    207: (_sint16, 100, "inverter_freq"),
    208: (_sint16, 1,   "inverter_power"),
    209: (_sint16, 1,   "inverter_charging_power"),
    210: (_sint16, 10,  "output_voltage"),
    211: (_sint16, 10,  "output_current"),
    212: (_sint16, 100, "output_freq"),
    213: (_sint16, 1,   "output_active_power"),
    214: (_sint16, 1,   "output_apparent_va"),
    215: (_sint16, 10,  "bat_voltage"),
    216: (_sint16, 1,   "bat_current"),  # Looks the same as 232
    217: (_sint16, 1,   "bat_power"),
    219: (_sint16, 10,  "pv_voltage"),
    220: (_sint16, 10,  "pv_current"),
    223: (_sint16, 1,   "pv_power"),
    224: (_sint16, 1,   "pv_charging_power"),
    225: (_sint16, 1,   "unit_load_percent"),
    226: (_sint16, 1,   "unit_dcdc_temp"),
    227: (_sint16, 1,   "unit_inverter_temp"),
    229: (_uint16, 1,   "bat_percent"),  # Uint? Only this, really?
    # 232: (_sint16, 1,   "bat_current2"), # Net Battery Current (Solar + Grid - Load)
    233: (_sint16, 10,  "inverter_charging_current"),
    234: (_sint16, 10,  "pv_charging_current"),
    # 3xx
    300: (_uint16, 1,   "unit_output_mode"),
}.items(), key=operator.itemgetter(0)))


def _decode_uint32_msgs(mask: int, known: dict[int, str]) -> str:
    if mask == 0:
        return ""
    msgs: list[str] = []
    for bit in range(32):
        if (mask >> bit) & 1:
            msg = known.get(bit, "Unknown")
            msg += f" [{bit}]"
            msgs.append(msg)
    return "; ".join(msgs)


def _split_sign(data: dict, key: str, neg: str, pos: str) -> None:
    neg = f"{key}_{neg}"
    pos = f"{key}_{pos}"
    value = data[key]
    data[neg] = 0
    data[pos] = 0
    data[neg if value < 0 else pos] = abs(value)
    # del data[key]


def _read(conn: ModbusClient, begin: int, end: int) -> list[int]:
    resp = conn.read_holding_registers(begin, count=(end - begin))
    if not isinstance(resp, ReadHoldingRegistersResponse):
        raise _InvalidModbusResponseError(f"{type(resp).__name__}: {resp}")
    return resp.registers


def _read_data(conn: ModbusClient) -> dict:
    data: [str, int] = {}

    for (begin, end) in [(100, 110), (200, 240), (300, 302)]:
        regs = _read(conn, begin, end)
        for (reg, (func, div, name)) in _REGISTERS:
            if begin <= reg <= end:
                value = func(regs, reg - begin)
                if div != 1:
                    value /= div
                data[name] = value

    _split_sign(data, "bat_power", "dis", "chr")
    _split_sign(data, "bat_current", "dis", "chr")
    # _split_sign(data, "bat_current2", "dis", "chr")
    _split_sign(data, "inverter_power", "cons", "prod")

    data["unit_warning_msgs"] = _decode_uint32_msgs(data["unit_warning_mask"], _WARNING_BITS)

    data["unit_fault_msgs"] = _decode_uint32_msgs(data["unit_fault_mask"], _FAULT_BITS)

    data["unit_working_msgs"] = _WORKING_CODES.get(data["unit_working_mask"], "Unknown")
    data["unit_working_msgs"] += f" [{data['unit_working_mask']}]"

    mode = data.pop("unit_output_mode")
    assert mode in [2, 3, 4], mode  # 3-phase only
    data["__phase"] = f"L{mode - 1}"
    data["__ts"] = time.monotonic()
    return data


# =====
multiprocessing.set_start_method("fork")

_mgr = multiprocessing.Manager()
_stats = {f"L{phase}": _mgr.dict() for phase in [1, 2, 3]}
_procs: list[multiprocessing.Process] = []


def _worker(path: str) -> None:
    name = re.sub(r"[^a-zA-Z0-9_]", "_", path)
    logger = logging.getLogger(f"worker-{name}")
    try:
        with ModbusClient(
            port=path,
            baudrate=9600,
            stopbits=1,
            bytesize=8,
            parity="N",
            timeout=1,
        ) as conn:

            while True:
                data = _read_data(conn)
                phase = data.pop("__phase")
                _stats[phase].update(data)
    except Exception as ex:
        logger.exception("Can't query inverter: %s", path)
        time.sleep(1)


def _get_actual_stats() -> dict:
    now_ts = time.monotonic()
    stats: dict[str, dict] = {}
    for (phase, data) in _stats.items():
        data = dict(data)
        if data and (time.monotonic() - data.pop("__ts") <= 1):
            stats[phase] = dict(data)
    return stats


async def _stats_handler(req: aiohttp.web.Request) -> aiohttp.web.Response:
    for proc in _procs:
        if not proc.is_alive():
            raise SystemExit("Something is dead")
    stats = _get_actual_stats()
    if req.query.get("format", "json") == "prometheus":
        rows: list[str] = []
        for (phase, kvs) in sorted(stats.items(), key=operator.itemgetter(0)):
            for (key, value) in sorted(kvs.items(), key=operator.itemgetter(0)):
                if not isinstance(value, str):
                    path = f"anj3p_{phase}_{key}"
                    rows.extend([f"# TYPE {path} gauge", f"{path} {value}", ""])
        return aiohttp.web.Response(text="\n".join(rows))
    else:
        return aiohttp.web.json_response({"phases": stats})


def main() -> None:
    # logging.basicConfig(level=logging.DEBUG)
    # pymodbus.pymodbus_apply_logging_config(logging.CRITICAL)

    global _procs
    _procs = [
        multiprocessing.Process(target=_worker, args=(path,), daemon=True)
        for path in ["/dev/ttyACM0", "/dev/ttyACM1", "/dev/ttyACM2"]
    ]
    for proc in _procs:
        proc.start()

    app = aiohttp.web.Application()
    app.add_routes([aiohttp.web.get("/stats", _stats_handler)])
    aiohttp.web.run_app(app, host="0.0.0.0", port=8080)


if __name__ == "__main__":
    main()
