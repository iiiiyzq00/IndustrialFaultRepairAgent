"""
Industrial self-healing actions: PLC parameter rollback, CNC adjustments.

HIGH RISK operations — always require HITL dual approval.
In development mode, these are simulated (no real PLC/CNC connected).
"""

from __future__ import annotations

import os
import logging
from typing import Any, Dict

logger = logging.getLogger(__name__)
DRY_RUN = os.getenv("DRY_RUN", "false").lower() == "true"


def plc_parameter_rollback(params: Dict[str, Any], dry_run: bool = False) -> Dict[str, Any]:
    """
    HIGH RISK — requires HITL dual approval.
    Rollback a PLC parameter to a previous value.

    In production, this would use Modbus TCP or OPC UA to communicate
    with the PLC. For now, it's simulated.

    Expected params:
      - plc_id: PLC identifier (e.g., "PLC-L1-01")
      - parameter: parameter name (e.g., "scan_cycle_ms", "temperature_setpoint")
      - old_value: value to rollback to
      - current_value: current (faulty) value
      - protocol: "modbus" | "opcua" (default: modbus)
    """
    plc_id = params.get("plc_id", params.get("target", ""))
    parameter = params.get("parameter", "")
    old_value = params.get("old_value", "")
    current_value = params.get("current_value", "")
    protocol = params.get("protocol", "modbus")

    if dry_run or DRY_RUN:
        logger.info("[DRY RUN] plc_parameter_rollback %s.%s: %s → %s",
                    plc_id, parameter, current_value, old_value)
        return {"success": True, "dry_run": True, "plc_id": plc_id, "parameter": parameter}

    # ── Production implementation ──
    # if protocol == "modbus":
    #     client = ModbusTcpClient(plc_ip)
    #     client.write_register(register_address, old_value)
    # elif protocol == "opcua":
    #     client = Client(plc_opcua_url)
    #     node = client.get_node(f"ns=2;s={parameter}")
    #     node.set_value(old_value)

    # MVP: simulated execution with safety confirmation
    logger.warning("PLC ROLLBACK (SIMULATED): %s.%s: %s → %s [protocol=%s]",
                   plc_id, parameter, current_value, old_value, protocol)

    return {
        "success": True,
        "simulated": True,
        "plc_id": plc_id,
        "parameter": parameter,
        "old_value": old_value,
        "current_value": current_value,
        "protocol": protocol,
        "note": "In production, this would execute real Modbus/OPC UA write. HITL dual approval required."
    }


def cnc_parameter_adjust(params: Dict[str, Any], dry_run: bool = False) -> Dict[str, Any]:
    """
    Adjust CNC machine parameter (feed rate, spindle speed limit).

    HIGH RISK — physical equipment damage possible if misconfigured.
    """
    cnc_id = params.get("cnc_id", params.get("target", ""))
    parameter = params.get("parameter", "")
    value = params.get("value", "")

    if dry_run or DRY_RUN:
        return {"success": True, "dry_run": True, "cnc_id": cnc_id}

    logger.warning("CNC ADJUST (SIMULATED): %s.%s = %s", cnc_id, parameter, value)
    return {"success": True, "simulated": True, "cnc_id": cnc_id, "parameter": parameter, "value": value}


def emergency_stop(params: Dict[str, Any], dry_run: bool = False) -> Dict[str, Any]:
    """
    CRITICAL — emergency stop of a production line segment.
    Only executed with explicit human confirmation.
    """
    zone = params.get("zone", params.get("target", ""))
    reason = params.get("reason", "manual_trigger")

    if dry_run or DRY_RUN:
        return {"success": True, "dry_run": True, "zone": zone}

    logger.critical("EMERGENCY STOP (SIMULATED): zone=%s reason=%s", zone, reason)
    return {"success": True, "simulated": True, "zone": zone, "reason": reason,
            "warning": "Emergency stop executed. All equipment in zone halted."}
