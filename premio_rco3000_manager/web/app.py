"""
Premio RCO-3000 Web Management Interface

FastAPI-based web UI for managing Premio RCO-3000 servers via Intel AMT.
Provides a simple operator interface for PXE boot re-imaging and system monitoring.

Usage:
    uvicorn app:app --host 0.0.0.0 --port 8080
    # or
    python app.py
"""

import json
import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel

# Add scripts directory to path for amt_controller import
sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
from amt_controller import AMTClient, AMTConfig, PowerState

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Premio RCO-3000 Server Manager",
    description="Web interface for managing Premio RCO-3000 servers via Intel AMT",
    version="1.0.0",
)

# Static files and templates
BASE_DIR = Path(__file__).parent
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))

# In-memory device registry (in production, use a database)
DEVICES_FILE = BASE_DIR / "devices.json"


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------

class DeviceCreate(BaseModel):
    name: str
    host: str
    username: str = "admin"
    password: str
    port: int = 16993
    use_tls: bool = True
    serial_number: Optional[str] = None
    location: Optional[str] = None


class DeviceUpdate(BaseModel):
    name: Optional[str] = None
    host: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None
    port: Optional[int] = None
    use_tls: Optional[bool] = None
    serial_number: Optional[str] = None
    location: Optional[str] = None


class PowerAction(BaseModel):
    action: str  # on, off, off-graceful, cycle, cycle-graceful, reset


# ---------------------------------------------------------------------------
# Device Registry
# ---------------------------------------------------------------------------

def load_devices() -> dict:
    """Load devices from JSON file."""
    if DEVICES_FILE.exists():
        return json.loads(DEVICES_FILE.read_text())
    return {}


def save_devices(devices: dict):
    """Save devices to JSON file."""
    DEVICES_FILE.write_text(json.dumps(devices, indent=2))


def get_amt_client(device_id: str) -> AMTClient:
    """Get an AMT client for a registered device."""
    devices = load_devices()
    if device_id not in devices:
        raise HTTPException(status_code=404, detail=f"Device '{device_id}' not found")

    device = devices[device_id]
    config = AMTConfig(
        host=device["host"],
        username=device["username"],
        password=device["password"],
        port=device.get("port", 16993),
        use_tls=device.get("use_tls", True),
    )
    return AMTClient(config)


# ---------------------------------------------------------------------------
# Web UI Routes
# ---------------------------------------------------------------------------

@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    """Main dashboard page."""
    return templates.TemplateResponse("index.html", {"request": request})


# ---------------------------------------------------------------------------
# API Routes - Device Management
# ---------------------------------------------------------------------------

@app.get("/api/devices")
async def list_devices():
    """List all registered devices."""
    devices = load_devices()
    # Strip passwords from response
    safe_devices = {}
    for did, d in devices.items():
        safe_devices[did] = {k: v for k, v in d.items() if k != "password"}
        safe_devices[did]["has_password"] = bool(d.get("password"))
    return safe_devices


@app.post("/api/devices")
async def add_device(device: DeviceCreate):
    """Register a new device."""
    devices = load_devices()
    device_id = device.host.replace(".", "-")
    if device_id in devices:
        raise HTTPException(status_code=409, detail="Device already registered")

    devices[device_id] = {
        "name": device.name,
        "host": device.host,
        "username": device.username,
        "password": device.password,
        "port": device.port,
        "use_tls": device.use_tls,
        "serial_number": device.serial_number,
        "location": device.location,
        "added_at": datetime.utcnow().isoformat(),
    }
    save_devices(devices)
    return {"device_id": device_id, "message": "Device registered successfully"}


@app.put("/api/devices/{device_id}")
async def update_device(device_id: str, update: DeviceUpdate):
    """Update a registered device."""
    devices = load_devices()
    if device_id not in devices:
        raise HTTPException(status_code=404, detail="Device not found")

    for field, value in update.model_dump(exclude_none=True).items():
        devices[device_id][field] = value

    save_devices(devices)
    return {"message": "Device updated"}


@app.delete("/api/devices/{device_id}")
async def remove_device(device_id: str):
    """Remove a registered device."""
    devices = load_devices()
    if device_id not in devices:
        raise HTTPException(status_code=404, detail="Device not found")

    del devices[device_id]
    save_devices(devices)
    return {"message": "Device removed"}


# ---------------------------------------------------------------------------
# API Routes - AMT Operations
# ---------------------------------------------------------------------------

@app.get("/api/devices/{device_id}/status")
async def device_status(device_id: str):
    """Get the current power state of a device."""
    try:
        client = get_amt_client(device_id)
        power = client.get_power_state()
        return {"device_id": device_id, "power": power}
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Failed to get status for %s", device_id)
        return JSONResponse(
            status_code=502,
            content={"error": str(e), "device_id": device_id},
        )


@app.get("/api/devices/{device_id}/hw-info")
async def device_hw_info(device_id: str):
    """Get hardware information for a device."""
    try:
        client = get_amt_client(device_id)
        hw = client.get_hardware_info()
        bios = client.get_bios_info()
        settings = client.get_general_settings()
        return {
            "device_id": device_id,
            "hardware": hw,
            "bios": bios,
            "settings": settings,
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Failed to get hw-info for %s", device_id)
        return JSONResponse(
            status_code=502,
            content={"error": str(e), "device_id": device_id},
        )


@app.post("/api/devices/{device_id}/pxe-boot")
async def trigger_pxe_boot(device_id: str):
    """Set PXE as the next boot device (does not reboot)."""
    try:
        client = get_amt_client(device_id)
        result = client.set_pxe_boot()
        return {"device_id": device_id, **result}
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Failed to set PXE boot for %s", device_id)
        return JSONResponse(
            status_code=502,
            content={"error": str(e), "device_id": device_id},
        )


@app.post("/api/devices/{device_id}/pxe-reboot")
async def trigger_pxe_reboot(device_id: str):
    """Configure PXE boot and immediately reboot the device."""
    try:
        client = get_amt_client(device_id)
        result = client.pxe_reboot()
        return {"device_id": device_id, **result}
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Failed to PXE reboot %s", device_id)
        return JSONResponse(
            status_code=502,
            content={"error": str(e), "device_id": device_id},
        )


@app.post("/api/devices/{device_id}/power")
async def power_action(device_id: str, action: PowerAction):
    """Change the power state of a device."""
    action_map = {
        "on": PowerState.ON,
        "off": PowerState.OFF_HARD,
        "off-graceful": PowerState.OFF_SOFT_GRACEFUL,
        "cycle": PowerState.POWER_CYCLE,
        "cycle-graceful": PowerState.POWER_CYCLE_SOFT_GRACEFUL,
        "reset": PowerState.MASTER_BUS_RESET,
    }
    if action.action not in action_map:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid action. Choose from: {list(action_map.keys())}",
        )

    try:
        client = get_amt_client(device_id)
        state = action_map[action.action]
        result = client.change_power_state(state)
        return {"device_id": device_id, "action": action.action, **result}
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Power action failed for %s", device_id)
        return JSONResponse(
            status_code=502,
            content={"error": str(e), "device_id": device_id},
        )


# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8080))
    uvicorn.run(app, host="0.0.0.0", port=port)
