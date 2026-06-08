# Premio RCO-3000 Server Manager

Tools for managing Premio RCO-3000 series servers via Intel AMT, including PXE boot re-imaging.

## Components

### 1. PXE Boot Runbook (`docs/PXE_BOOT_RUNBOOK.md`)
Step-by-step documentation for manually performing PXE boot re-imaging via MeshCommander, with annotated screenshots from Premio's Calvin Chen.

### 2. AMT Automation Script (`scripts/amt_controller.py`)
Python CLI tool for programmatic Intel AMT control:

```bash
# Install dependencies
pip install requests

# Check power state
python scripts/amt_controller.py --host 192.168.1.100 --user admin --password <pass> status

# Trigger PXE boot + reboot in one step
python scripts/amt_controller.py --host 192.168.1.100 --user admin --password <pass> pxe-reboot

# Set PXE as next boot device (without rebooting)
python scripts/amt_controller.py --host 192.168.1.100 --user admin --password <pass> pxe-boot

# Power management
python scripts/amt_controller.py --host 192.168.1.100 --user admin --password <pass> power cycle-graceful

# Hardware / BIOS info
python scripts/amt_controller.py --host 192.168.1.100 --user admin --password <pass> hw-info
python scripts/amt_controller.py --host 192.168.1.100 --user admin --password <pass> bios-info
```

### 3. Web Management Interface (`web/`)
FastAPI web application with a dashboard for operators to manage multiple RCO-3000 units:

```bash
# Install dependencies
pip install -r requirements.txt

# Run the server
cd web
uvicorn app:app --host 0.0.0.0 --port 8080
# or
python app.py
```

Then open http://localhost:8080 in your browser.

**Features:**
- Register and manage multiple RCO-3000 devices
- One-click PXE boot re-imaging
- Power state monitoring (on/off/sleep)
- Power management (on, off, reboot, power cycle)
- Hardware and BIOS information display
- Confirmation dialogs for destructive actions

## Architecture

```
premio_rco3000_manager/
├── README.md
├── requirements.txt
├── docs/
│   ├── PXE_BOOT_RUNBOOK.md      # Manual procedure with screenshots
│   └── images/                    # Screenshots from MeshCommander
├── scripts/
│   ├── __init__.py
│   └── amt_controller.py          # CLI tool & AMT WSMAN client library
└── web/
    ├── app.py                     # FastAPI web server
    ├── devices.json               # Device registry (auto-created)
    ├── static/
    └── templates/
        └── index.html             # Dashboard UI
```

## How It Works

The tools use **Intel AMT WSMAN APIs** to communicate with the RCO-3000's management engine over the network. This allows:

1. **Remote power control** — Power on/off, reboot, power cycle without physical access
2. **Boot configuration** — Set PXE as the next boot device via `CIM_BootConfigSetting` and `AMT_BootSettingData` WSMAN resources
3. **System monitoring** — Query power state, hardware info, BIOS version

The PXE boot flow:
1. The script configures `AMT_BootSettingData` for network boot
2. Sets `CIM_BootSourceSetting` to "Force PXE Boot"
3. Activates the boot config via `CIM_BootService/SetBootConfigRole`
4. Triggers a graceful power cycle
5. On reboot, the unit PXE boots from Premio's server → Ubuntu 24.04 loads → automated testing runs

## Prerequisites

- Intel AMT must be configured and enabled on the RCO-3000 units
- Network connectivity from the management machine to the AMT interface (port 16993 for TLS, 16992 for non-TLS)
- The unit's 2nd LAN port must be connected to the PXE boot server network
- AMT credentials (username/password)

## Security Notes

- AMT credentials are stored locally in `devices.json` — protect this file appropriately
- Use TLS (port 16993) for encrypted communication when possible
- The web interface is intended for internal lab/operations use — add authentication before exposing to wider networks
- Consider using environment variables or a secrets manager for credentials in production
