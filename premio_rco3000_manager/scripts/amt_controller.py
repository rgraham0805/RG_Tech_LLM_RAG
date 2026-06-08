#!/usr/bin/env python3
"""
Intel AMT Controller for Premio RCO-3000 Series Servers

Provides programmatic control of Premio RCO-3000 servers via Intel AMT WSMAN APIs.
Supports power management, PXE boot configuration, and system status queries.

Usage:
    # Check power state
    python amt_controller.py --host 192.168.1.100 --user admin --password <pass> status

    # Force PXE boot on next reboot
    python amt_controller.py --host 192.168.1.100 --user admin --password <pass> pxe-boot

    # Reboot with PXE boot
    python amt_controller.py --host 192.168.1.100 --user admin --password <pass> pxe-reboot

    # Power cycle
    python amt_controller.py --host 192.168.1.100 --user admin --password <pass> power-cycle

    # Get hardware info
    python amt_controller.py --host 192.168.1.100 --user admin --password <pass> hw-info
"""

import argparse
import enum
import logging
import sys
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from typing import Optional

import requests
from requests.auth import HTTPDigestAuth

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

AMT_PORT_TLS = 16993
AMT_PORT_NO_TLS = 16992

# WSMAN namespaces
NS = {
    "s": "http://www.w3.org/2003/05/soap-envelope",
    "wsa": "http://schemas.xmlsoap.org/ws/2004/08/addressing",
    "wsman": "http://schemas.dmtf.org/wbem/wsman/1/wsman.xsd",
    "cim_power": "http://schemas.dmtf.org/wbem/wscim/1/cim-schema/2/CIM_AssociatedPowerManagementService",
    "cim_ps": "http://schemas.dmtf.org/wbem/wscim/1/cim-schema/2/CIM_PowerManagementService",
    "amt_boot": "http://intel.com/wbem/wscim/1/amt-schema/1/AMT_BootSettingData",
    "cim_boot": "http://schemas.dmtf.org/wbem/wscim/1/cim-schema/2/CIM_BootConfigSetting",
    "cim_bios": "http://schemas.dmtf.org/wbem/wscim/1/cim-schema/2/CIM_BIOSElement",
    "cim_cs": "http://schemas.dmtf.org/wbem/wscim/1/cim-schema/2/CIM_ComputerSystem",
    "cim_chassis": "http://schemas.dmtf.org/wbem/wscim/1/cim-schema/2/CIM_Chassis",
    "amt_gs": "http://intel.com/wbem/wscim/1/amt-schema/1/AMT_GeneralSettings",
    "cim_sp": "http://schemas.dmtf.org/wbem/wscim/1/cim-schema/2/CIM_SoftwareIdentity",
}

# CIM Power State values
class PowerState(enum.IntEnum):
    ON = 2
    SLEEP_LIGHT = 3
    SLEEP_DEEP = 4
    POWER_CYCLE = 5
    OFF_HARD = 6
    HIBERNATE = 7
    OFF_SOFT = 8
    POWER_CYCLE_SOFT = 9
    MASTER_BUS_RESET = 10
    NMI = 11
    OFF_SOFT_GRACEFUL = 12
    OFF_HARD_GRACEFUL = 13
    MASTER_BUS_RESET_GRACEFUL = 14
    POWER_CYCLE_SOFT_GRACEFUL = 15
    POWER_CYCLE_HARD_GRACEFUL = 16


# Boot source values
BOOT_SOURCE_PXE = (
    "http://schemas.dmtf.org/wbem/wscim/1/cim-schema/2/"
    "CIM_BootSourceSetting?InstanceID="
    '"Intel(r) AMT: Force PXE Boot"'
)
BOOT_SOURCE_HDD = (
    "http://schemas.dmtf.org/wbem/wscim/1/cim-schema/2/"
    "CIM_BootSourceSetting?InstanceID="
    '"Intel(r) AMT: Force Hard-drive Boot"'
)
BOOT_SOURCE_CD = (
    "http://schemas.dmtf.org/wbem/wscim/1/cim-schema/2/"
    "CIM_BootSourceSetting?InstanceID="
    '"Intel(r) AMT: Force CD/DVD Boot"'
)


# ---------------------------------------------------------------------------
# WSMAN Envelope Builder
# ---------------------------------------------------------------------------

def _build_envelope(resource_uri: str, action: str, body_xml: str = "",
                    selector: Optional[dict] = None) -> str:
    """Build a WSMAN SOAP envelope."""
    selector_xml = ""
    if selector:
        selector_set = "".join(
            f'<wsman:Selector Name="{k}">{v}</wsman:Selector>'
            for k, v in selector.items()
        )
        selector_xml = f"<wsman:SelectorSet>{selector_set}</wsman:SelectorSet>"

    return f"""<?xml version="1.0" encoding="UTF-8"?>
<s:Envelope
    xmlns:s="http://www.w3.org/2003/05/soap-envelope"
    xmlns:wsa="http://schemas.xmlsoap.org/ws/2004/08/addressing"
    xmlns:wsman="http://schemas.dmtf.org/wbem/wsman/1/wsman.xsd">
  <s:Header>
    <wsa:Action>{action}</wsa:Action>
    <wsa:To>http://schemas.xmlsoap.org/ws/2004/08/addressing/role/anonymous</wsa:To>
    <wsman:ResourceURI>{resource_uri}</wsman:ResourceURI>
    {selector_xml}
  </s:Header>
  <s:Body>
    {body_xml}
  </s:Body>
</s:Envelope>"""


# ---------------------------------------------------------------------------
# AMT Client
# ---------------------------------------------------------------------------

@dataclass
class AMTConfig:
    """Configuration for connecting to an Intel AMT device."""
    host: str
    username: str
    password: str
    port: int = AMT_PORT_TLS
    use_tls: bool = True
    verify_ssl: bool = False


class AMTClient:
    """Intel AMT WSMAN client for Premio RCO-3000 management."""

    def __init__(self, config: AMTConfig):
        self.config = config
        scheme = "https" if config.use_tls else "http"
        self.url = f"{scheme}://{config.host}:{config.port}/wsman"
        self.auth = HTTPDigestAuth(config.username, config.password)
        self.session = requests.Session()
        self.session.auth = self.auth
        self.session.verify = config.verify_ssl
        self.session.headers.update({
            "Content-Type": "application/soap+xml; charset=UTF-8",
        })

    def _send(self, envelope: str) -> ET.Element:
        """Send a WSMAN request and parse the response."""
        resp = self.session.post(self.url, data=envelope, timeout=30)
        resp.raise_for_status()
        return ET.fromstring(resp.text)

    # -- Power Management --

    def get_power_state(self) -> dict:
        """Query the current power state of the system."""
        resource = (
            "http://schemas.dmtf.org/wbem/wscim/1/cim-schema/2/"
            "CIM_AssociatedPowerManagementService"
        )
        action = "http://schemas.xmlsoap.org/ws/2004/09/transfer/Get"
        envelope = _build_envelope(resource, action)
        root = self._send(envelope)

        body = root.find(".//s:Body", NS)
        power_state_el = body.find(".//{%s}PowerState" % NS["cim_power"])
        state_num = int(power_state_el.text) if power_state_el is not None else -1

        state_map = {
            2: "On", 3: "Sleep (Light)", 4: "Sleep (Deep)",
            6: "Off (Hard)", 7: "Hibernate", 8: "Off (Soft)",
        }
        return {
            "power_state_code": state_num,
            "power_state": state_map.get(state_num, f"Unknown ({state_num})"),
        }

    def change_power_state(self, state: PowerState) -> dict:
        """Change the power state of the system."""
        resource = (
            "http://schemas.dmtf.org/wbem/wscim/1/cim-schema/2/"
            "CIM_PowerManagementService"
        )
        action = (
            "http://schemas.dmtf.org/wbem/wscim/1/cim-schema/2/"
            "CIM_PowerManagementService/RequestPowerStateChange"
        )
        body = f"""
        <p:RequestPowerStateChange_INPUT
            xmlns:p="{resource}">
          <p:PowerState>{state.value}</p:PowerState>
          <p:ManagedElement>
            <wsa:Address>http://schemas.xmlsoap.org/ws/2004/08/addressing/role/anonymous</wsa:Address>
            <wsa:ReferenceParameters>
              <wsman:ResourceURI>http://schemas.dmtf.org/wbem/wscim/1/cim-schema/2/CIM_ComputerSystem</wsman:ResourceURI>
              <wsman:SelectorSet>
                <wsman:Selector Name="CreationClassName">CIM_ComputerSystem</wsman:Selector>
                <wsman:Selector Name="Name">ManagedSystem</wsman:Selector>
              </wsman:SelectorSet>
            </wsa:ReferenceParameters>
          </p:ManagedElement>
        </p:RequestPowerStateChange_INPUT>
        """
        envelope = _build_envelope(resource, action, body,
                                   selector={"CreationClassName": "CIM_PowerManagementService",
                                             "Name": "Intel(r) AMT Power Management Service",
                                             "SystemCreationClassName": "CIM_ComputerSystem",
                                             "SystemName": "Intel(r) AMT"})
        root = self._send(envelope)

        return_el = root.find(".//{%s}ReturnValue" % NS["cim_ps"])
        return_val = int(return_el.text) if return_el is not None else -1
        return {
            "return_code": return_val,
            "success": return_val == 0,
            "message": "Success" if return_val == 0 else f"Failed (code: {return_val})",
        }

    # -- Boot Configuration --

    def set_pxe_boot(self) -> dict:
        """Configure the next boot to use PXE (network boot)."""
        # Step 1: Get current AMT_BootSettingData
        resource = "http://intel.com/wbem/wscim/1/amt-schema/1/AMT_BootSettingData"
        action = "http://schemas.xmlsoap.org/ws/2004/09/transfer/Get"
        envelope = _build_envelope(resource, action,
                                   selector={"InstanceID": "Intel(r) AMT: Boot Configuration 0"})
        root = self._send(envelope)

        # Step 2: Modify boot settings to enable PXE
        put_action = "http://schemas.xmlsoap.org/ws/2004/09/transfer/Put"
        body = f"""
        <p:AMT_BootSettingData
            xmlns:p="http://intel.com/wbem/wscim/1/amt-schema/1/AMT_BootSettingData">
          <p:BIOSPause>false</p:BIOSPause>
          <p:BIOSSetup>false</p:BIOSSetup>
          <p:BootMediaIndex>0</p:BootMediaIndex>
          <p:ConfigurationDataReset>false</p:ConfigurationDataReset>
          <p:ElementName>Intel(r) AMT: Boot Configuration 0</p:ElementName>
          <p:EnforceSecureBoot>false</p:EnforceSecureBoot>
          <p:FirmwareVerbosity>0</p:FirmwareVerbosity>
          <p:ForcedProgressEvents>false</p:ForcedProgressEvents>
          <p:IDERBootDevice>0</p:IDERBootDevice>
          <p:InstanceID>Intel(r) AMT: Boot Configuration 0</p:InstanceID>
          <p:LockKeyboard>false</p:LockKeyboard>
          <p:LockPowerButton>false</p:LockPowerButton>
          <p:LockResetButton>false</p:LockResetButton>
          <p:LockSleepButton>false</p:LockSleepButton>
          <p:OwningEntity>Intel(r) AMT</p:OwningEntity>
          <p:ReflashBIOS>false</p:ReflashBIOS>
          <p:UseIDER>false</p:UseIDER>
          <p:UseSOL>false</p:UseSOL>
          <p:UseSafeMode>false</p:UseSafeMode>
          <p:UserPasswordBypass>false</p:UserPasswordBypass>
          <p:SecureErase>false</p:SecureErase>
          <p:RPEEnabled>true</p:RPEEnabled>
          <p:PlatformErase>0</p:PlatformErase>
          <p:UEFIBootParametersArray></p:UEFIBootParametersArray>
          <p:UEFIBootNumberOfParams>0</p:UEFIBootNumberOfParams>
        </p:AMT_BootSettingData>
        """
        envelope = _build_envelope(resource, put_action, body,
                                   selector={"InstanceID": "Intel(r) AMT: Boot Configuration 0"})
        self._send(envelope)

        # Step 3: Set PXE as the boot source via CIM_BootConfigSetting
        bcs_resource = (
            "http://schemas.dmtf.org/wbem/wscim/1/cim-schema/2/"
            "CIM_BootConfigSetting"
        )
        bcs_action = (
            "http://schemas.dmtf.org/wbem/wscim/1/cim-schema/2/"
            "CIM_BootConfigSetting/ChangeBootOrder"
        )
        bcs_body = f"""
        <p:ChangeBootOrder_INPUT
            xmlns:p="http://schemas.dmtf.org/wbem/wscim/1/cim-schema/2/CIM_BootConfigSetting">
          <p:Source>
            <wsa:Address>http://schemas.xmlsoap.org/ws/2004/08/addressing/role/anonymous</wsa:Address>
            <wsa:ReferenceParameters>
              <wsman:ResourceURI>http://schemas.dmtf.org/wbem/wscim/1/cim-schema/2/CIM_BootSourceSetting</wsman:ResourceURI>
              <wsman:SelectorSet>
                <wsman:Selector Name="InstanceID">Intel(r) AMT: Force PXE Boot</wsman:Selector>
              </wsman:SelectorSet>
            </wsa:ReferenceParameters>
          </p:Source>
        </p:ChangeBootOrder_INPUT>
        """
        envelope = _build_envelope(bcs_resource, bcs_action, bcs_body,
                                   selector={"InstanceID": "Intel(r) AMT: Boot Configuration 0"})
        root = self._send(envelope)

        return_el = root.find(".//{%s}ReturnValue" % NS["cim_boot"])
        return_val = int(return_el.text) if return_el is not None else -1

        # Step 4: Set one-time boot via CIM_BootService
        svc_resource = (
            "http://schemas.dmtf.org/wbem/wscim/1/cim-schema/2/"
            "CIM_BootService"
        )
        svc_action = (
            "http://schemas.dmtf.org/wbem/wscim/1/cim-schema/2/"
            "CIM_BootService/SetBootConfigRole"
        )
        svc_body = f"""
        <p:SetBootConfigRole_INPUT
            xmlns:p="http://schemas.dmtf.org/wbem/wscim/1/cim-schema/2/CIM_BootService">
          <p:BootConfigSetting>
            <wsa:Address>http://schemas.xmlsoap.org/ws/2004/08/addressing/role/anonymous</wsa:Address>
            <wsa:ReferenceParameters>
              <wsman:ResourceURI>http://schemas.dmtf.org/wbem/wscim/1/cim-schema/2/CIM_BootConfigSetting</wsman:ResourceURI>
              <wsman:SelectorSet>
                <wsman:Selector Name="InstanceID">Intel(r) AMT: Boot Configuration 0</wsman:Selector>
              </wsman:SelectorSet>
            </wsa:ReferenceParameters>
          </p:BootConfigSetting>
          <p:Role>1</p:Role>
        </p:SetBootConfigRole_INPUT>
        """
        envelope = _build_envelope(svc_resource, svc_action, svc_body)
        self._send(envelope)

        return {
            "return_code": return_val,
            "success": return_val == 0,
            "message": "PXE boot configured for next reboot" if return_val == 0
                       else f"Failed to set PXE boot (code: {return_val})",
        }

    def pxe_reboot(self) -> dict:
        """Configure PXE boot and immediately reboot the system."""
        pxe_result = self.set_pxe_boot()
        if not pxe_result["success"]:
            return pxe_result

        power_result = self.change_power_state(PowerState.POWER_CYCLE_SOFT_GRACEFUL)
        return {
            "pxe_config": pxe_result,
            "power_cycle": power_result,
            "success": power_result["success"],
            "message": "PXE boot initiated - system is rebooting"
                       if power_result["success"]
                       else f"PXE configured but reboot failed: {power_result['message']}",
        }

    # -- System Information --

    def get_general_settings(self) -> dict:
        """Get AMT general settings (hostname, digest realm, etc.)."""
        resource = "http://intel.com/wbem/wscim/1/amt-schema/1/AMT_GeneralSettings"
        action = "http://schemas.xmlsoap.org/ws/2004/09/transfer/Get"
        envelope = _build_envelope(resource, action)
        root = self._send(envelope)

        body = root.find(".//s:Body", NS)
        gs = body.find(".//{%s}AMT_GeneralSettings" % NS["amt_gs"])

        result = {}
        if gs is not None:
            for child in gs:
                tag = child.tag.split("}")[-1] if "}" in child.tag else child.tag
                result[tag] = child.text
        return result

    def get_hardware_info(self) -> dict:
        """Get basic hardware information via CIM_ComputerSystem."""
        resource = (
            "http://schemas.dmtf.org/wbem/wscim/1/cim-schema/2/"
            "CIM_ComputerSystem"
        )
        action = "http://schemas.xmlsoap.org/ws/2004/09/transfer/Get"
        envelope = _build_envelope(resource, action,
                                   selector={"CreationClassName": "CIM_ComputerSystem",
                                             "Name": "ManagedSystem"})
        root = self._send(envelope)

        body = root.find(".//s:Body", NS)
        cs = body.find(".//{%s}CIM_ComputerSystem" % NS["cim_cs"])

        result = {}
        if cs is not None:
            for child in cs:
                tag = child.tag.split("}")[-1] if "}" in child.tag else child.tag
                result[tag] = child.text
        return result

    def get_bios_info(self) -> dict:
        """Get BIOS version information."""
        resource = (
            "http://schemas.dmtf.org/wbem/wscim/1/cim-schema/2/"
            "CIM_BIOSElement"
        )
        action = "http://schemas.xmlsoap.org/ws/2004/09/transfer/Get"
        envelope = _build_envelope(resource, action,
                                   selector={"Name": "Primary BIOS"})
        root = self._send(envelope)

        body = root.find(".//s:Body", NS)
        bios = body.find(".//{%s}CIM_BIOSElement" % NS["cim_bios"])

        result = {}
        if bios is not None:
            for child in bios:
                tag = child.tag.split("}")[-1] if "}" in child.tag else child.tag
                result[tag] = child.text
        return result


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Intel AMT Controller for Premio RCO-3000 Servers",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--host", required=True, help="AMT IP address or hostname")
    parser.add_argument("--user", default="admin", help="AMT username (default: admin)")
    parser.add_argument("--password", required=True, help="AMT password")
    parser.add_argument("--port", type=int, default=AMT_PORT_TLS,
                        help=f"AMT port (default: {AMT_PORT_TLS})")
    parser.add_argument("--no-tls", action="store_true",
                        help="Disable TLS (use HTTP instead of HTTPS)")
    parser.add_argument("-v", "--verbose", action="store_true", help="Verbose logging")

    subparsers = parser.add_subparsers(dest="command", help="Command to execute")

    subparsers.add_parser("status", help="Get current power state")
    subparsers.add_parser("pxe-boot", help="Set PXE as next boot device")
    subparsers.add_parser("pxe-reboot", help="Set PXE boot and reboot immediately")
    subparsers.add_parser("hw-info", help="Get hardware information")
    subparsers.add_parser("bios-info", help="Get BIOS information")
    subparsers.add_parser("settings", help="Get AMT general settings")

    power_parser = subparsers.add_parser("power", help="Change power state")
    power_parser.add_argument("action", choices=[
        "on", "off", "off-graceful", "cycle", "cycle-graceful", "reset",
    ])

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    config = AMTConfig(
        host=args.host,
        username=args.user,
        password=args.password,
        port=args.port if not args.no_tls else AMT_PORT_NO_TLS,
        use_tls=not args.no_tls,
    )
    client = AMTClient(config)

    try:
        if args.command == "status":
            result = client.get_power_state()
            print(f"Power State: {result['power_state']} (code: {result['power_state_code']})")

        elif args.command == "pxe-boot":
            result = client.set_pxe_boot()
            print(f"PXE Boot Config: {result['message']}")

        elif args.command == "pxe-reboot":
            result = client.pxe_reboot()
            print(f"PXE Reboot: {result['message']}")

        elif args.command == "hw-info":
            result = client.get_hardware_info()
            for k, v in result.items():
                print(f"  {k}: {v}")

        elif args.command == "bios-info":
            result = client.get_bios_info()
            for k, v in result.items():
                print(f"  {k}: {v}")

        elif args.command == "settings":
            result = client.get_general_settings()
            for k, v in result.items():
                print(f"  {k}: {v}")

        elif args.command == "power":
            action_map = {
                "on": PowerState.ON,
                "off": PowerState.OFF_HARD,
                "off-graceful": PowerState.OFF_SOFT_GRACEFUL,
                "cycle": PowerState.POWER_CYCLE,
                "cycle-graceful": PowerState.POWER_CYCLE_SOFT_GRACEFUL,
                "reset": PowerState.MASTER_BUS_RESET,
            }
            state = action_map[args.action]
            result = client.change_power_state(state)
            print(f"Power {args.action}: {result['message']}")

    except requests.exceptions.ConnectionError:
        logger.error("Cannot connect to %s:%d - verify AMT is enabled and reachable",
                     config.host, config.port)
        sys.exit(1)
    except requests.exceptions.HTTPError as e:
        logger.error("HTTP error: %s", e)
        sys.exit(1)


if __name__ == "__main__":
    main()
