import os

DNAC = os.getenv("DNAC") or "sandboxdnac.cisco.com"
DNAC_USER = os.getenv("DNAC_USER") or "devnetuser"
DNAC_PASSWORD = os.getenv("DNAC_PASSWORD") or "Cisco123!"
DNAC_VERSION = os.getenv("DNAC_VERSION") or "2.3.7.9"
