"""
Tor Browser known schema definitions.

This module defines known torrc directives, state file keys, and other
schema information for schema warning support.

References:
- https://2019.www.torproject.org/docs/tor-manual.html.en
- Tor source: src/app/config/torrc.sample.in

Initial implementation
"""
from __future__ import annotations

from typing import Dict, Set

__all__ = [
    "KNOWN_TORRC_DIRECTIVES",
    "KNOWN_STATE_KEYS",
    "TORRC_DIRECTIVE_CATEGORIES",
    "get_directive_category",
]


# Known torrc directives (not exhaustive, but covers common ones)
# Organized by category for documentation
KNOWN_TORRC_DIRECTIVES: Set[str] = {
    # =========================================================================
    # General Configuration
    # =========================================================================
    "DataDirectory",
    "Log",
    "RunAsDaemon",
    "User",
    "PidFile",
    "AvoidDiskWrites",
    "ControlPort",
    "ControlSocket",
    "HashedControlPassword",
    "CookieAuthentication",
    "CookieAuthFile",
    "CookieAuthFileGroupReadable",

    # =========================================================================
    # Client Configuration
    # =========================================================================
    "SocksPort",
    "SocksPolicy",
    "SocksTimeout",
    "SafeSocks",
    "TestSocks",
    "AllowNonRFC953Hostnames",
    "HTTPTunnelPort",
    "TransPort",
    "TransProxyType",
    "NATDPort",
    "DNSPort",
    "AutomapHostsOnResolve",
    "AutomapHostsSuffixes",
    "VirtualAddrNetworkIPv4",
    "VirtualAddrNetworkIPv6",

    # =========================================================================
    # Entry/Exit Node Configuration
    # =========================================================================
    "EntryNodes",
    "ExitNodes",
    "ExcludeNodes",
    "ExcludeExitNodes",
    "StrictNodes",
    "MiddleNodes",
    "GeoIPFile",
    "GeoIPv6File",

    # =========================================================================
    # Bridge Configuration (Censorship Circumvention)
    # =========================================================================
    "UseBridges",
    "Bridge",
    "UpdateBridgesFromAuthority",
    "ClientTransportPlugin",
    "ServerTransportPlugin",
    "ServerTransportListenAddr",
    "ServerTransportOptions",

    # =========================================================================
    # Circuit Configuration
    # =========================================================================
    "CircuitBuildTimeout",
    "CircuitIdleTimeout",
    "CircuitStreamTimeout",
    "LearnCircuitBuildTimeout",
    "MaxClientCircuitsPending",
    "NewCircuitPeriod",
    "MaxCircuitDirtiness",

    # =========================================================================
    # Security/Privacy
    # =========================================================================
    "DisableAllSwap",
    "HardwareAccel",
    "AccelName",
    "AccelDir",
    "UseEntryGuards",
    "UseEntryGuardsAsDirGuards",
    "NumEntryGuards",
    "NumDirectoryGuards",
    "GuardfractionFile",
    "UseMicrodescriptors",
    "PathBiasCircThreshold",
    "PathBiasNoticeRate",
    "PathBiasWarnRate",
    "PathBiasExtremeRate",
    "PathBiasScaleThreshold",
    "PathBiasUseThreshold",
    "PathBiasNoticeUseRate",
    "PathBiasExtremeUseRate",
    "PathBiasScaleUseThreshold",

    # =========================================================================
    # Relay Configuration (if running as relay)
    # =========================================================================
    "ORPort",
    "Address",
    "OutboundBindAddress",
    "OutboundBindAddressOR",
    "OutboundBindAddressExit",
    "Nickname",
    "ContactInfo",
    "DirPort",
    "DirPortFrontPage",
    "MyFamily",
    "BridgeRelay",
    "PublishServerDescriptor",
    "ExitPolicy",
    "ExitPolicyRejectPrivate",
    "ExitPolicyRejectLocalInterfaces",
    "IPv6Exit",
    "ReducedExitPolicy",
    "MaxOnionQueueDelay",
    "RelayBandwidthRate",
    "RelayBandwidthBurst",
    "PerConnBWRate",
    "PerConnBWBurst",
    "BandwidthRate",
    "BandwidthBurst",
    "MaxAdvertisedBandwidth",
    "AccountingMax",
    "AccountingRule",
    "AccountingStart",
    "ShutdownWaitLength",
    "HeartbeatPeriod",
    "SigningKeyLifetime",
    "OfflineMasterKey",

    # =========================================================================
    # Hidden Service Configuration
    # =========================================================================
    "HiddenServiceDir",
    "HiddenServicePort",
    "HiddenServiceVersion",
    "HiddenServiceAuthorizeClient",
    "HiddenServiceAllowUnknownPorts",
    "HiddenServiceMaxStreams",
    "HiddenServiceMaxStreamsCloseCircuit",
    "HiddenServiceDirGroupReadable",
    "HiddenServiceNumIntroductionPoints",
    "RendPostPeriod",

    # =========================================================================
    # Directory Server/Authority
    # =========================================================================
    "DirCache",
    "DirAuthority",
    "AlternateBridgeAuthority",
    "AlternateDirAuthority",
    "DisableDebuggerAttachment",
    "FetchDirInfoEarly",
    "FetchDirInfoExtraEarly",
    "FetchHidServDescriptors",
    "FetchServerDescriptors",
    "FetchUselessDescriptors",
    "DownloadExtraInfo",
    "ClientBootstrapConsensusAuthorityDownloadInitialDelay",
    "ClientBootstrapConsensusFallbackDownloadInitialDelay",
    "ClientBootstrapConsensusAuthorityOnlyDownloadInitialDelay",
    "ClientBootstrapConsensusMaxInProgressTries",

    # =========================================================================
    # Testing/Sandbox
    # =========================================================================
    "TestingTorNetwork",
    "Sandbox",
    "Socks5Proxy",
    "Socks5ProxyUsername",
    "Socks5ProxyPassword",
    "Socks4Proxy",
    "HTTPSProxy",
    "HTTPSProxyAuthenticator",

    # =========================================================================
    # Tor Browser Specific
    # =========================================================================
    "ClientOnionAuthDir",
    "ClientUseIPv4",
    "ClientUseIPv6",
    "ClientPreferIPv6ORPort",
    "ClientPreferIPv6DirPort",
    "ReachableAddresses",
    "ReachableORAddresses",
    "ReachableDirAddresses",
    "FascistFirewall",
    "FirewallPorts",
    "ConnectionPadding",
    "ReducedConnectionPadding",
}


# Known state file keys
KNOWN_STATE_KEYS: Set[str] = {
    # Version info
    "TorVersion",

    # Timestamps
    "LastWritten",
    "AccountingBytesReadInInterval",
    "AccountingBytesWrittenInInterval",
    "AccountingExpectedUsage",
    "AccountingIntervalStart",
    "AccountingSecondsActive",
    "AccountingSecondsToReachSoftLimit",
    "AccountingSoftLimitHitAt",

    # Guards
    "Guard",
    "GuardFraction",

    # Circuit building
    "CircuitBuildTimeBin",
    "CircuitBuildAbandonedCount",
    "CircuitBuildTimeCount",
    "CircuitBuildTimeTotal",

    # Transport plugins
    "TransportProxy",

    # Hidden services
    "HidServRevCounter",

    # BWAuth
    "BWHistoryReadEnds",
    "BWHistoryReadInterval",
    "BWHistoryReadMaxima",
    "BWHistoryReadValues",
    "BWHistoryWriteEnds",
    "BWHistoryWriteInterval",
    "BWHistoryWriteMaxima",
    "BWHistoryWriteValues",
    "BWHistoryDirReadEnds",
    "BWHistoryDirReadInterval",
    "BWHistoryDirReadMaxima",
    "BWHistoryDirReadValues",
    "BWHistoryDirWriteEnds",
    "BWHistoryDirWriteInterval",
    "BWHistoryDirWriteMaxima",
    "BWHistoryDirWriteValues",

    # Misc
    "TotalBuildTimes",
    "MinRouteLifetime",
    "Dormant",
    "DoSCircuitCreationEnabled",
    "DoSConnectionEnabled",
}


# Directive categories for forensic reporting
TORRC_DIRECTIVE_CATEGORIES: Dict[str, str] = {
    # Bridge/censorship circumvention (high forensic value)
    "UseBridges": "censorship",
    "Bridge": "censorship",
    "ClientTransportPlugin": "censorship",

    # Entry/exit control (forensic interest)
    "EntryNodes": "routing",
    "ExitNodes": "routing",
    "ExcludeNodes": "routing",
    "ExcludeExitNodes": "routing",
    "StrictNodes": "routing",

    # Hidden services (high forensic value)
    "HiddenServiceDir": "hidden_service",
    "HiddenServicePort": "hidden_service",
    "HiddenServiceVersion": "hidden_service",

    # Network access
    "SocksPort": "network",
    "ControlPort": "network",
    "HTTPTunnelPort": "network",

    # Security
    "CookieAuthentication": "security",
    "HashedControlPassword": "security",
}


def get_directive_category(directive: str) -> str:
    """
    Get the category for a torrc directive.

    Returns:
        Category string or "general" if unknown
    """
    return TORRC_DIRECTIVE_CATEGORIES.get(directive, "general")
