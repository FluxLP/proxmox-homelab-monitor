# Copyright (c) 2026 Felix Wicha
# SPDX-License-Identifier: MIT
"""
Homelab-Erweiterung: Domain-Überwachung, Dienst-Gesamtstatus, Problem-Übersicht.

Kommt zu allem hinzu, was die Proxmox-VE-Integration ohnehin liefert. Pro
Container/VM lässt sich in den Optionen eine Domain eintragen. Daraus entstehen:

* `binary_sensor.<gast>_domain`        Domain erreichbar (HTTP 1xx-4xx)
* `sensor.<gast>_response_time`        Antwortzeit der Domain in ms
* `binary_sensor.<gast>_service`       Gesamtstatus: Gast läuft und Domain ok
                                       (für jeden Gast, auch ohne Domain)
* `sensor.homelab_<node>_problems`     Anzahl der Dienste mit Problem, Liste
                                       der Probleme als Attribut

Alles liegt in einer eigenen Datei, damit Updates der Originalintegration
sich mit möglichst wenigen Konflikten übernehmen lassen.
"""

from __future__ import annotations

import asyncio
import re
import time
from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any

import aiohttp
import voluptuous as vol
from homeassistant.components.binary_sensor import (
    BinarySensorDeviceClass,
    BinarySensorEntity,
)
from homeassistant.components.sensor import (
    SensorDeviceClass,
    SensorEntity,
    SensorStateClass,
)
from homeassistant.const import EntityCategory, UnitOfTime
from homeassistant.core import callback
from homeassistant.helpers import selector
from homeassistant.helpers.aiohttp_client import async_get_clientsession
from homeassistant.helpers.device_registry import DeviceInfo
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator
from homeassistant.util import dt as dt_util
from homeassistant.util import slugify

from .const import CONF_LXC, CONF_QEMU, COORDINATORS, DOMAIN, LOGGER, ProxmoxType
from .discovery import selected
from .entity import ProxmoxEntity, ProxmoxEntityDescription

if TYPE_CHECKING:
    from homeassistant.config_entries import ConfigEntry, OptionsFlow
    from homeassistant.core import HomeAssistant
    from homeassistant.data_entry_flow import FlowResult
    from homeassistant.helpers.entity_platform import EntityPlatform

HOMELAB = "homelab"

# Optionen
CONF_HOMELAB_DOMAINS = "homelab_domains"  # {"lxc_100": "https://jellyfin.example.org"}
CONF_HOMELAB_INTERVAL = "homelab_interval"
CONF_HOMELAB_TIMEOUT = "homelab_timeout"
CONF_HOMELAB_THRESHOLD = "homelab_failure_threshold"
CONF_HOMELAB_VERIFY_SSL = "homelab_verify_ssl"

HOMELAB_DEFAULTS: dict[str, Any] = {
    CONF_HOMELAB_INTERVAL: 60,
    CONF_HOMELAB_TIMEOUT: 10,
    CONF_HOMELAB_THRESHOLD: 2,
    CONF_HOMELAB_VERIFY_SSL: True,
}

GUEST_TYPES = ((ProxmoxType.QEMU, CONF_QEMU), (ProxmoxType.LXC, CONF_LXC))

# Problem-Codes: sprachneutral in den Attributen, die Karten übersetzen selbst
PROBLEM_OFFLINE = "offline"
PROBLEM_UNAVAILABLE = "unavailable"
PROBLEM_DOMAIN = "domain"

MESSAGES = {
    "de": {
        PROBLEM_OFFLINE: "ist offline",
        PROBLEM_UNAVAILABLE: "Status nicht verfügbar",
        PROBLEM_DOMAIN: "Domain nicht erreichbar",
        "ok": "In Betrieb",
    },
    "en": {
        PROBLEM_OFFLINE: "is offline",
        PROBLEM_UNAVAILABLE: "status unavailable",
        PROBLEM_DOMAIN: "domain unreachable",
        "ok": "Running",
    },
}


def homelab_options(config_entry: ConfigEntry) -> dict[str, Any]:
    """Return the homelab options with defaults filled in."""
    return {**HOMELAB_DEFAULTS, **config_entry.options}


def guest_domains(config_entry: ConfigEntry) -> dict[str, str]:
    """Return the configured domains, keyed like `lxc_100`."""
    return dict(config_entry.options.get(CONF_HOMELAB_DOMAINS) or {})


def normalize_url(value: str | None) -> str:
    """Turn `jellyfin.example.org` into `https://jellyfin.example.org`."""
    value = str(value or "").strip()
    if not value:
        return ""
    return value if re.match(r"^https?://", value, re.IGNORECASE) else f"https://{value}"


def pretty_guest_name(name: str | None, vmid: str) -> str:
    """`paperless-ngx` -> `Paperless-ngx`; fall back to the id."""
    name = str(name or "").strip()
    return name[:1].upper() + name[1:] if name else vmid


# ---------------------------------------------------------------------------
# Domain-Prüfung
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class CheckResult:
    """Outcome of one domain check."""

    url: str
    ok: bool | None
    raw_ok: bool
    http_status: int | None
    response_time_ms: int | None
    error: str | None
    failures: int
    checked_at: datetime


class HomelabDomainCoordinator(DataUpdateCoordinator[dict[str, CheckResult]]):
    """Check every configured domain; one result per guest key."""

    def __init__(self, hass: HomeAssistant, config_entry: ConfigEntry) -> None:
        """Set up the coordinator from the entry options."""
        options = homelab_options(config_entry)
        super().__init__(
            hass,
            LOGGER,
            config_entry=config_entry,
            name=f"{DOMAIN}_homelab",
            update_interval=timedelta(seconds=max(15, int(options[CONF_HOMELAB_INTERVAL]))),
        )
        self.urls = {key: normalize_url(url) for key, url in guest_domains(config_entry).items() if url}
        self._timeout = max(2, int(options[CONF_HOMELAB_TIMEOUT]))
        self._threshold = max(1, int(options[CONF_HOMELAB_THRESHOLD]))
        self._session = async_get_clientsession(hass, verify_ssl=bool(options[CONF_HOMELAB_VERIFY_SSL]))
        self._failures: dict[str, int] = {}

    async def _check(self, url: str) -> tuple[int | None, int | None, str | None]:
        """Return (status, milliseconds, error) for one URL."""
        start = time.monotonic()
        try:
            async with asyncio.timeout(self._timeout):
                async with self._session.get(
                    url,
                    allow_redirects=False,
                    headers={"User-Agent": "ProxmoxHomelabMonitor"},
                ) as response:
                    status = response.status
        except TimeoutError:
            return None, None, "timeout"
        except aiohttp.ClientSSLError as err:
            return None, None, f"ssl: {err}"
        except aiohttp.ClientError as err:
            return None, None, f"{type(err).__name__}: {err}"
        return status, round((time.monotonic() - start) * 1000), None

    async def _async_update_data(self) -> dict[str, CheckResult]:
        """Check all domains in parallel. A failing domain never fails the update."""
        keys = list(self.urls)
        outcomes = await asyncio.gather(*(self._check(self.urls[key]) for key in keys))
        previous = self.data or {}
        now = dt_util.utcnow()
        data: dict[str, CheckResult] = {}
        for key, (status, millis, error) in zip(keys, outcomes, strict=True):
            raw_ok = status is not None and 0 < status < 500
            failures = 0 if raw_ok else self._failures.get(key, 0) + 1
            self._failures[key] = failures
            # Erst nach mehreren Fehlschlägen hintereinander als "down" melden,
            # damit ein einzelner Aussetzer keine Benachrichtigung auslöst.
            before = previous.get(key)
            ok = raw_ok or (before is not None and before.ok is True and failures < self._threshold)
            data[key] = CheckResult(
                url=self.urls[key],
                ok=ok,
                raw_ok=raw_ok,
                http_status=status,
                response_time_ms=millis,
                error=error,
                failures=failures,
                checked_at=now,
            )
        return data


async def async_setup_homelab(hass: HomeAssistant, config_entry: ConfigEntry) -> None:
    """Create the domain coordinator; called before the platforms are set up."""
    coordinator = HomelabDomainCoordinator(hass, config_entry)
    if coordinator.urls:
        await coordinator.async_refresh()
    else:
        coordinator.data = {}
    config_entry.runtime_data[HOMELAB] = coordinator


# ---------------------------------------------------------------------------
# Status eines Gastes, geteilt zwischen Dienst-Sensor und Problem-Übersicht
# ---------------------------------------------------------------------------
def tracked_guests(config_entry: ConfigEntry, only: tuple[str, str] | None = None) -> list[tuple[ProxmoxType, str]]:
    """Return (type, vmid) of every guest with a coordinator."""
    coordinators = config_entry.runtime_data[COORDINATORS]
    guests: list[tuple[ProxmoxType, str]] = []
    for api_category, key in GUEST_TYPES:
        ids = [only[1]] if only and only[0] == api_category else ([] if only else selected(config_entry, key, None))
        for vmid in ids:
            coordinator = coordinators.get(f"{api_category}_{vmid}")
            if coordinator is not None and coordinator.data is not None:
                guests.append((api_category, str(vmid)))
    return guests


def guest_status(hass: HomeAssistant, config_entry: ConfigEntry, api_category: ProxmoxType, vmid: str) -> dict[str, Any]:
    """Evaluate one guest: container state plus domain."""
    lang = "de" if str(hass.config.language).startswith("de") else "en"
    msgs = MESSAGES[lang]
    runtime = config_entry.runtime_data
    coordinator = runtime[COORDINATORS].get(f"{api_category}_{vmid}")
    data = coordinator.data if coordinator is not None and coordinator.last_update_success else None
    name = pretty_guest_name(getattr(data, "name", None), vmid)
    node = getattr(data, "node", None)
    raw_status = getattr(data, "status", None)
    guest_state = raw_status if isinstance(raw_status, str) else None

    key = f"{api_category}_{vmid}"
    homelab: HomelabDomainCoordinator | None = runtime.get(HOMELAB)
    check = (homelab.data or {}).get(key) if homelab else None
    url = homelab.urls.get(key, "") if homelab else ""

    problem = None
    if guest_state is None:
        problem = PROBLEM_UNAVAILABLE
    elif guest_state != "running":
        problem = PROBLEM_OFFLINE
    elif url and check is not None and check.ok is False:
        problem = PROBLEM_DOMAIN

    if problem == PROBLEM_DOMAIN:
        detail = f" (HTTP {check.http_status})" if check and check.http_status else (f" ({check.error})" if check and check.error else "")
        message = f"{name}: {msgs[PROBLEM_DOMAIN]}{detail}"
    elif problem:
        message = f"{name} {msgs[problem]}" if problem == PROBLEM_OFFLINE else f"{name}: {msgs[problem]}"
    else:
        message = f"{name}: {msgs['ok']}"

    return {
        "service_name": name,
        "type": str(api_category),
        "vmid": vmid,
        "node": node,
        "guest_status": guest_state,
        "url": url or None,
        "domain_ok": None if not url or check is None else check.ok,
        "http_status": check.http_status if check else None,
        "problem": problem,
        "message": message,
    }


def _guest_device(config_entry: ConfigEntry, api_category: ProxmoxType, vmid: str) -> DeviceInfo:
    """Attach to the guest's existing device."""
    return DeviceInfo(identifiers={(DOMAIN, f"{config_entry.entry_id}_{api_category.upper()}_{vmid}")})


# ---------------------------------------------------------------------------
# Entitäten
# ---------------------------------------------------------------------------
class HomelabEntity(ProxmoxEntity):
    """Base for the homelab entities: suggests readable English entity ids."""

    _homelab_item: str = ""

    def __init__(self, coordinator: DataUpdateCoordinator, config_entry: ConfigEntry, unique_id: str, translation_key: str) -> None:
        """Initialize with a translated name."""
        super().__init__(
            coordinator,
            unique_id,
            ProxmoxEntityDescription(key=translation_key, translation_key=translation_key),
        )
        self._config_entry = config_entry
        self._homelab_item = translation_key

    def _suggested_object_id(self) -> str | None:
        """Object id for the standard scheme, without relying on the UI language."""
        return None

    def add_to_platform_start(self, hass: HomeAssistant, platform: EntityPlatform, parallel_updates: Any) -> None:
        """Keep the extended scheme of the original; otherwise suggest our own id."""
        super().add_to_platform_start(hass, platform, parallel_updates)
        if self.entity_id is None and (object_id := self._suggested_object_id()):
            self.entity_id = f"{platform.domain}.{object_id}"


class HomelabGuestEntity(HomelabEntity):
    """Entity of one guest; listens to the guest and the domain coordinator."""

    def __init__(self, config_entry: ConfigEntry, api_category: ProxmoxType, vmid: str, translation_key: str) -> None:
        """Bind to the guest coordinator."""
        coordinators = config_entry.runtime_data[COORDINATORS]
        super().__init__(
            coordinators[f"{api_category}_{vmid}"],
            config_entry,
            f"{config_entry.entry_id}_{api_category.upper()}_{vmid}_homelab_{translation_key}",
            translation_key,
        )
        self._api_category = api_category
        self._vmid = vmid
        self._key = f"{api_category}_{vmid}"
        self._attr_device_info = _guest_device(config_entry, api_category, vmid)

    @property
    def _homelab(self) -> HomelabDomainCoordinator:
        return self._config_entry.runtime_data[HOMELAB]

    def _suggested_object_id(self) -> str | None:
        data = self.coordinator.data
        name = getattr(data, "name", None) or ""
        return slugify(f"{self._api_category} {name} {self._vmid} {self._homelab_item}")

    async def async_added_to_hass(self) -> None:
        """Also update when the domain check finishes."""
        await super().async_added_to_hass()
        self.async_on_remove(self._homelab.async_add_listener(self._handle_coordinator_update))


class HomelabServiceBinarySensor(HomelabGuestEntity, BinarySensorEntity):
    """Overall state: guest running and, if configured, domain reachable."""

    _attr_device_class = BinarySensorDeviceClass.RUNNING

    def __init__(self, config_entry: ConfigEntry, api_category: ProxmoxType, vmid: str) -> None:
        """Initialize."""
        super().__init__(config_entry, api_category, vmid, "service")

    @property
    def available(self) -> bool:
        """Stay available: an unreachable guest is exactly what this reports."""
        return True

    @property
    def _status(self) -> dict[str, Any]:
        return guest_status(self.hass, self._config_entry, self._api_category, self._vmid)

    @property
    def is_on(self) -> bool:
        """Return True when nothing is wrong."""
        return self._status["problem"] is None

    @property
    def extra_state_attributes(self) -> dict[str, Any]:
        """Expose everything the cards and notifications need."""
        return self._status


class HomelabDomainBinarySensor(HomelabGuestEntity, BinarySensorEntity):
    """Domain reachable."""

    _attr_device_class = BinarySensorDeviceClass.CONNECTIVITY

    def __init__(self, config_entry: ConfigEntry, api_category: ProxmoxType, vmid: str) -> None:
        """Initialize."""
        super().__init__(config_entry, api_category, vmid, "domain")

    @property
    def _check(self) -> CheckResult | None:
        return (self._homelab.data or {}).get(self._key)

    @property
    def available(self) -> bool:
        """Available as soon as there is a result."""
        return self._check is not None and self._check.ok is not None

    @property
    def is_on(self) -> bool | None:
        """Return True when the domain answers with HTTP 1xx-4xx."""
        check = self._check
        return None if check is None else check.ok

    @property
    def extra_state_attributes(self) -> dict[str, Any]:
        """Details of the last check."""
        check = self._check
        if check is None:
            return {"url": self._homelab.urls.get(self._key)}
        return {
            "url": check.url,
            "http_status": check.http_status,
            "response_time_ms": check.response_time_ms,
            "error": check.error,
            "consecutive_failures": check.failures,
            "last_check": check.checked_at.isoformat(),
        }


class HomelabResponseTimeSensor(HomelabGuestEntity, SensorEntity):
    """Response time of the domain."""

    _attr_device_class = SensorDeviceClass.DURATION
    _attr_state_class = SensorStateClass.MEASUREMENT
    _attr_native_unit_of_measurement = UnitOfTime.MILLISECONDS
    _attr_entity_category = EntityCategory.DIAGNOSTIC

    def __init__(self, config_entry: ConfigEntry, api_category: ProxmoxType, vmid: str) -> None:
        """Initialize."""
        super().__init__(config_entry, api_category, vmid, "response_time")

    @property
    def available(self) -> bool:
        """Only with a successful answer."""
        check = (self._homelab.data or {}).get(self._key)
        return check is not None and check.response_time_ms is not None

    @property
    def native_value(self) -> int | None:
        """Milliseconds of the last check."""
        check = (self._homelab.data or {}).get(self._key)
        return check.response_time_ms if check else None


class HomelabProblemsSensor(HomelabEntity, SensorEntity):
    """Number of guests with a problem, on the cluster device."""

    _attr_state_class = SensorStateClass.MEASUREMENT

    def __init__(self, config_entry: ConfigEntry) -> None:
        """Initialize on the domain coordinator; also follows every guest."""
        super().__init__(
            config_entry.runtime_data[HOMELAB],
            config_entry,
            f"{config_entry.entry_id}_homelab_problems",
            "problems",
        )
        self._attr_device_info = DeviceInfo(identifiers={(DOMAIN, f"{config_entry.entry_id}_cluster")})

    def _suggested_object_id(self) -> str | None:
        nodes = selected(self._config_entry, "nodes", None)
        return slugify(f"homelab {' '.join(nodes) or self._config_entry.title} problems")

    @property
    def available(self) -> bool:
        """Always: it reports on others."""
        return True

    async def async_added_to_hass(self) -> None:
        """Follow every guest coordinator too."""
        await super().async_added_to_hass()
        coordinators = self._config_entry.runtime_data[COORDINATORS]
        for api_category, vmid in tracked_guests(self._config_entry):
            self.async_on_remove(coordinators[f"{api_category}_{vmid}"].async_add_listener(self._handle_coordinator_update))

    @property
    def _statuses(self) -> list[dict[str, Any]]:
        return [guest_status(self.hass, self._config_entry, cat, vmid) for cat, vmid in tracked_guests(self._config_entry)]

    @property
    def native_value(self) -> int:
        """Number of problems."""
        return sum(1 for status in self._statuses if status["problem"])

    @property
    def extra_state_attributes(self) -> dict[str, Any]:
        """List of the problems and the total."""
        statuses = self._statuses
        problems = [
            {k: s[k] for k in ("service_name", "type", "vmid", "problem", "http_status", "url", "message")}
            for s in statuses
            if s["problem"]
        ]
        return {"total": len(statuses), "problems": problems}


# ---------------------------------------------------------------------------
# Plattform-Helfer, von binary_sensor.py und sensor.py aufgerufen
# ---------------------------------------------------------------------------
def homelab_binary_sensors(config_entry: ConfigEntry, only: tuple[str, str] | None = None) -> list[BinarySensorEntity]:
    """Service sensor for every guest, domain sensor where a domain is set."""
    if HOMELAB not in config_entry.runtime_data:
        return []
    urls = config_entry.runtime_data[HOMELAB].urls
    entities: list[BinarySensorEntity] = []
    for api_category, vmid in tracked_guests(config_entry, only):
        entities.append(HomelabServiceBinarySensor(config_entry, api_category, vmid))
        if f"{api_category}_{vmid}" in urls:
            entities.append(HomelabDomainBinarySensor(config_entry, api_category, vmid))
    return entities


def homelab_sensors(config_entry: ConfigEntry, only: tuple[str, str] | None = None) -> list[SensorEntity]:
    """Response time where a domain is set; the problems sensor once per entry."""
    if HOMELAB not in config_entry.runtime_data:
        return []
    urls = config_entry.runtime_data[HOMELAB].urls
    entities: list[SensorEntity] = [
        HomelabResponseTimeSensor(config_entry, api_category, vmid)
        for api_category, vmid in tracked_guests(config_entry, only)
        if f"{api_category}_{vmid}" in urls
    ]
    if only is None:
        entities.append(HomelabProblemsSensor(config_entry))
    return entities


# ---------------------------------------------------------------------------
# Optionen: Domains je Gast und Einstellungen der Prüfung
# ---------------------------------------------------------------------------
_LABEL_RE = re.compile(r"^(QEMU|LXC) .* \((\d+)\)$")


def _guest_labels(config_entry: ConfigEntry) -> dict[str, str]:
    """`LXC jellyfin (100)` -> `lxc_100`, sorted like the Proxmox tree."""
    coordinators = config_entry.runtime_data.get(COORDINATORS, {}) if hasattr(config_entry, "runtime_data") else {}
    labels: dict[str, str] = {}
    for api_category, key in GUEST_TYPES:
        for vmid in sorted(selected(config_entry, key, None), key=lambda v: int(v) if v.isdigit() else 0):
            coordinator = coordinators.get(f"{api_category}_{vmid}")
            name = getattr(getattr(coordinator, "data", None), "name", None) or "?"
            labels[f"{api_category.upper()} {name} ({vmid})"] = f"{api_category}_{vmid}"
    return labels


async def _async_save(flow: OptionsFlow, options: dict[str, Any]) -> FlowResult:
    """Save like the other option steps: update, reload, done."""
    flow.hass.config_entries.async_update_entry(flow.config_entry, options=options)
    await flow.hass.config_entries.async_reload(flow.config_entry.entry_id)
    return flow.async_abort(reason="changes_successful")


async def async_step_homelab_domains(flow: OptionsFlow, user_input: dict[str, Any] | None) -> FlowResult:
    """One optional text field per guest: its domain or URL."""
    config_entry = flow.config_entry
    if getattr(config_entry, "runtime_data", None) is None:
        return flow.async_abort(reason="homelab_not_loaded")
    labels = _guest_labels(config_entry)
    current = guest_domains(config_entry)
    if user_input is not None:
        domains = {
            key: normalize_url(user_input.get(label))
            for label, key in labels.items()
            if str(user_input.get(label) or "").strip()
        }
        return await _async_save(flow, {**config_entry.options, CONF_HOMELAB_DOMAINS: domains})
    schema = {
        vol.Optional(label, description={"suggested_value": current.get(key, "")}): selector.TextSelector()
        for label, key in labels.items()
    }
    return flow.async_show_form(step_id="homelab_domains", data_schema=vol.Schema(schema))


async def async_step_homelab_settings(flow: OptionsFlow, user_input: dict[str, Any] | None) -> FlowResult:
    """Interval, timeout, failure threshold, certificate check."""
    config_entry = flow.config_entry
    if user_input is not None:
        return await _async_save(flow, {**config_entry.options, **user_input})
    options = homelab_options(config_entry)

    def number(minimum: int, maximum: int, unit: str | None = None) -> selector.NumberSelector:
        return selector.NumberSelector(
            selector.NumberSelectorConfig(min=minimum, max=maximum, step=1, mode=selector.NumberSelectorMode.BOX, unit_of_measurement=unit)
        )

    schema = vol.Schema(
        {
            vol.Required(CONF_HOMELAB_INTERVAL, default=options[CONF_HOMELAB_INTERVAL]): vol.All(number(15, 3600, "s"), vol.Coerce(int)),
            vol.Required(CONF_HOMELAB_TIMEOUT, default=options[CONF_HOMELAB_TIMEOUT]): vol.All(number(2, 60, "s"), vol.Coerce(int)),
            vol.Required(CONF_HOMELAB_THRESHOLD, default=options[CONF_HOMELAB_THRESHOLD]): vol.All(number(1, 10), vol.Coerce(int)),
            vol.Required(CONF_HOMELAB_VERIFY_SSL, default=options[CONF_HOMELAB_VERIFY_SSL]): selector.BooleanSelector(),
        }
    )
    return flow.async_show_form(step_id="homelab_settings", data_schema=schema)


@callback
def homelab_guest_key(api_category: ProxmoxType, resource_id: str) -> tuple[str, str] | None:
    """Key for discovered guests, None for anything else."""
    if api_category in (ProxmoxType.QEMU, ProxmoxType.LXC):
        return (api_category, str(resource_id))
    return None
