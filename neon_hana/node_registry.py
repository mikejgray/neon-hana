# NEON AI (TM) SOFTWARE, Software Development Kit & Application Development System
# All trademark and other rights reserved by their respective owners
# Copyright 2008-2026 Neongecko.com Inc.
# BSD-3
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
# 1. Redistributions of source code must retain the above copyright notice,
#    this list of conditions and the following disclaimer.
# 2. Redistributions in binary form must reproduce the above copyright notice,
#    this list of conditions and the following disclaimer in the documentation
#    and/or other materials provided with the distribution.
# 3. Neither the name of the copyright holder nor the names of its
#    contributors may be used to endorse or promote products derived from this
#    software without specific prior written permission.
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
# THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
# PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR
# CONTRIBUTORS  BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
# EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
# PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA,
# OR PROFITS;  OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
# LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
# NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
# SOFTWARE,  EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

import json
import os

from datetime import datetime, timezone
from threading import RLock
from typing import Dict, List, Optional

from neon_data_models.enum import NotificationScope
from ovos_utils.log import LOG

_REGISTRY_FORMAT_VERSION = 1


def default_registry_path() -> str:
    """
    Default registry location. The XDG config home is the only host-mounted
    directory in the HANA container, so the registry lives there to survive
    container recreation (same reasoning as the persisted `hub_id`).
    """
    xdg_config = os.environ.get("XDG_CONFIG_HOME",
                                os.path.expanduser("~/.config"))
    return os.path.join(xdg_config, "neon", "hana", "node_registry.json")


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class NodeRegistry:
    """
    Persistent record of every Node that has connected to this HANA instance:
    `node_id -> {user_id, node_name, capabilities, first_seen, last_seen,
    last_catch_up}`. Identity must outlive a websocket session so that a Node
    that is offline stays a known notification target.

    Ownership (`user_id`) comes from JWT claims at connect and is
    authoritative. `node_name` and `capabilities` are optional self-description
    from `node.hello`. `last_seen`/`last_catch_up` are diagnostic timestamps,
    not per-notification delivery receipts.

    Notification routing consumes only `resolve` and `owner`; the registry can
    grow for other purposes without touching that interface.
    """

    def __init__(self, path: str):
        self._path = path
        self._lock = RLock()
        self._nodes: Dict[str, dict] = self._load()

    @property
    def path(self) -> str:
        return self._path

    def resolve(self, scope: NotificationScope,
                target: Optional[str]) -> List[str]:
        """
        Expand a notification address to concrete node_ids.
        @param scope: scope the notification is addressed to
        @param target: node_id (CLIENT), user_id (USER) or None (GLOBAL)
        @return: sorted node_ids the notification is addressed to
        """
        with self._lock:
            if scope == NotificationScope.GLOBAL:
                return sorted(self._nodes)
            if scope == NotificationScope.USER:
                return sorted(node_id for node_id, record in self._nodes.items()
                              if target and record.get("user_id") == target)
            if scope == NotificationScope.CLIENT:
                return [target] if target in self._nodes else []
        LOG.warning("Unknown notification scope: %s", scope)
        return []

    def owner(self, node_id: str) -> Optional[str]:
        """
        @param node_id: Node to look up
        @return: user_id that authenticated `node_id`, or None if unknown
        """
        with self._lock:
            record = self._nodes.get(node_id)
        return record.get("user_id") if record else None

    def get(self, node_id: str) -> Optional[dict]:
        """
        @param node_id: Node to look up
        @return: copy of the stored record, or None if unknown
        """
        with self._lock:
            record = self._nodes.get(node_id)
        return dict(record) if record else None

    def upsert_connect(self, node_id: str, user_id: str) -> dict:
        """
        Record a Node connection. Called after JWT validation, so `user_id`
        is authoritative and replaces any prior owner.
        @param node_id: `client_id` claim of the connecting Node
        @param user_id: `sub` claim of the connecting Node
        @return: copy of the stored record
        """
        now = _utc_now()
        with self._lock:
            record = self._nodes.get(node_id)
            if record is None:
                record = {"user_id": user_id,
                          "node_name": "",
                          "capabilities": None,
                          "first_seen": now,
                          "last_seen": now,
                          "last_catch_up": None}
                self._nodes[node_id] = record
            else:
                record["user_id"] = user_id
                record["last_seen"] = now
            self._save()
            return dict(record)

    def update_hello(self, node_id: str, node_name: Optional[str] = None,
                     capabilities: Optional[dict] = None) -> bool:
        """
        Store `node.hello` self-description for a known Node.
        @param node_id: Node that sent the hello
        @param node_name: user-facing Node name, if advertised
        @param capabilities: advertised capability map, if any
        @return: False if the Node has never connected (hello ignored)
        """
        with self._lock:
            record = self._nodes.get(node_id)
            if record is None:
                LOG.debug("Ignoring hello for unregistered node: %s", node_id)
                return False
            if node_name is not None:
                record["node_name"] = node_name
            if capabilities is not None:
                record["capabilities"] = dict(capabilities)
            self._save()
            return True

    def touch_catch_up(self, node_id: str) -> bool:
        """
        Record a successful `GET /notifications` catch-up for diagnostics.
        @param node_id: Node that fetched its notifications
        @return: False if the Node is unknown
        """
        with self._lock:
            record = self._nodes.get(node_id)
            if record is None:
                return False
            now = _utc_now()
            record["last_catch_up"] = now
            record["last_seen"] = now
            self._save()
            return True

    def _load(self) -> Dict[str, dict]:
        try:
            with open(self._path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except FileNotFoundError:
            return {}
        except (OSError, ValueError) as e:
            LOG.error("Node registry at %s is unreadable (%s); starting with "
                      "an empty registry", self._path, e)
            return {}
        nodes = data.get("nodes") if isinstance(data, dict) else None
        if not isinstance(nodes, dict):
            LOG.error("Node registry at %s has an unexpected layout; "
                      "starting with an empty registry", self._path)
            return {}
        return nodes

    def _save(self):
        """
        Atomic write (temp file + `os.replace`). Callers hold `self._lock`.
        A write failure is logged, not raised: a Node connection must not fail
        because the registry directory is read-only.
        """
        tmp_path = f"{self._path}.tmp"
        try:
            directory = os.path.dirname(self._path)
            if directory:
                os.makedirs(directory, exist_ok=True)
            with open(tmp_path, "w", encoding="utf-8") as f:
                json.dump({"version": _REGISTRY_FORMAT_VERSION,
                           "nodes": self._nodes}, f, indent=2, sort_keys=True)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp_path, self._path)
        except OSError as e:
            LOG.error("Failed to persist node registry to %s: %s",
                      self._path, e)
