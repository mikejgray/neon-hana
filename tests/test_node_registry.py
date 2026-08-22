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
import tempfile
import unittest

from unittest.mock import patch

from neon_data_models.enum import NotificationScope

from neon_hana.node_registry import NodeRegistry, default_registry_path


class TestNodeRegistry(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        # Nested path: the registry must create missing parent directories
        self.path = os.path.join(tmp.name, "nested", "node_registry.json")
        self.registry = NodeRegistry(self.path)

    def _seed(self):
        self.registry.upsert_connect("phone-a", "user-a")
        self.registry.upsert_connect("tablet-a", "user-a")
        self.registry.upsert_connect("phone-b", "user-b")

    def test_upsert_connect_creates_record(self):
        record = self.registry.upsert_connect("phone-a", "user-a")
        self.assertEqual(record["user_id"], "user-a")
        self.assertEqual(record["node_name"], "")
        self.assertIsNone(record["capabilities"])
        self.assertIsNone(record["last_catch_up"])
        self.assertEqual(record["first_seen"], record["last_seen"])
        self.assertTrue(os.path.isfile(self.path))

    def test_upsert_connect_updates_owner_and_last_seen(self):
        first = self.registry.upsert_connect("phone-a", "user-a")
        with patch("neon_hana.node_registry._utc_now",
                   return_value="2026-08-21T12:00:00+00:00"):
            second = self.registry.upsert_connect("phone-a", "user-b")
        # The JWT at connect is authoritative for ownership
        self.assertEqual(second["user_id"], "user-b")
        self.assertEqual(second["first_seen"], first["first_seen"])
        self.assertEqual(second["last_seen"], "2026-08-21T12:00:00+00:00")

    def test_persistence_round_trip(self):
        self._seed()
        self.registry.update_hello("phone-a", "Kitchen Phone",
                                   {"launch_camera_app": True})
        self.registry.touch_catch_up("tablet-a")

        reloaded = NodeRegistry(self.path)
        for node_id in ("phone-a", "tablet-a", "phone-b"):
            self.assertEqual(reloaded.get(node_id), self.registry.get(node_id))
        self.assertEqual(reloaded.get("phone-a")["node_name"],
                         "Kitchen Phone")
        self.assertIsNotNone(reloaded.get("tablet-a")["last_catch_up"])
        # Atomic write leaves no temp file behind and a versioned layout
        self.assertFalse(os.path.exists(f"{self.path}.tmp"))
        with open(self.path, encoding="utf-8") as f:
            on_disk = json.load(f)
        self.assertEqual(on_disk["version"], 1)
        self.assertEqual(set(on_disk["nodes"]),
                         {"phone-a", "tablet-a", "phone-b"})

    def test_get_returns_copy(self):
        self._seed()
        record = self.registry.get("phone-a")
        record["user_id"] = "mallory"
        self.assertEqual(self.registry.owner("phone-a"), "user-a")
        self.assertIsNone(self.registry.get("unknown"))

    def test_resolve_client_scope(self):
        self._seed()
        self.assertEqual(self.registry.resolve(NotificationScope.CLIENT,
                                               "phone-a"), ["phone-a"])
        self.assertEqual(self.registry.resolve(NotificationScope.CLIENT,
                                               "laptop-unknown"), [])
        self.assertEqual(self.registry.resolve(NotificationScope.CLIENT,
                                               None), [])

    def test_resolve_user_scope(self):
        self._seed()
        self.assertEqual(self.registry.resolve(NotificationScope.USER, "user-a"),
                         ["phone-a", "tablet-a"])
        self.assertEqual(self.registry.resolve(NotificationScope.USER, "user-b"),
                         ["phone-b"])
        self.assertEqual(self.registry.resolve(NotificationScope.USER,
                                               "nobody"), [])
        self.assertEqual(self.registry.resolve(NotificationScope.USER, None),
                         [])

    def test_resolve_global_scope(self):
        self._seed()
        self.assertEqual(self.registry.resolve(NotificationScope.GLOBAL, None),
                         ["phone-a", "phone-b", "tablet-a"])
        # `target` is ignored for GLOBAL
        self.assertEqual(self.registry.resolve(NotificationScope.GLOBAL,
                                               "user-a"),
                         ["phone-a", "phone-b", "tablet-a"])

    def test_resolve_accepts_the_int_wire_values(self):
        # `NotificationScope` is an IntEnum, so a decoded wire value compares
        # equal to the enum member without conversion
        self._seed()
        self.assertEqual(self.registry.resolve(0, "phone-b"), ["phone-b"])
        self.assertEqual(self.registry.resolve(1, "user-b"), ["phone-b"])
        self.assertEqual(len(self.registry.resolve(2, None)), 3)

    def test_resolve_unknown_scope(self):
        self._seed()
        self.assertEqual(self.registry.resolve(99, "phone-a"), [])

    def test_owner(self):
        self._seed()
        self.assertEqual(self.registry.owner("phone-a"), "user-a")
        self.assertEqual(self.registry.owner("phone-b"), "user-b")
        self.assertIsNone(self.registry.owner("laptop-unknown"))

    def test_owner_gating_data(self):
        # The data notification routing relies on: every node a USER scope
        # resolves to is owned by that user
        self._seed()
        for node_id in self.registry.resolve(NotificationScope.USER, "user-a"):
            self.assertEqual(self.registry.owner(node_id), "user-a")

    def test_update_hello(self):
        self.assertFalse(self.registry.update_hello("phone-a", "Phone", {}))
        self._seed()
        self.assertTrue(self.registry.update_hello(
            "phone-a", "Kitchen Phone", {"launch_camera_app": True}))
        record = self.registry.get("phone-a")
        self.assertEqual(record["node_name"], "Kitchen Phone")
        self.assertEqual(record["capabilities"], {"launch_camera_app": True})
        # Partial update keeps the other field
        self.assertTrue(self.registry.update_hello("phone-a", "Den Phone"))
        record = self.registry.get("phone-a")
        self.assertEqual(record["node_name"], "Den Phone")
        self.assertEqual(record["capabilities"], {"launch_camera_app": True})

    def test_touch_catch_up(self):
        self.assertFalse(self.registry.touch_catch_up("phone-a"))
        self._seed()
        with patch("neon_hana.node_registry._utc_now",
                   return_value="2026-08-21T13:00:00+00:00"):
            self.assertTrue(self.registry.touch_catch_up("phone-a"))
        record = self.registry.get("phone-a")
        self.assertEqual(record["last_catch_up"], "2026-08-21T13:00:00+00:00")
        self.assertEqual(record["last_seen"], "2026-08-21T13:00:00+00:00")

    def test_corrupt_file_starts_empty(self):
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        with open(self.path, "w", encoding="utf-8") as f:
            f.write("{not json")
        registry = NodeRegistry(self.path)
        self.assertEqual(registry.resolve(NotificationScope.GLOBAL, None), [])
        # The registry recovers: the next write produces a valid file
        registry.upsert_connect("phone-a", "user-a")
        self.assertEqual(NodeRegistry(self.path).owner("phone-a"), "user-a")

    def test_unexpected_layout_starts_empty(self):
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(["not", "a", "mapping"], f)
        registry = NodeRegistry(self.path)
        self.assertEqual(registry.resolve(NotificationScope.GLOBAL, None), [])

    def test_save_failure_keeps_memory_state(self):
        # Parent "directory" is a regular file, so the write must fail
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        blocker = os.path.join(tmp.name, "blocker")
        with open(blocker, "w", encoding="utf-8") as f:
            f.write("")
        registry = NodeRegistry(os.path.join(blocker, "node_registry.json"))
        registry.upsert_connect("phone-a", "user-a")
        self.assertEqual(registry.owner("phone-a"), "user-a")

    def test_default_registry_path_uses_xdg_config_home(self):
        with patch.dict(os.environ, {"XDG_CONFIG_HOME": "/config"}):
            self.assertEqual(default_registry_path(),
                             os.path.join("/config", "neon", "hana",
                                          "node_registry.json"))


if __name__ == '__main__':
    unittest.main()
