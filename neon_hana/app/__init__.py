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

from fastapi import FastAPI, Response

from neon_hana.app.dependencies import client_manager, jwt_bearer, mq_connector  # noqa: F401
from neon_hana.app.routers.api_proxy import proxy_route
from neon_hana.app.routers.assist import assist_route
from neon_hana.app.routers.brainforge import bf_route
from neon_hana.app.routers.llm import llm_route
from neon_hana.app.routers.mq_backend import mq_route
from neon_hana.app.routers.auth import auth_route
from neon_hana.app.routers.user import user_route
from neon_hana.app.routers.util import util_route
from neon_hana.app.routers.hub import hub_route
from neon_hana.app.routers.node_server import node_route, socket_api
from neon_hana.app.routers.notifications import notifications_route
from neon_hana.version import __version__


def create_app(config: dict):
    title = config.get("fastapi_title") or "HANA: HTTP API for Neon Applications"
    summary = config.get("fastapi_summary") or ""
    version = __version__
    app = FastAPI(title=title, summary=summary, version=version)
    app.include_router(auth_route)
    app.include_router(assist_route)
    app.include_router(proxy_route)
    app.include_router(mq_route)
    app.include_router(llm_route)
    app.include_router(util_route)
    app.include_router(node_route)
    app.include_router(user_route)
    app.include_router(bf_route)
    app.include_router(hub_route)
    app.include_router(notifications_route)

    @app.get("/status")
    def get_status():
        """
        Get service status
        """
        if not client_manager.check_health():
            return Response(status_code=500, content="Client manager is not healthy")
        if not mq_connector.check_health():
            return Response(status_code=500, content="MQ Connector is not healthy")
        if socket_api and not socket_api.check_health():
            return Response(status_code=500, content="Websocket API is not healthy")
        return Response(status_code=200, content="Ready")

    return app
