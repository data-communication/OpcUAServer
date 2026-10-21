import asyncio
import sys
import os

from dotenv import load_dotenv

from asyncua import Server, ua

from messaging.mqtt_service import MqttService
from opcua.opcua_climate_service import OpcUAClimateService

load_dotenv()

async def main():
    mqtt_service = MqttService(
        host=os.getenv("MQTT_HOST"), 
        port=int(os.getenv("MQTT_PORT")), 
        username=os.getenv("MQTT_USERNAME"), 
        password=os.getenv("MQTT_PASSWORD")
    )

    services = [
        OpcUAClimateService(mqtt_service)
    ]

    server = Server()
    await server.init()

    server.set_endpoint("opc.tcp://0.0.0.0:4840")
    server.set_server_name("OpcUaGateway")
    await server.set_application_uri("urn:jesten:OpcUaGateway")
    server.product_uri = "urn:jesten:OpcUaGateway"

    server.set_security_policy([ua.SecurityPolicyType.NoSecurity])
    server.set_identity_tokens([ua.AnonymousIdentityToken])

    namespace = await server.register_namespace("urn:jesten:devices")

    for service in services:
        await service.create_nodes(server, namespace, os.getenv("MQTT_TOPICS"))

    async with server:
        await mqtt_service.run()

if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.run(main(), loop_factory=asyncio.SelectorEventLoop)
    else:
        asyncio.run(main())