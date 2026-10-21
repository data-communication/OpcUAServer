import json
from asyncua import ua

class OpcUAClimateService:
    def __init__(self, mqtt_service):
        self.mqtt_service = mqtt_service
        self.devices = {}

    async def create_nodes(self, server, namespace, topics):
        self.namespace = namespace
        self.devices_folder = await server.nodes.objects.add_folder(namespace, "Devices")

        for topic in topics.split(";"):
            self.mqtt_service.register(topic, self.handle_message)

    async def _create_device_nodes(self, device_id: str):
        sub_device_folder = await self.devices_folder.add_folder(self.namespace, device_id)

        temperature_folder = await sub_device_folder.add_folder(
            self.namespace, "Temperature"
        )

        temperature_nodes = {
            "value": await temperature_folder.add_variable(
                self.namespace, "Value", 0.0
            ),
            "unit": await temperature_folder.add_variable(
                self.namespace, "Unit", "°C"
            ),
            "timestamp": await temperature_folder.add_variable(
                self.namespace, "Timestamp", ""
            ),
        }

        humidity_folder = await sub_device_folder.add_folder(
            self.namespace, "Humidity"
        )

        humidity_nodes = {
            "value": await humidity_folder.add_variable(
                self.namespace, "Value", 0.0
            ),
            "unit": await humidity_folder.add_variable(
                self.namespace, "Unit", "%"
            ),
            "timestamp": await humidity_folder.add_variable(
                self.namespace, "Timestamp", ""
            ),
        }

        self.devices[device_id] = {
            "temperature": temperature_nodes,
            "humidity": humidity_nodes,
        }

    async def handle_message(self, payload):
        try:
            message = json.loads(payload)
        except json.JSONDecodeError:
            print("Invalid JSON:", payload)
            return

        device_id = message.get("deviceId")
        measurements = message.get("measurements", [])
        timestamp = message.get("timestamp")

        if device_id not in self.devices:
            await self._create_device_nodes(device_id)

        output = f"{device_id}: {{"

        for m in measurements:
            m_type = m.get("type")
            m_value = m.get("value")
            m_unit = m.get("unit")
            m_timestamp = m.get("timestamp", timestamp)

            if m_type not in self.devices[device_id]:
                print(f"Unknown measurement type: {m_type}")
                continue

            nodes = self.devices[device_id][m_type]

            if m_value is not None:
                await nodes["value"].write_value(float(m_value))

            if m_unit is not None:
                await nodes["unit"].write_value(str(m_unit))

            if m_timestamp is not None:
                await nodes["timestamp"].write_value(str(m_timestamp))

            output += (
                f"{m_type.capitalize()}: {m_value} {m_unit}, "
                f"Timestamp: {m_timestamp}; "
            )

        output += "}"
        print(output)