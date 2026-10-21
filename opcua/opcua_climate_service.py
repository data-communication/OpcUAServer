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

        device_nodes = {}
        device_nodes = {
            "device_id": await sub_device_folder.add_variable(self.namespace, "DeviceId", device_id),
            "timestamp": await sub_device_folder.add_variable(self.namespace, "Timestamp", ""),

            "temperature": {
                "value": await sub_device_folder.add_variable(self.namespace, "TemperatureValue", 0.0),
                "unit": await sub_device_folder.add_variable(self.namespace, "TemperatureUnit", "°C"),
            },

            "humidity": {
                "value": await sub_device_folder.add_variable(self.namespace, "HumidityValue", 0.0),
                "unit": await sub_device_folder.add_variable(self.namespace, "HumidityUnit", "%"),
            }
        }

        self.devices[device_id] = device_nodes

    async def handle_message(self, payload):
        try:
            message = json.loads(payload)
        except json.JSONDecodeError:
            print("Invalid JSON:", payload)
            return

        device_id = message.get("deviceId")
        measurements = message.get("measurements", [])
        timestamp = message.get("timestamp")

        if device_id and device_id not in self.devices:
            await self._create_device_nodes(device_id)

        if device_id:
            await self.devices[device_id]["device_id"].write_value(device_id)

        if timestamp:
            await self.devices[device_id]["timestamp"].write_value(timestamp)


        output = f"{device_id}: "
        output += "{ "

        for m in measurements:
            m_type = m.get("type")
            m_value = m.get("value")
            m_unit = m.get("unit")

            if m_type not in self.devices[device_id]:
                print(f"Unknown measurement type: {m_type}")
                continue

            if m_value is not None:
                await self.devices[device_id][m_type]["value"].write_value(float(m_value))

            if m_unit is not None:
                await self.devices[device_id][m_type]["unit"].write_value(m_unit)

            output += f"{m_type.capitalize()}: {m_value}{m_unit}, "
        output += f"Timestamp: {timestamp}"
        output += " }"
        print(output)