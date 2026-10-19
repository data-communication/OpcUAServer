import json
from asyncua import ua

class OpcUAClimateService:
    def __init__(self, mqtt_service):
        self.mqtt_service = mqtt_service
        self.measurement_nodes = {}

    async def create_nodes(self, server, namespace, topic):
        climate = await server.nodes.objects.add_folder(namespace, "Climate")

        self.device_id = await climate.add_variable(namespace, "DeviceId", "")
        self.timestamp = await climate.add_variable(namespace, "Timestamp", "")

        self.measurement_nodes["temperature"] = {
            "value": await climate.add_variable(namespace, "TemperatureValue", 0.0),
            "unit": await climate.add_variable(namespace, "TemperatureUnit", "°C"),
        }

        self.measurement_nodes["humidity"] = {
            "value": await climate.add_variable(namespace, "HumidityValue", 0.0),
            "unit": await climate.add_variable(namespace, "HumidityUnit", "%"),
        }

        self.mqtt_service.register(topic, self.handle_message)

    async def handle_message(self, payload):
        try:
            message = json.loads(payload)
        except json.JSONDecodeError:
            print("Invalid JSON:", payload)
            return

        device_id = message.get("deviceId")
        measurements = message.get("measurements", [])
        timestamp = message.get("timestamp")

        if device_id:
            await self.device_id.write_value(device_id)

        if timestamp:
            await self.timestamp.write_value(timestamp)

        for m in measurements:
            m_type = m.get("type")
            m_value = m.get("value")
            m_unit = m.get("unit")

            if m_type not in self.measurement_nodes:
                print(f"Unknown measurement type: {m_type}")
                continue

            if m_value is not None:
                await self.measurement_nodes[m_type]["value"].write_value(float(m_value))

            if m_unit is not None:
                await self.measurement_nodes[m_type]["unit"].write_value(m_unit)

            print(f"{m_type}: {m_value} {m_unit}")
