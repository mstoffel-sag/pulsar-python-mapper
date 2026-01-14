#!/usr/bin/env python3
"""
Cumulocity Microservice with Apache Pulsar Integration
"""
from datetime import datetime
import os
import logging
import time
import json
from typing import Optional
import pulsar
from c8y_api import CumulocityApi
from c8y_api.model import Device, Measurement
from pprint import pformat, pprint


# Configure logging
log_level = os.getenv('LOG_LEVEL', 'INFO').upper()
logging.basicConfig(
    level=getattr(logging, log_level, logging.INFO),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class PulsarCumulocityBridge:
    """Bridge between Apache Pulsar and Cumulocity IoT"""
    external_id_type = 'c8y_Serial'
    device_prefix = 'MyDevice-'
    device_type = 'mqtt_pulsar_Device'
    def __init__(self):
        self.c8y_client: Optional[CumulocityApi] = None
        self.pulsar_client: Optional[pulsar.Client] = None
        self.consumer: Optional[pulsar.Consumer] = None
        self._running = False
        self.tenant_id = os.getenv('C8Y_TENANT', 'public')
        
    def initialize_cumulocity(self):
        """Initialize Cumulocity connection"""
        try:
            # Get Cumulocity credentials from environment
            c8y_base_url = os.getenv('C8Y_BASEURL', 'https://your-tenant.cumulocity.com')
            c8y_tenant = os.getenv('C8Y_TENANT')
            c8y_user = os.getenv('C8Y_USER')
            c8y_password = os.getenv('C8Y_PASSWORD')
            
            logger.info(f"Connecting to Cumulocity at {c8y_base_url}")
            
            self.c8y_client = CumulocityApi(
                base_url=c8y_base_url,
                tenant_id=c8y_tenant,
                username=c8y_user,
                password=c8y_password
            )
            
            # Test connection

            for d in self.c8y_client.inventory.select():
                logger.info(f"Found device #{d.id} '{d.name}', owned by {d.owner}")
                break
            logger.info("Successfully connected to Cumulocity")
            
        except Exception as e:
            logger.error(f"Failed to connect to Cumulocity: {e}")
            raise
    
    def message_listener(self, consumer, message):
        """Callback function for processing incoming Pulsar messages"""
        try:
            # Get raw message data
            propertys = message.properties()
            client_id = propertys.get('clientID', None)
            topic = propertys.get('topic', None)

            # Filter message by topic. You could also use a regular expression here for wildcard matching.
            if not topic == 'mytopic':
                logger.info(f"Ignoring message from topic: {topic}")
                consumer.acknowledge(message)
                return
            else:         
                # Process the message
                self.process_message(topic, client_id, message)
                # Acknowledge the message
                consumer.acknowledge(message) 

        except Exception as e:
            consumer.acknowledge(message)
            logger.error(f"Error processing message: {e}")

    def process_message(self, topic: str, client_id: str, message: any):
        """Process incoming message and send to Cumulocity"""
        try:
            raw_data = message.data()
            logger.debug(f"Raw message (length={len(raw_data)}): {raw_data[:200]}")
            
            # Decode message
            decoded_message = raw_data.decode('utf-8')
            try:
                message_data = json.loads(decoded_message)
            except json.JSONDecodeError:
                logger.error(f"Failed to decode JSON message: {decoded_message}")
                self.consumer.acknowledge(message)
                return

            # Now we have the challenge to check whether the device already exists in Cumulocity. If not, we have create it. 
            # We will use the externalId for that. The externalId has a type we will use the default type c8y_Serial for that.
            # We also assume that the clientID from Pulsar is the unique identifier for the device. If the unique identifier is located somewhere else
            # E.g. in the topic of payload you have to adjust the code accordingly.

            if client_id:
                external_id = client_id
            else:
                logger.error("No clientID property found in message")
                self.consumer.acknowledge(message)
                return
            
            # Check if device exists
            device = self.get_device(external_id)
            if device is None:
                logger.error(f"Device with external ID {external_id} could not be created or retrieved.")
                self.consumer.acknowledge(message)
                return
            
            #### We assume the message contains measurement data in a specific format like: 
            # {
            #   "timestamp": "2026-01-14T12:00:00Z",
            #   "temperature":  23.5,
            #   "pressure": 90
            # }
            time = datetime.fromisoformat(message_data.get('timestamp'))
            temperature = message_data.get('temperature')
            pressure = message_data.get('pressure')

            measurement = Measurement(type='TempPress', source=device.id,
                       time=time,
                       TempPress={'temperature': {'value': temperature, 'unit': '°C'},
                                'pressure': {'value': pressure, 'unit': 'kPa'}})
            self.c8y_client.measurements.create(measurement)
            logger.info(f"Sent measurement for device {device.id}  {pformat(measurement.to_json())}")
                
        except Exception as e:
            logger.error(f"Failed to process message: {e}")
            self.consumer.acknowledge(message)


    def get_device(self, external_id: str) -> Optional[Device]:
        """Retrieve or create device in Cumulocity by external ID"""

        device = None
        try:
            device = self.c8y_client.identity.get_object(external_id, self.external_id_type)
        except Exception as e:
            logger.error(f"No device found retrieving or creating device: {e}")
        if device is None:
            # Create new device
            try:
                device = Device(self.c8y_client, type=self.device_type, name=f'{self.device_prefix}{external_id}').create()
                self.c8y_client.identity.create(external_id, self.external_id_type, device.id)
                logger.info(f"Created new device with ID: {device.id} for external ID: {external_id}")
                return device
            except Exception as e:
                logger.error(f"Failed to create device for external ID {external_id}: {e}")
                # @todo handle creation failure, what if device creation works but external ID creation fails?
                return None
        return device


    def initialize_pulsar(self):
        """Initialize Apache Pulsar connection with message listener"""
        try:
            pulsar_url = os.getenv('C8Y_BASEURL_PULSAR')
            pulsar_topic = f'persistent://{self.tenant_id}/mqtt/from-device'
            subscription_name = 'pulsar-python-mapper'
            
            # Use Cumulocity credentials for Pulsar authentication
            c8y_user = os.getenv('C8Y_USER')
            c8y_tenant = os.getenv('C8Y_TENANT')
            c8y_password = os.getenv('C8Y_PASSWORD')
            
            logger.info(f"Connecting to Pulsar at {pulsar_url}")
            
            # Configure authentication using C8Y credentials
            auth_params = {}
            if c8y_user and c8y_tenant and c8y_password:
                pulsar_username = f"{c8y_tenant}/{c8y_user}"
                logger.info(f"Using basic authentication for user: {pulsar_username}")
                auth_params['authentication'] = pulsar.AuthenticationBasic(
                    pulsar_username,
                    c8y_password
                )
            
            self.pulsar_client = pulsar.Client(
                pulsar_url,
                **auth_params
            )
            
            # Subscribe with message listener for event-driven processing
            self.consumer = self.pulsar_client.subscribe(
                topic=pulsar_topic,
                subscription_name=subscription_name,
                consumer_type=pulsar.ConsumerType.Shared,
                message_listener=self.message_listener
            )
            
            logger.info(f"Successfully subscribed to Pulsar topic: {pulsar_topic} with message listener")
            
        except Exception as e:
            logger.error(f"Failed to connect to Pulsar: {e}")
            raise
    
    
    def start(self):
        """Start the message listener and keep the service running"""
        self._running = True
        logger.info("Pulsar-Cumulocity bridge is running with message listener")
        logger.info("Press Ctrl+C to stop")
        
        try:
            # Keep the service running - messages are processed by the listener
            while self._running:
                time.sleep(1)
                    
        except KeyboardInterrupt:
            logger.info("Received interrupt signal")
        finally:
            self.stop()
    
    def stop(self):
        """Stop the bridge and cleanup resources"""
        self._running = False
        logger.info("Stopping Pulsar-Cumulocity bridge")
        
        if self.consumer:
            self.consumer.close()
        if self.pulsar_client:
            self.pulsar_client.close()
        
        logger.info("Bridge stopped")


def main():
    """Main entry point"""
    logger.info("Initializing Pulsar-Cumulocity microservice")
    
    bridge = PulsarCumulocityBridge()
    
    try:
        # Initialize connections
        bridge.initialize_cumulocity()
        bridge.initialize_pulsar()
        
        # Start processing messages
        bridge.start()
        
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        raise


if __name__ == '__main__':
    main()
