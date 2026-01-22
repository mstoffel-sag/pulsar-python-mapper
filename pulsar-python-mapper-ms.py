#!/usr/bin/env python3
"""
Cumulocity Microservice with Apache Pulsar Integration
"""
from datetime import datetime
import os
import logging
import time
import json
import threading
from typing import Optional
from dotenv import load_dotenv
import pulsar
from c8y_api import CumulocityApi
from c8y_api.model import Device, Measurement
from c8y_api.app import MultiTenantCumulocityApp
from flask import Flask, jsonify
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
        load_dotenv()
        self.c8yapp: Optional[MultiTenantCumulocityApp] = None
        self._running = False
        self.pulsar_clients = {}  # tenant_id -> {'client': pulsar.Client, 'consumer': pulsar.Consumer, 'c8y_client': CumulocityApi}
        
    def initialize_cumulocity(self):
        """Initialize Cumulocity connection"""
        try:
            # Get Cumulocity credentials from environment
            self.c8yapp = MultiTenantCumulocityApp()
            logging.info("CumulocityApp initialized.")
            c8y_bootstrap = self.c8yapp.bootstrap_instance
            logging.info(f"Bootstrap: {c8y_bootstrap.base_url}, Tenant: {c8y_bootstrap.tenant_id}, User:{c8y_bootstrap.username}")
            #self.add_subscriber(c8y_bootstrap)
            for subscriber_tenant_id in self.c8yapp.get_subscribers():
                logging.info(f"Adding subscriber for tenant: {subscriber_tenant_id}")
                self.add_subscriber(self.c8yapp.get_tenant_instance(subscriber_tenant_id))
               
            
        except Exception as e:
            logger.error(f"Failed to connect to Cumulocity: {e}")
            raise
            
    def add_subscriber(self, c8y_api: CumulocityApi):
        logging.info(f"Subscriber: {c8y_api.tenant_id} creating pulsar connection")
        c8y_client = c8y_api
        pulsar_client, consumer = self.initialize_pulsar(c8y_api)
        self.pulsar_clients[c8y_api.tenant_id] = {
            'client': pulsar_client,
            'consumer': consumer,
            'c8y_client': c8y_client
        }

    def message_listener(self, tenant_id: str, c8y_api: CumulocityApi, consumer, message):
        """Callback function for processing incoming Pulsar messages for a specific tenant"""
        try:
            # Get raw message data
            propertys = message.properties()
            client_id = propertys.get('clientID', None)
            topic = propertys.get('topic', None)

            # Filter message by topic. You could also use a regular expression here for wildcard matching.
            if not topic == 'mytopic':
                logger.info(f"Tenant {tenant_id}: Ignoring message from topic: {topic}")
                consumer.acknowledge(message)
                return
            else:         
                # Process the message
                self.process_message(tenant_id, c8y_api, consumer, topic, client_id, message)
                # Acknowledge the message
                consumer.acknowledge(message) 

        except Exception as e:
            consumer.acknowledge(message)
            logger.error(f"Tenant {tenant_id}: Error processing message: {e}")

    def process_message(self, tenant_id: str, c8y_api: CumulocityApi, consumer, topic: str, client_id: str, message: any):
        """Process incoming message and send to Cumulocity for a specific tenant"""
        try:
            raw_data = message.data()
            logger.info(f"Raw message (length={len(raw_data)}): {raw_data[:200]}")
            
            # Decode message
            decoded_message = raw_data.decode('utf-8')
            try:
                message_data = json.loads(decoded_message)
            except json.JSONDecodeError:
                logger.error(f"Tenant {tenant_id}: Failed to decode JSON message: {decoded_message}")
                consumer.acknowledge(message)
                return

            # Now we have the challenge to check whether the device already exists in Cumulocity. If not, we have create it. 
            # We will use the the identity api (externalId) for that. The externalId has a type we will use the default type c8y_Serial for that.
            # We also assume that the clientID from Pulsar is the unique identifier for the device. If the unique identifier is located somewhere else
            # E.g. in the topic of payload you have to adjust the code accordingly.

            if client_id:
                external_id = client_id
            else:
                logger.error(f"Tenant {tenant_id}: No clientID property found in message")
                consumer.acknowledge(message)
                return
            
            # Check if device exists
            device = self.get_device(c8y_api, external_id)
            if device is None:
                logger.error(f"Tenant {tenant_id}: Device with external ID {external_id} could not be created or retrieved.")
                consumer.acknowledge(message)
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
            c8y_api.measurements.create(measurement)
            logger.info(f"Tenant {tenant_id}: Sent measurement for device {device.id}  {pformat(measurement.to_json())}")
                
        except Exception as e:
            logger.error(f"Tenant {tenant_id}: Failed to process message: {e}")
            consumer.acknowledge(message)

    def get_device(self, c8y_client: CumulocityApi, external_id: str) -> Optional[Device]:
        """Retrieve or create device in Cumulocity by external ID"""

        device = None
        try:
            device = c8y_client.identity.get_object(external_id, self.external_id_type)
        except Exception as e:
            logger.error(f"No device found retrieving or creating device: {e}")
        if device is None:
            # Create new device
            try:
                device = Device(c8y_client, type=self.device_type, name=f'{self.device_prefix}{external_id}').create()
                c8y_client.identity.create(external_id, self.external_id_type, device.id)
                logger.info(f"Created new device with ID: {device.id} for external ID: {external_id}")
                return device
            except Exception as e:
                logger.error(f"Failed to create device for external ID {external_id}: {e}")
                # @todo handle creation failure, what if device creation works but external ID creation fails?
                return None
        return device

    def initialize_pulsar(self, c8y_api: CumulocityApi):
        """Initialize Apache Pulsar connection with message listener for a specific tenant"""
        try:
            c8y_tenant = c8y_api.tenant_id
            pulsar_url = os.environ.get('C8Y_BASEURL_PULSAR')
            pulsar_topic = f'persistent://{c8y_api.tenant_id}/mqtt/from-device'
            subscription_name = f'{c8y_tenant}_pulsar-python-mapper'

            logger.info(f"Connecting to Pulsar for tenant {c8y_tenant} at {pulsar_url}")
            
            # Configure authentication using C8Y credentials
            auth_params = {}
            logger.info(f"Using basic authentication for user: {c8y_api.auth.username}")
            auth_params['authentication'] = pulsar.AuthenticationBasic(
                c8y_api.auth.username,
                c8y_api.auth.password
            )
            
            pulsar_client = pulsar.Client(
                pulsar_url,
                **auth_params
            )
            
            # Create tenant-specific message listener
            def tenant_message_listener(consumer, message):
                self.message_listener(c8y_tenant, c8y_api, consumer, message)
            
            # Subscribe with message listener for event-driven processing
            consumer = pulsar_client.subscribe(
                topic=pulsar_topic,
                subscription_name=subscription_name,
                consumer_type=pulsar.ConsumerType.Shared,
                message_listener=tenant_message_listener
            )
            
            logger.info(f"Successfully subscribed to Pulsar topic: {pulsar_topic} for tenant {c8y_tenant}")
            
            return pulsar_client, consumer
            
        except Exception as e:
            logger.error(f"Failed to connect to Pulsar for tenant {c8y_api.tenant_id}: {e}")
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
        
        # Close all Pulsar connections
        for tenant_id, connections in self.pulsar_clients.items():
            logger.info(f"Closing Pulsar connections for tenant {tenant_id}")
            try:
                if connections['consumer']:
                    connections['consumer'].close()
                if connections['client']:
                    connections['client'].close()
            except Exception as e:
                logger.error(f"Error closing Pulsar connections for tenant {tenant_id}: {e}")
        
        self.pulsar_clients.clear()
        logger.info("Bridge stopped")


# Global reference to the bridge instance for health checks
bridge_instance = None

# Flask app for health endpoint
app = Flask(__name__)

@app.route('/health', methods=['GET'])
def health_check():
    """Simple health check endpoint for Cumulocity microservice monitoring"""
    if bridge_instance and bridge_instance._running:
        return jsonify({"status": "healthy"}), 200
    return jsonify({"status": "unhealthy"}), 503


def run_health_server():
    """Run the Flask health server in a separate thread"""
    app.run(host='0.0.0.0', port=80, debug=False, use_reloader=False)


def main():
    """Main entry point"""
    global bridge_instance
    
    logger.info("Initializing Pulsar-Cumulocity microservice")
    
    bridge_instance = PulsarCumulocityBridge()
    
    try:
        # Initialize connections
        bridge_instance.initialize_cumulocity()
        
        # Start health server in background thread
        health_thread = threading.Thread(target=run_health_server, daemon=True)
        health_thread.start()
        
        # Start processing messages
        bridge_instance.start()
        
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        raise


if __name__ == '__main__':
    main()
