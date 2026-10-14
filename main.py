import json
import time
import threading
import ssl
import paho.mqtt.client as mqtt
from datetime import datetime


# ============================================================
# TIMESTAMP HELPER
# ============================================================

def log(message):
    timestamp = datetime.now().astimezone().isoformat(
        sep=" ", timespec="milliseconds"
    )
    print(f"[{timestamp}] {message}", flush=True)


# ============================================================
# MQTT / TLS CONFIGURATION
# ============================================================

CA_CERT = r"C:\Users\Algosium-User1\Desktop\DevServer-EventCerts\ca.crt"
CLIENT_CERT = r"C:\Users\Algosium-User1\Desktop\DevServer-EventCerts\client.crt"
CLIENT_KEY = r"C:\Users\Algosium-User1\Desktop\DevServer-EventCerts\client.key"

BROKER_HOST = "52.66.181.73"
BROKER_PORT = 8883

# CA_CERT = r"C:\Users\Algosium-User1\IdeaProjects\RMCSv4\rmcs-eventprocessor\src\main\resources\certs\ca.crt"
# CLIENT_CERT = r"C:\Users\Algosium-User1\IdeaProjects\RMCSv4\rmcs-eventprocessor\src\main\resources\certs\client.crt"
# CLIENT_KEY = r"C:\Users\Algosium-User1\IdeaProjects\RMCSv4\rmcs-eventprocessor\src\main\resources\certs\client.key"
# BROKER_HOST = "localhost"
# BROKER_PORT = 8883


TEST_CONFIG_TOPIC = "cp/algosium1/rmcs1/device1/testconfig"


# ============================================================
# DEVICE CONFIGURATION
# ============================================================

device_id = None

CFG_TOPIC = None
UPLINK_TOPIC = None

telemetry_interval = 600
heartbeat_interval = 300


# ============================================================
# EVENTS / THREAD SYNCHRONIZATION
# ============================================================

config_received_event = threading.Event()
telemetry_config_event = threading.Event()
heartbeat_config_event = threading.Event()

lock = threading.Lock()


# ============================================================
# TELEMETRY DATA
# ============================================================

TELEMETRY_DATA = [
    "A3","7F","2C","D8","51","9B","04","E6","38","72","BF","15","CA","89","43","F1","6D","27","AC",
    "5E","93","0B","D4","68","FA","31","87","C2","4A","BE","76","19","E3","55","A8","0F","CD","62",
    "3B","F7","84","2E","B9","70","46","DA","13","9F","5C","E8","35","AB","61","C7","08","F4","92",
    "2B","DE","57","A1","6F","3D","BC","74","18","E0","4F","96","C9","25","D1","83","5A","EF","39",
    "B4","07","6B","F8","42","AD","71","1C","C5","90","3E","E9","56","B2","0D","7A","CC","24","F6",
    "48","9D","63","BA","16","D7","85","30","AF","5F","E2","79","0A","C3","94","37","DB","52","B7",
    "1F","68","EA","45","8C","D0","29"
]

telemetry_counter = 0

# ============================================================
# MQTT CALLBACK - CONNECT
# ============================================================

def on_connect(client, userdata, flags, reason_code, properties):

    if reason_code == 0:
        log("Connected to Dev EMQX successfully")

        result, mid = client.subscribe(TEST_CONFIG_TOPIC, qos=0)

        if result == mqtt.MQTT_ERR_SUCCESS:
            log(
                f"Subscription requested | "
                f"Topic: {TEST_CONFIG_TOPIC} | MID: {mid}"
            )
        else:
            log(
                f"ERROR: Failed to request subscription | "
                f"Topic: {TEST_CONFIG_TOPIC} | RC: {result}"
            )

    else:
        log(f"ERROR: MQTT connection failed | Reason: {reason_code}")


# ============================================================
# MQTT CALLBACK - SUBSCRIBE ACKNOWLEDGEMENT
# ============================================================

def on_subscribe(client, userdata, mid, reason_code_list, properties):

    log(
        f"Subscription acknowledgement received | "
        f"MID: {mid} | Result: {reason_code_list}"
    )


# ============================================================
# MQTT CALLBACK - DISCONNECT
# ============================================================

def on_disconnect(client, userdata, disconnect_flags, reason_code, properties):

    log(f"MQTT disconnected | Reason: {reason_code}")


# ============================================================
# MQTT CALLBACK - MESSAGE
# ============================================================

def on_message(client, userdata, message):

    global device_id
    global CFG_TOPIC
    global UPLINK_TOPIC
    global telemetry_interval
    global heartbeat_interval

    received_at = datetime.now().astimezone().isoformat(
        sep=" ", timespec="milliseconds"
    )

    try:
        payload = json.loads(message.payload.decode("utf-8"))

        log(
            f"Message received | Topic: {message.topic} | "
            f"Payload: {payload}"
        )

        # ====================================================
        # INITIAL TEST CONFIGURATION
        # ====================================================

        if message.topic == TEST_CONFIG_TOPIC:

            if payload.get("type") != "test":
                log("Ignoring test configuration: type is not 'test'")
                return

            data = payload.get("data", {})
            received_device_id = data.get("deviceId")

            if not isinstance(received_device_id, str) or not received_device_id.strip():
                log("ERROR: Missing or invalid deviceId in test configuration")
                return

            # Validate intervals before updating configuration.
            try:
                new_tel_frq = int(data["tel_frq"])
                new_hbt_frq = int(data["hbt_frq"])

                if new_tel_frq <= 0 or new_hbt_frq <= 0:
                    log("ERROR: Telemetry and heartbeat intervals must be positive")
                    return

            except (KeyError, TypeError, ValueError):
                log("ERROR: Missing or invalid tel_frq/hbt_frq in test configuration")
                return

            device_id = received_device_id.strip()

            # Dynamically create device-specific topics.
            CFG_TOPIC = f"cp/algosium1/rmcs1/{device_id}/cfg"
            UPLINK_TOPIC = f"cp/algosium1/rmcs1/{device_id}/uplink"

            with lock:
                telemetry_interval = new_tel_frq
                heartbeat_interval = new_hbt_frq

            log("========== INITIAL DEVICE CONFIGURATION ==========")
            log(f"Configuration received at: {received_at}")
            log(f"Device ID: {device_id}")
            log(f"Telemetry interval: {telemetry_interval} seconds")
            log(f"Heartbeat interval: {heartbeat_interval} seconds")
            log(f"CFG topic: {CFG_TOPIC}")
            log(f"UPLINK topic: {UPLINK_TOPIC}")
            log("==================================================")

            result, mid = client.subscribe(CFG_TOPIC, qos=0)

            if result == mqtt.MQTT_ERR_SUCCESS:
                log(
                    f"Device CFG subscription requested | "
                    f"Topic: {CFG_TOPIC} | MID: {mid}"
                )
            else:
                log(
                    f"ERROR: Failed to request CFG subscription | "
                    f"Topic: {CFG_TOPIC} | RC: {result}"
                )

            # Wake the loops and reset their timers.
            config_received_event.set()
            telemetry_config_event.set()
            heartbeat_config_event.set()

            log("Initial configuration applied; telemetry and heartbeat timers reset")
            return

        # ====================================================
        # DEVICE CONFIGURATION UPDATE
        # ====================================================

        if CFG_TOPIC and message.topic == CFG_TOPIC:

            if payload.get("type") != "cfg":
                log("Ignoring CFG message: type is not 'cfg'")
                return

            with lock:
                new_tel_frq = telemetry_interval
                new_hbt_frq = heartbeat_interval

                try:
                    if "tel_frq" in payload:
                        new_tel_frq = int(payload["tel_frq"])

                    if "hbt_frq" in payload:
                        new_hbt_frq = int(payload["hbt_frq"])

                except (TypeError, ValueError):
                    log("ERROR: Invalid tel_frq/hbt_frq in CFG message")
                    return

                if new_tel_frq <= 0 or new_hbt_frq <= 0:
                    log("ERROR: Configuration intervals must be positive")
                    return

                telemetry_changed = new_tel_frq != telemetry_interval
                heartbeat_changed = new_hbt_frq != heartbeat_interval

                telemetry_interval = new_tel_frq
                heartbeat_interval = new_hbt_frq

            log("========== DEVICE CONFIGURATION UPDATED ==========")
            log(f"Device ID: {device_id}")
            log(f"Telemetry interval: {telemetry_interval} seconds")
            log(f"Heartbeat interval: {heartbeat_interval} seconds")
            log("==================================================")

            # Reset only the timer(s) whose interval changed.
            if telemetry_changed:
                telemetry_config_event.set()
                log("Telemetry timer reset requested")

            if heartbeat_changed:
                heartbeat_config_event.set()
                log("Heartbeat timer reset requested")

    except json.JSONDecodeError:
        log(f"ERROR: Invalid JSON received on topic {message.topic}")

    except Exception as e:
        log(f"ERROR processing message on {message.topic}: {e}")


# ============================================================
# TELEMETRY LOOP
# ============================================================

def telemetry_loop(client):

    global telemetry_counter

    config_received_event.wait()
    log("Telemetry thread started")

    while True:

        with lock:
            interval = telemetry_interval
            current_device_id = device_id
            current_uplink_topic = UPLINK_TOPIC

        if interval <= 0:
            telemetry_config_event.wait(timeout=1)
            telemetry_config_event.clear()
            continue

        # Wait for the interval to expire or a config change.
        config_changed = telemetry_config_event.wait(timeout=interval)

        if config_changed:
            telemetry_config_event.clear()
            log("Telemetry timer restarted due to configuration change")
            continue

        if not current_uplink_topic:
            log("ERROR: Cannot send telemetry; uplink topic is not configured")
            continue

        telemetry_counter += 1

        incremented_data = [
            f"{(int(value, 16) + telemetry_counter) & 0xFF:02X}"
            for value in TELEMETRY_DATA
        ]

        telemetry_payload = {
            "type": "tel",
            "data": incremented_data
        }

        try:
            result = client.publish(
                current_uplink_topic,
                json.dumps(telemetry_payload),
                qos=0,
                retain=False
            )

            if result.rc == mqtt.MQTT_ERR_SUCCESS:
                log(
                    f"Telemetry publish accepted by local MQTT client | "
                    f"Device: {current_device_id} | "
                    f"Topic: {current_uplink_topic} | "
                    f"Interval: {interval}s | "
                    f"Values: {len(TELEMETRY_DATA)}"
                )
            else:
                log(f"ERROR: Telemetry publish failed | RC: {result.rc}")

        except Exception as e:
            log(f"ERROR publishing telemetry: {e}")


# ============================================================
# HEARTBEAT LOOP
# ============================================================

def heartbeat_loop(client):

    config_received_event.wait()
    log("Heartbeat thread started")

    while True:

        with lock:
            interval = heartbeat_interval
            current_device_id = device_id
            current_uplink_topic = UPLINK_TOPIC

        if interval <= 0:
            heartbeat_config_event.wait(timeout=1)
            heartbeat_config_event.clear()
            continue

        # Wait for the interval to expire or a config change.
        config_changed = heartbeat_config_event.wait(timeout=interval)

        if config_changed:
            heartbeat_config_event.clear()
            log("Heartbeat timer restarted due to configuration change")
            continue

        if not current_uplink_topic:
            log("ERROR: Cannot send heartbeat; uplink topic is not configured")
            continue

        heartbeat_payload = {
            "type": "hbt",
            "data": "ok"
        }

        try:
            result = client.publish(
                current_uplink_topic,
                json.dumps(heartbeat_payload),
                qos=0,
                retain=False
            )

            if result.rc == mqtt.MQTT_ERR_SUCCESS:
                log(
                    f"Heartbeat publish accepted by local MQTT client | "
                    f"Device: {current_device_id} | "
                    f"Topic: {current_uplink_topic} | "
                    f"Interval: {interval}s"
                )
            else:
                log(f"ERROR: Heartbeat publish failed | RC: {result.rc}")

        except Exception as e:
            log(f"ERROR publishing heartbeat: {e}")


# ============================================================
# MAIN
# ============================================================

def main():

    client = mqtt.Client(
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        client_id="python-device-simulator"
    )

    client.on_connect = on_connect
    client.on_message = on_message
    client.on_subscribe = on_subscribe
    client.on_disconnect = on_disconnect

    # ========================================================
    # TLS CONFIGURATION
    # ========================================================

    client.tls_set(
        ca_certs=CA_CERT,
        certfile=CLIENT_CERT,
        keyfile=CLIENT_KEY,
        tls_version=ssl.PROTOCOL_TLS_CLIENT
    )

    # ========================================================
    # MQTT AUTHENTICATION
    # ========================================================

    client.username_pw_set("admin", "P@ssword1")

    # ========================================================
    # CONNECT TO DEV EMQX
    # ========================================================

    log(f"Connecting to Dev EMQX: {BROKER_HOST}:{BROKER_PORT}")

    try:
        client.connect(
            BROKER_HOST,
            BROKER_PORT,
            keepalive=60
        )

        client.loop_start()

        telemetry_thread = threading.Thread(
            target=telemetry_loop,
            args=(client,),
            daemon=True
        )

        heartbeat_thread = threading.Thread(
            target=heartbeat_loop,
            args=(client,),
            daemon=True
        )

        telemetry_thread.start()
        heartbeat_thread.start()

        log("MQTT device simulator started")
        log(f"Waiting for initial test configuration: {TEST_CONFIG_TOPIC}")

        while True:
            time.sleep(1)

    except KeyboardInterrupt:
        log("Shutdown requested by user")

    except Exception as e:
        log(f"FATAL: Simulator error: {e}")

    finally:
        log("Stopping MQTT network loop")
        client.disconnect()
        client.loop_stop()
        log("MQTT device simulator stopped")


if __name__ == "__main__":
    main()
