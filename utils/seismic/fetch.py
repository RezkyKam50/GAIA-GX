import json
import logging
from tornado.websocket import websocket_connect
from tornado.ioloop import IOLoop, PeriodicCallback
from tornado import gen
from collections import deque
import cudf

echo_uri = 'wss://www.seismicportal.eu/standing_order/websocket'
PING_INTERVAL = 10
recent_events = deque(maxlen=1000)
CSV_PATH = './utils/datasets/earthquake_events.csv'

@gen.coroutine
def listen(ws):
    while True:
        msg = yield ws.read_message()
        if msg is None:
            logging.info("WebSocket closed.")
            break

        try:
            data = json.loads(msg)
            props = data['data']['properties']
            coords = data['data']['geometry']['coordinates']
            lon, lat, depth = coords
            mag = props.get('mag', None)
            region = props.get('flynn_region', 'Unknown')

            recent_events.append({
                'lat': lat,
                'lon': lon,
                'depth': depth,
                'mag': mag,
                'region': region,
                'time': props.get('time'),
            })
        except Exception:
            logging.exception("Error parsing message")

@gen.coroutine
def launch_client():
    logging.info("Connecting to Seismic Portal...")
    ws = yield websocket_connect(echo_uri, ping_interval=PING_INTERVAL)
    logging.info("Listening for earthquake updates...")
    yield listen(ws)

def save_csv():
    if recent_events:
        df = cudf.DataFrame(list(recent_events))
        df.to_csv(CSV_PATH, index=False)
        logging.info(f"Saved {len(recent_events)} events to CSV.")

def start_seismic_listener():
    ioloop = IOLoop.current()
    ioloop.spawn_callback(launch_client)
    
    # Save CSV every 30 seconds
    periodic = PeriodicCallback(save_csv, 30000)
    periodic.start()
    
    ioloop.start()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    start_seismic_listener()
