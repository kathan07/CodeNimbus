import redis
import urllib.parse
import time

class RedisQueueMonitor:
    def __init__(self, redis_url, queue_name):
        # Parse the Redis URL
        parsed_url = urllib.parse.urlparse(redis_url)
        redis_host = parsed_url.hostname
        redis_port = parsed_url.port or 6379
        redis_username = parsed_url.username or 'default'
        redis_password = parsed_url.password or ''
        
        # Initialize the Redis client
        self.redis_client = redis.Redis(
            host=redis_host,
            port=redis_port,
            username=redis_username,
            password=redis_password,
            ssl=True if parsed_url.scheme == 'rediss' else False,
            ssl_cert_reqs='none',  # Adjust for your certificate settings
            decode_responses=True
        )
        self.queue_name = queue_name

    def monitor_queue(self):
        last_size = 0  # Track the last known size of the queue
        print(f"Monitoring queue '{self.queue_name}'...")
        
        while True:
            try:
                # Get the current queue size
                current_size = self.redis_client.llen(self.queue_name)
                
                # Compare sizes and print if the size has increased
                if current_size > last_size:
                    print(f"Queue size increased: {current_size} (previous: {last_size})")
                
                # Update last known size
                last_size = current_size
                
                # Polling interval
                # time.sleep(1)  # Adjust this based on requirements
                
            except redis.ConnectionError as e:
                print(f"Redis connection error: {e}")
                time.sleep(5)  # Retry connection after a delay
                
            except Exception as e:
                print(f"An error occurred: {e}")
                time.sleep(5)

# Parameters
REDIS_URL = "rediss://username:password@host:port"
QUEUE_NAME = "worker_queue"

# Initialize and monitor
if __name__ == "__main__":
    monitor = RedisQueueMonitor('rediss://red-ctc7l3tumphs73b2aee0:kzQppr0FwtANbjuQqoo5GiwM53VuVFCz@singapore-redis.render.com:6379', 'code_execution_queue')
    monitor.monitor_queue()
