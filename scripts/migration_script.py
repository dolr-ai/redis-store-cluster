import redis
import sys
import time
from typing import Set, Dict
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from itertools import islice

CERT_DIR = ""
CA_CERT = CERT_DIR + "/ca-cert.pem"
CLIENT_CERT = CERT_DIR + "/client-cert.pem"
CLIENT_KEY = CERT_DIR + "/client-key.pem"

# Configuration - READ ONLY for Fly Redis (source)
FLY_REDIS_CONFIG = {
    'host': '',
    'port': 16379,
    'password': '',  # Update this
    'decode_responses': False
}

DRAGONFLY_CONFIG = {
    'host': '',
    'port': 6380,
    'password': '',  # Update this
    'ssl': True,
    'ssl_ca_certs': CA_CERT,
    'ssl_certfile': CLIENT_CERT,
    'ssl_keyfile': CLIENT_KEY,
    'decode_responses': False
}

KEY_PREFIX = ''
SYNC_INTERVAL = 60  # Check for changes every 60 seconds
SCAN_BATCH_SIZE = 5000

class RedisSyncMonitor:
    def __init__(self, source: redis.Redis, dest: redis.Redis):
        self.source = source
        self.dest = dest
        self.known_keys: Dict[bytes, float] = {}
        self.stats = {
            'total_synced': 0,
            'new_keys': 0,
            'updated_keys': 0,
            'deleted_keys': 0,
            'errors': 0
        }

        self.BATCH_SIZE = 500
        self.WORKERS = 12

    def chunk(self, iterable, size):
        it = iter(iterable)
        while True:
            batch = list(islice(it, size))
            if not batch:
                return
            yield batch

    def get_all_keys(self):
        keys = []
        cursor = 0
        while True:
            cursor, batch = self.source.scan(cursor, count=SCAN_BATCH_SIZE)
            keys.extend(batch)
            if cursor == 0:
                break
        return keys

    def sync_batch(self, keys):
        pipe = self.dest.pipeline(transaction=False)

        for key in keys:
            try:
                key_str = key.decode()
                new_key = f"{KEY_PREFIX}{key_str}".encode()

                if not self.source.exists(key):
                    pipe.delete(new_key)
                    self.stats['deleted_keys'] += 1
                    continue

                key_type = self.source.type(key).decode()
                ttl = self.source.ttl(key)

                pipe.delete(new_key)

                if key_type == 'string':
                    pipe.set(new_key, self.source.get(key))

                elif key_type == 'list':
                    vals = self.source.lrange(key, 0, -1)
                    if vals:
                        pipe.rpush(new_key, *vals)

                elif key_type == 'set':
                    members = self.source.smembers(key)
                    if members:
                        pipe.sadd(new_key, *members)

                elif key_type == 'zset':
                    members = self.source.zrange(key, 0, -1, withscores=True)
                    if members:
                        pipe.zadd(new_key, {m: s for m, s in members})

                elif key_type == 'hash':
                    print(f"Syncing hash key: {key_str}")
                    data = self.source.hgetall(key)
                    if data:
                        pipe.hset(new_key, mapping=data)

                if ttl > 0:
                    pipe.expire(new_key, ttl)

                if key not in self.known_keys:
                    self.stats['new_keys'] += 1
                else:
                    self.stats['updated_keys'] += 1

                self.known_keys[key] = time.time()
                self.stats['total_synced'] += 1

            except Exception:
                self.stats['errors'] += 1

        pipe.execute()
        print(f"Synced batch of {len(keys)} keys")

    def parallel_sync(self, keys):
        with ThreadPoolExecutor(max_workers=self.WORKERS) as ex:
            for batch in self.chunk(keys, self.BATCH_SIZE):
                ex.submit(self.sync_batch, batch)

    def initial_sync(self):
        print("\nStarting optimized initial sync...")
        keys = self.get_all_keys()
        print(f"Found {len(keys)} keys")

        self.parallel_sync(keys)

        print(f"Initial sync done: {self.stats['total_synced']} keys synced")

    def sync_changes(self):
        current_keys = set(self.get_all_keys())
        previous_keys = set(self.known_keys.keys())

        deleted = previous_keys - current_keys
        for key in deleted:
            new_key = f"{KEY_PREFIX}{key.decode()}".encode()
            self.dest.delete(new_key)
            del self.known_keys[key]
            self.stats['deleted_keys'] += 1

        self.parallel_sync(list(current_keys))

    def print_stats(self): 
        """Print current statistics""" 
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"\n[{timestamp}] Sync Stats:")
        print(f" Total synced: {self.stats['total_synced']}")
        print(f" New keys: {self.stats['new_keys']}")
        print(f" Updated keys: {self.stats['updated_keys']}")
        print(f" Deleted keys: {self.stats['deleted_keys']}")
        print(f" Errors: {self.stats['errors']}")
        print(f" Currently tracking: {len(self.known_keys)} keys")

    def run(self):
        self.initial_sync()

        try :
            while True:
                time.sleep(SYNC_INTERVAL)
                before = self.stats['total_synced']
                self.sync_changes()
                delta = self.stats['total_synced'] - before
                print(f"Synced {delta} changes")
        except KeyboardInterrupt:
            print("Sync monitor stopped by user.")
            self.print_stats()


def get_total_keys_on_source(source: redis.Redis, pattern: str = '*') -> int:   
    """Get total number of keys on source matching the pattern (READ-ONLY)"""
    total = 0
    cursor = 0
    while True:
        cursor, keys = source.scan(cursor, match=pattern, count=SCAN_BATCH_SIZE)
        total += len(keys)
        if cursor == 0:
            break
    return total

def get_redis_clients():
    """Establish connections to both Redis instances"""
    try:
        source = redis.Redis(**FLY_REDIS_CONFIG)
        source.ping()
        print("✓ Connected to Fly Redis (READ-ONLY mode)")
    except Exception as e:
        print(f"✗ Failed to connect to Fly Redis: {e}")
        sys.exit(1)
    
    try:
        dest = redis.Redis(**DRAGONFLY_CONFIG)
        dest.ping()
        print("✓ Connected to Dragonfly (WRITE mode)")
    except Exception as e:
        print(f"✗ Failed to connect to Dragonfly: {e}")
        sys.exit(1)
    
    return source, dest

if __name__ == "__main__":
    print("=" * 60)
    print("Redis Continuous Sync Script")
    print("=" * 60)
    
    # Connect to both Redis instances
    source_redis, dest_redis = get_redis_clients()
    
    # Configuration info
    print(f"\nConfiguration:")
    print(f"  Source: Fly Redis (READ-ONLY)")
    print(f"  Destination: Dragonfly")
    print(f"  Key prefix: '{KEY_PREFIX}'")
    print(f"  Sync interval: {SYNC_INTERVAL} seconds")
    
    response = input("\nStart continuous sync? (yes/no): ")
    if response.lower() != 'yes':
        print("Sync cancelled.")
        sys.exit(0)

    print("fetching total count of keys on source...")
    total_keys = get_total_keys_on_source(source_redis)
    print(f"\nTotal keys on source to sync: {total_keys}")

    # Create and run monitor
    monitor = RedisSyncMonitor(source_redis, dest_redis)
    monitor.run()