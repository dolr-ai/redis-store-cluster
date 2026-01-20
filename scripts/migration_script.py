import redis
import sys
import time
import logging
import traceback
from typing import Set, Dict
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from itertools import islice

# ===================== LOGGING =====================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("redis_sync.log")
    ]
)

logger = logging.getLogger("redis-sync")

# ===================== CERTS =====================

CERT_DIR = "" # Update this
CA_CERT = CERT_DIR + "/ca-cert.pem"
CLIENT_CERT = CERT_DIR + "/client-cert.pem"
CLIENT_KEY = CERT_DIR + "/client-key.pem"

# ===================== CONFIG =====================

FLY_REDIS_CONFIG = {
    'host': '',      # Update this
    'port': 16379,   # Update this
    'password': '',  # Update this
    'decode_responses': False
}

DRAGONFLY_CONFIG = {
    'host': '',      # Update this
    'port': 6380,    # Update this
    'password': '',  # Update this
    'ssl': True,
    'ssl_ca_certs': CA_CERT,
    'ssl_certfile': CLIENT_CERT,
    'ssl_keyfile': CLIENT_KEY,
    'decode_responses': False
}


KEY_PREFIX = '' # Update this
SYNC_INTERVAL = 600
SCAN_BATCH_SIZE = 10000


# ===================================================
#                REDIS SYNC MONITOR
# ===================================================

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

        self.BATCH_SIZE = 1000
        self.WORKERS = 12

    # ------------------------------------------------

    def chunk(self, iterable, size):
        it = iter(iterable)
        while True:
            batch = list(islice(it, size))
            if not batch:
                return
            yield batch

    # ------------------------------------------------

    def get_all_keys(self):
        keys = []
        cursor = 0

        while True:
            cursor, batch = self.source.scan(cursor, count=SCAN_BATCH_SIZE)
            keys.extend(batch)

            if cursor == 0:
                break

        return keys

    # ------------------------------------------------

    def check_connections(self):
        try:
            self.source.ping()
            self.dest.ping()
            return True
        except Exception as e:
            logger.critical(f"Redis connection lost: {e}")
            return False

    # ------------------------------------------------

    def sync_batch(self, keys):
        pipe = self.dest.pipeline(transaction=False)

        for key in keys:
            try:
                key_str = key.decode()
                new_key = f"{KEY_PREFIX}{key_str}".encode()

                # ----- Key deleted on source -----
                if not self.source.exists(key):
                    pipe.delete(new_key)
                    self.stats['deleted_keys'] += 1
                    continue

                key_type = self.source.type(key).decode()
                ttl = self.source.ttl(key)

                pipe.delete(new_key)

                # ----- STRING -----
                if key_type == 'string':
                    pipe.set(new_key, self.source.get(key))

                # ----- LIST -----
                elif key_type == 'list':
                    vals = self.source.lrange(key, 0, -1)
                    if vals:
                        pipe.rpush(new_key, *vals)

                # ----- SET -----
                elif key_type == 'set':
                    members = self.source.smembers(key)
                    if members:
                        pipe.sadd(new_key, *members)

                # ----- ZSET -----
                elif key_type == 'zset':
                    members = self.source.zrange(key, 0, -1, withscores=True)
                    if members:
                        pipe.zadd(new_key, {m: s for m, s in members})

                # ----- HASH -----
                elif key_type == 'hash':
                    print(f"Syncing hash key: {key_str}")
                    data = self.source.hgetall(key)
                    if data:
                        pipe.hset(new_key, mapping=data)

                else:
                    logger.warning(f"Unknown type {key_type} for key {key_str}")
                    continue

                if ttl > 0:
                    pipe.expire(new_key, ttl)

                if key not in self.known_keys:
                    self.stats['new_keys'] += 1
                else:
                    self.stats['updated_keys'] += 1

                self.known_keys[key] = time.time()
                self.stats['total_synced'] += 1

            except Exception as e:
                self.stats['errors'] += 1

                logger.error(
                    f"❌ Failed syncing key: {key}\n"
                    f"Type: {self.source.type(key)}\n"
                    f"Error: {str(e)}\n"
                    f"{traceback.format_exc()}"
                )

        # ----- EXECUTE PIPELINE -----
        try:
            pipe.execute()
            logger.info(f"Synced batch of {len(keys)} keys")

        except Exception as e:
            self.stats['errors'] += len(keys)

            logger.critical(
                f"🚨 PIPELINE FAILURE for batch!\n"
                f"Keys: {keys[:5]}...\n"
                f"Error: {str(e)}\n"
                f"{traceback.format_exc()}"
            )

    # ------------------------------------------------

    def parallel_sync(self, keys):
        with ThreadPoolExecutor(max_workers=self.WORKERS) as ex:
            futures = []

            for batch in self.chunk(keys, self.BATCH_SIZE):
                futures.append(ex.submit(self.sync_batch, batch))

            for f in futures:
                try:
                    f.result()
                except Exception as e:
                    logger.critical(
                        f"Thread crashed: {str(e)}\n"
                        f"{traceback.format_exc()}"
                    )
                    self.stats['errors'] += 1

    # ------------------------------------------------

    def initial_sync(self):
        print("\nStarting optimized initial sync...")
        keys = self.get_all_keys()
        print(f"Found {len(keys)} keys")

        self.parallel_sync(keys)

        logger.info(
            f"Initial sync done: {self.stats['total_synced']} keys synced"
        )

    # ------------------------------------------------

    def sync_changes(self):
        current_keys = set(self.get_all_keys())
        previous_keys = set(self.known_keys.keys())

        deleted = previous_keys - current_keys

        for key in deleted:
            try:
                new_key = f"{KEY_PREFIX}{key.decode()}".encode()
                self.dest.delete(new_key)

                del self.known_keys[key]
                self.stats['deleted_keys'] += 1

            except Exception as e:
                logger.error(f"Failed deleting key {key}: {e}")

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

        try:
            while True:
                time.sleep(SYNC_INTERVAL)

                if not self.check_connections():
                    logger.error("Skipping cycle due to connection failure")
                    continue

                before = self.stats['total_synced']
                self.sync_changes()

                delta = self.stats['total_synced'] - before
                logger.info(f"Synced {delta} changes")

        except KeyboardInterrupt:
            logger.info("Sync monitor stopped by user.")
            self.print_stats()


# ===================================================
#                  HELPERS
# ===================================================

def get_total_keys_on_source(source: redis.Redis, pattern: str = '*') -> int:
    total = 0
    cursor = 0

    while True:
        cursor, keys = source.scan(cursor, match=pattern, count=SCAN_BATCH_SIZE)
        total += len(keys)

        if cursor == 0:
            break

    return total


def get_redis_clients():
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


# ===================================================
#                     MAIN
# ===================================================

if __name__ == "__main__":

    print("=" * 60)
    print("Redis Continuous Sync Script")
    print("=" * 60)

    source_redis, dest_redis = get_redis_clients() 
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

    monitor = RedisSyncMonitor(source_redis, dest_redis)
    monitor.run()