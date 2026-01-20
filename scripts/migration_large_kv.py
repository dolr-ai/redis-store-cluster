import redis
import sys
import logging
import traceback
import time
from typing import Iterator, Dict, Any
from datetime import datetime
# ===================== LOGGING =====================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("large_hash_sync.log")
    ]
)

logger = logging.getLogger("large-hash-sync")

# ===================== CERTS =====================

CERT_DIR = "path to cert directory"
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

# Max size per batch (8MB to stay safely under 10MB limit)
MAX_BATCH_SIZE_BYTES = 512 * 1024

CHUNK_DELAY_SECONDS = 15

# ===================================================
#           LARGE HASH KEY SYNC HANDLER
# ===================================================

class LargeHashSyncHandler:
    def __init__(self, source: redis.Redis, dest: redis.Redis):
        self.source = source
        self.dest = dest
        
    def estimate_size(self, data: Dict[bytes, bytes]) -> int:
        """Estimate the size of hash data in bytes"""
        return sum(len(k) + len(v) for k, v in data.items())
    
    def hscan_chunked(self, key: bytes, chunk_size_bytes: int) -> Iterator[Dict[bytes, bytes]]:
        """
        Scan a hash key and yield chunks that don't exceed chunk_size_bytes
        """
        cursor = 0
        current_chunk = {}
        current_size = 0
        scan_iterations = 0
        
        print(f"\n[HSCAN] Starting to scan hash key: {key.decode()}")
        logger.info(f"Starting HSCAN with count=10000 per iteration")
        
        try:
            while True:
                scan_iterations += 1
                print(f"[HSCAN] Iteration #{scan_iterations}, cursor={cursor}")
                
                try:
                    cursor, data = self.source.hscan(key, cursor, count=10000)
                    print(f"[HSCAN] Retrieved {len(data)} fields in this iteration")
                    
                except Exception as e:
                    logger.error(f"❌ HSCAN failed at iteration {scan_iterations}")
                    logger.error(f"Error: {str(e)}")
                    logger.error(traceback.format_exc())
                    raise
                
                for field, value in data.items():
                    item_size = len(field) + len(value)
                    
                    # If adding this item would exceed the chunk size, yield current chunk
                    if current_size + item_size > chunk_size_bytes and current_chunk:
                        chunk_fields = len(current_chunk)
                        print(f"\n[CHUNK] Chunk full! Yielding {chunk_fields:,} fields (~{current_size:,} bytes)")
                        logger.info(f"Yielding chunk with {chunk_fields:,} fields (~{current_size:,} bytes)")
                        yield current_chunk
                        current_chunk = {}
                        current_size = 0
                    
                    current_chunk[field] = value
                    current_size += item_size
                
                if cursor == 0:
                    print(f"[HSCAN] Scan completed after {scan_iterations} iterations")
                    break
            
            # Yield remaining data
            if current_chunk:
                chunk_fields = len(current_chunk)
                print(f"\n[CHUNK] Final chunk: {chunk_fields:,} fields (~{current_size:,} bytes)")
                logger.info(f"Yielding final chunk with {chunk_fields:,} fields (~{current_size:,} bytes)")
                yield current_chunk
                
        except Exception as e:
            logger.error(f"❌ Exception in hscan_chunked: {str(e)}")
            logger.error(traceback.format_exc())
            raise
    
    def sync_large_hash(self, source_key: bytes):
        """
        Sync a large hash key by chunking it into smaller batches
        """
        start_time = time.time()
        
        try:
            key_str = source_key.decode()
            dest_key = f"{KEY_PREFIX}{key_str}".encode()
            
            print(f"\n{'='*70}")
            print(f"STARTING LARGE HASH SYNC")
            print(f"{'='*70}")
            logger.info(f"Starting sync for large hash: {key_str}")
            logger.info(f"Destination key: {dest_key.decode()}")
            
            # Check if key exists and is a hash
            print(f"\n[STEP 1] Checking if key exists on source...")
            if not self.source.exists(source_key):
                logger.error(f"❌ Key {key_str} does not exist on source")
                print(f"❌ ERROR: Key does not exist!")
                return False
            print(f"✓ Key exists on source")
            
            print(f"\n[STEP 2] Verifying key type...")
            key_type = self.source.type(source_key).decode()
            print(f"Key type: {key_type}")
            
            if key_type != 'hash':
                logger.error(f"❌ Key {key_str} is not a hash (type: {key_type})")
                print(f"❌ ERROR: Key is not a hash!")
                return False
            print(f"✓ Confirmed key is a hash")
            
            # Get total field count
            print(f"\n[STEP 3] Counting total fields in hash...")
            try:
                total_fields = self.source.hlen(source_key)
                print(f"✓ Total fields: {total_fields:,}")
                logger.info(f"Total fields in hash: {total_fields:,}")
            except Exception as e:
                logger.error(f"❌ Failed to get field count: {str(e)}")
                logger.error(traceback.format_exc())
                print(f"❌ ERROR: Could not count fields!")
                return False
            
            # Delete existing key on destination
            print(f"\n[STEP 4] Deleting existing key on destination (if exists)...")
            logger.info("Deleting existing key on destination...")
            try:
                deleted = self.dest.delete(dest_key)
                if deleted:
                    print(f"✓ Deleted existing key (had {deleted} key)")
                else:
                    print(f"✓ No existing key to delete")
            except Exception as e:
                logger.error(f"❌ Failed to delete destination key: {str(e)}")
                logger.error(traceback.format_exc())
                print(f"❌ ERROR: Could not delete destination key!")
                return False
            
            # Sync in chunks
            print(f"\n[STEP 5] Starting chunked sync...")
            print(f"Max chunk size: {MAX_BATCH_SIZE_BYTES:,} bytes ({MAX_BATCH_SIZE_BYTES//1024//1024}MB)")
            print(f"{'-'*70}")
            
            total_synced = 0
            chunk_num = 0
            
            try:
                for chunk in self.hscan_chunked(source_key, MAX_BATCH_SIZE_BYTES):
                    chunk_num += 1
                    chunk_size = len(chunk)
                    chunk_bytes = self.estimate_size(chunk)
                    
                    print(f"\n[CHUNK #{chunk_num}]")
                    print(f"  Fields: {chunk_size:,}")
                    print(f"  Size: ~{chunk_bytes:,} bytes ({chunk_bytes/1024/1024:.2f}MB)")
                    logger.info(f"Syncing chunk #{chunk_num} with {chunk_size:,} fields (~{chunk_bytes:,} bytes)")
                    
                    # Use pipeline for efficiency
                    print(f"  Writing to destination...")
                    try:
                        pipe = self.dest.pipeline(transaction=False)
                        pipe.hset(dest_key, mapping=chunk)
                        pipe.execute()
                        print(f"  ✓ Write successful")
                        
                    except Exception as e:
                        logger.error(f"❌ Failed to write chunk #{chunk_num}")
                        logger.error(f"Chunk size: {chunk_size:,} fields, ~{chunk_bytes:,} bytes")
                        logger.error(f"Error: {str(e)}")
                        logger.error(traceback.format_exc())
                        print(f"  ❌ ERROR writing chunk!")
                        raise
                    
                    total_synced += chunk_size
                    progress = total_synced * 100 // total_fields if total_fields > 0 else 0
                    print(f"  Progress: {total_synced:,}/{total_fields:,} fields ({progress}%)")
                    logger.info(f"Progress: {total_synced:,}/{total_fields:,} fields ({progress}%)")

                    if total_synced < total_fields:  # Don't sleep after the last chunk
                        print(f"  Sleeping {CHUNK_DELAY_SECONDS}s to stabilize...")
                        logger.info(f"Sleeping {CHUNK_DELAY_SECONDS}s between chunks")
                        time.sleep(CHUNK_DELAY_SECONDS)
                    
            except Exception as e:
                logger.error(f"❌ Chunked sync failed")
                logger.error(f"Error: {str(e)}")
                logger.error(traceback.format_exc())
                print(f"\n❌ ERROR during chunked sync!")
                return False
            
            # Set TTL if exists
            print(f"\n[STEP 6] Checking TTL...")
            try:
                ttl = self.source.ttl(source_key)
                if ttl > 0:
                    print(f"Setting TTL: {ttl} seconds")
                    logger.info(f"Setting TTL: {ttl} seconds")
                    self.dest.expire(dest_key, ttl)
                    print(f"✓ TTL set")
                else:
                    print(f"No TTL on source key (TTL={ttl})")
            except Exception as e:
                logger.warning(f"⚠ Could not set TTL: {str(e)}")
                print(f"⚠ Warning: Could not set TTL (continuing anyway)")
            
            # Verify sync
            print(f"\n[STEP 7] Verifying sync...")
            try:
                dest_fields = self.dest.hlen(dest_key)
                print(f"Source fields: {total_fields:,}")
                print(f"Destination fields: {dest_fields:,}")
            except Exception as e:
                logger.error(f"❌ Failed to verify sync: {str(e)}")
                logger.error(traceback.format_exc())
                print(f"❌ ERROR: Could not verify destination!")
                return False
            
            elapsed_time = time.time() - start_time
            
            print(f"\n{'='*70}")
            if total_fields == dest_fields:
                print(f"✓✓✓ SYNC COMPLETED SUCCESSFULLY ✓✓✓")
                print(f"{'='*70}")
                print(f"Source fields: {total_fields:,}")
                print(f"Destination fields: {dest_fields:,}")
                print(f"Total chunks: {chunk_num}")
                print(f"Time taken: {elapsed_time:.2f} seconds ({elapsed_time/60:.2f} minutes)")
                print(f"{'='*70}")
                
                logger.info(f"✓ Sync completed successfully!")
                logger.info(f"Source fields: {total_fields:,}")
                logger.info(f"Destination fields: {dest_fields:,}")
                logger.info(f"Total chunks: {chunk_num}")
                logger.info(f"Time taken: {elapsed_time:.2f} seconds")
                return True
            else:
                print(f"❌❌❌ SYNC FAILED - FIELD COUNT MISMATCH ❌❌❌")
                print(f"{'='*70}")
                print(f"Source fields: {total_fields:,}")
                print(f"Destination fields: {dest_fields:,}")
                print(f"Missing: {total_fields - dest_fields:,} fields")
                print(f"{'='*70}")
                
                logger.error(f"❌ Field count mismatch!")
                logger.error(f"Source: {total_fields:,}, Destination: {dest_fields:,}")
                return False
            
        except Exception as e:
            elapsed_time = time.time() - start_time
            print(f"\n{'='*70}")
            print(f"❌❌❌ SYNC FAILED WITH EXCEPTION ❌❌❌")
            print(f"{'='*70}")
            print(f"Error: {str(e)}")
            print(f"Time before failure: {elapsed_time:.2f} seconds")
            print(f"{'='*70}")
            
            logger.error(f"❌ Failed to sync large hash {source_key}: {str(e)}")
            logger.error(traceback.format_exc())
            return False


# ===================================================
#                  HELPERS
# ===================================================

def get_redis_clients():
    print("\n[CONNECTION] Connecting to Redis instances...")
    print(f"{'-'*70}")
    
    try:
        print(f"Connecting to Fly Redis at {FLY_REDIS_CONFIG['host']}:{FLY_REDIS_CONFIG['port']}...")
        source = redis.Redis(**FLY_REDIS_CONFIG)
        source.ping()
        print("✓ Connected to Fly Redis (READ-ONLY mode)")
        logger.info("Connected to Fly Redis")
    except Exception as e:
        print(f"❌ Failed to connect to Fly Redis")
        print(f"Error: {str(e)}")
        logger.error(f"Failed to connect to Fly Redis: {str(e)}")
        logger.error(traceback.format_exc())
        sys.exit(1)

    try:
        print(f"Connecting to Dragonfly at {DRAGONFLY_CONFIG['host']}:{DRAGONFLY_CONFIG['port']}...")
        dest = redis.Redis(**DRAGONFLY_CONFIG)
        dest.ping()
        print("✓ Connected to Dragonfly (WRITE mode)")
        logger.info("Connected to Dragonfly")
    except Exception as e:
        print(f"❌ Failed to connect to Dragonfly")
        print(f"Error: {str(e)}")
        logger.error(f"Failed to connect to Dragonfly: {str(e)}")
        logger.error(traceback.format_exc())
        sys.exit(1)

    print(f"{'-'*70}\n")
    return source, dest


# ===================================================
#                     MAIN
# ===================================================

if __name__ == "__main__":
    print("\n" + "=" * 70)
    print(" " * 20 + "LARGE HASH KEY SYNC SCRIPT")
    print("=" * 70)
    
    # The problematic key from your error
    KEY_TO_SYNC = b'canister2principal'
    
    print(f"\nConfiguration:")
    print(f"  Key to sync: {KEY_TO_SYNC.decode()}")
    print(f"  Destination prefix: {KEY_PREFIX}")
    print(f"  Max batch size: {MAX_BATCH_SIZE_BYTES:,} bytes ({MAX_BATCH_SIZE_BYTES//1024//1024}MB)")
    print(f"  Log file: large_hash_sync.log")
    
    source_redis, dest_redis = get_redis_clients()
    
    print(f"Ready to sync!")
    response = input("\nProceed with sync? (yes/no): ")
    if response.lower() != 'yes':
        print("Sync cancelled.")
        logger.info("Sync cancelled by user")
        sys.exit(0)
    
    print(f"\nStarting sync at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Starting sync at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    handler = LargeHashSyncHandler(source_redis, dest_redis)
    success = handler.sync_large_hash(KEY_TO_SYNC)
    
    print(f"\nSync finished at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Sync finished at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    if success:
        print("\n✓✓✓ SYNC COMPLETED SUCCESSFULLY ✓✓✓")
        print("Check 'large_hash_sync.log' for detailed logs")
        sys.exit(0)
    else:
        print("\n❌❌❌ SYNC FAILED ❌❌❌")
        print("Check 'large_hash_sync.log' for detailed error logs")
        sys.exit(1)
