import redis
import sys

# Configuration
PRIMARY_IP = 'IP_ADDRESS_OF_PRIMARY'
REPLICA_IP = 'IP_ADDRESS_OF_REPLICA'
PASSWORD = 'DRAGONFLY_PASSWORD'

def test_primary():
    print('\n=== Testing Primary ===')
    
    try:
        r = redis.Redis(
            host=PRIMARY_IP,
            port=6379,
            password=PASSWORD,
            decode_responses=True
        )
        
        # Test connection
        if r.ping():
            print('✅ Connection: PONG')
        
        # Test write
        r.set('test_key', 'Hello from Python')
        print('✅ Write successful')
        
        # Test read
        value = r.get('test_key')
        print(f'✅ Read successful: {value}')
        
        # Get database size
        size = r.dbsize()
        print(f'✅ Database size: {size} keys')
        
        # Get server info
        info = r.info('server')
        print(f'✅ Server version: {info.get("redis_version")}')
        
        print('✅ PRIMARY test complete!')
        
    except Exception as e:
        print(f'❌ Primary test failed: {e}')
        sys.exit(1)

def test_replica():
    print('\n=== Testing Replica ===')
    
    try:
        r = redis.Redis(
            host=REPLICA_IP,
            port=6379,
            password=PASSWORD,
            decode_responses=True
        )
        
        # Test connection
        if r.ping():
            print('✅ Connection: PONG')
        
        # Test read
        value = r.get('test_key')
        print(f'✅ Read successful: {value}')
        
        # Try write (should fail)
        try:
            r.set('another_key', 'test')
            print('⚠️  Write succeeded (unexpected!)')
        except redis.exceptions.ReadOnlyError:
            print('✅ Write blocked (expected): Read-only replica')
        
        # Check replication status
        info = r.info('replication')
        print(f'✅ Role: {info.get("role")}')
        print(f'✅ Master: {info.get("master_host")}')
        print(f'✅ Link status: {info.get("master_link_status")}')
        
        print('✅ Replica test complete!')
        
    except Exception as e:
        print(f'❌ Replica test failed: {e}')
        sys.exit(1)

if __name__ == '__main__':
    test_primary()
    test_replica()
    print('\n=== All tests complete! ===\n')