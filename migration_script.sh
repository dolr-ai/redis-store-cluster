#!/bin/bash

# Zero-Downtime Migration Script: Fly Redis -> Dragonfly
# This script performs live replication-based migration with health checks

set -e

# ============================================================================
# CONFIGURATION - UPDATE THESE VALUES
# ============================================================================

# Fly Redis Instance
FLY_REDIS_HOST="your-fly-redis.fly.dev"
FLY_REDIS_PORT="6379"
FLY_REDIS_PASSWORD="your-fly-password"

# Dragonfly Cluster (Master)
DRAGONFLY_MASTER_HOST="your-dragonfly-master-ip"
DRAGONFLY_MASTER_PORT="6379"
DRAGONFLY_MASTER_PASSWORD="dragonfly-password"  # Leave empty if no auth

# Dragonfly Cluster (Replica)
DRAGONFLY_REPLICA_HOST="your-dragonfly-replica-ip"
DRAGONFLY_REPLICA_PORT="6379"


# Migration settings
SYNC_CHECK_INTERVAL=5  # seconds between sync checks
MAX_WAIT_TIME=3600     # max seconds to wait for initial sync (1 hour)

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1"
}

error() {
    log "ERROR: $1"
    exit 1
}

# Execute redis-cli command with auth handling
redis_cmd() {
    local host=$1
    local port=$2
    local password=$3
    shift 3
    
    if [ -n "$password" ]; then
        redis-cli -h "$host" -p "$port" -a "$password" --no-auth-warning "$@"
    else
        redis-cli -h "$host" -p "$port" "$@"
    fi
}

# Check if redis/dragonfly instance is accessible
check_connectivity() {
    local host=$1
    local port=$2
    local password=$3
    local name=$4
    
    log "Checking connectivity to $name ($host:$port)..."
    if redis_cmd "$host" "$port" "$password" PING | grep -q "PONG"; then
        log "success: Successfully connected to $name"
        return 0
    else
        error "✗ Cannot connect to $name"
        return 1
    fi
}

# Get key count from instance
get_key_count() {
    local host=$1
    local port=$2
    local password=$3
    
    redis_cmd "$host" "$port" "$password" DBSIZE
}

# Get replication info
get_replication_info() {
    local host=$1
    local port=$2
    local password=$3
    
    redis_cmd "$host" "$port" "$password" INFO replication
}

# Check if replication is in sync
is_in_sync() {
    local host=$1
    local port=$2
    local password=$3
    
    local info=$(get_replication_info "$host" "$port" "$password")
    local role=$(echo "$info" | grep "role:" | cut -d: -f2 | tr -d '\r')
    local master_link=$(echo "$info" | grep "master_link_status:" | cut -d: -f2 | tr -d '\r')
    
    if [ "$role" = "slave" ] && [ "$master_link" = "up" ]; then
        return 0
    else
        return 1
    fi
}

# Get replication lag
get_replication_lag() {
    local host=$1
    local port=$2
    local password=$3
    
    local info=$(get_replication_info "$host" "$port" "$password")
    echo "$info" | grep "master_repl_offset:" | cut -d: -f2 | tr -d '\r'
}

# Setup replication on Dragonfly
setup_replication() {
    local df_host=$1
    local df_port=$2
    local df_password=$3
    local source_host=$4
    local source_port=$5
    local source_password=$6
    local name=$7
    
    log "Setting up replication on $name..."
    log "Source: $source_host:$source_port -> Target: $df_host:$df_port"
    
    # Configure master auth if needed
    if [ -n "$source_password" ]; then
        redis_cmd "$df_host" "$df_port" "$df_password" CONFIG SET masterauth "$source_password" > /dev/null
        log "success: Configured source authentication"
    fi
    
    # Start replication
    redis_cmd "$df_host" "$df_port" "$df_password" REPLICAOF "$source_host" "$source_port" > /dev/null
    log "success: Replication configured on $name"
}

# Wait for initial sync to complete
wait_for_sync() {
    local host=$1
    local port=$2
    local password=$3
    local name=$4
    
    log "Waiting for initial sync to complete on $name..."
    local elapsed=0
    
    while [ $elapsed -lt $MAX_WAIT_TIME ]; do
        if is_in_sync "$host" "$port" "$password"; then
            log "success: $name is now in sync!"
            return 0
        fi
        
        log "Still syncing... (waited ${elapsed}s)"
        sleep $SYNC_CHECK_INTERVAL
        elapsed=$((elapsed + SYNC_CHECK_INTERVAL))
    done
    
    error "Timeout waiting for sync on $name after ${MAX_WAIT_TIME}s"
}

# Monitor replication lag
monitor_lag() {
    local host=$1
    local port=$2
    local password=$3
    local name=$4
    
    local lag=$(get_replication_lag "$host" "$port" "$password")
    log "$name replication offset: $lag"
}

# Promote Dragonfly to master
promote_to_master() {
    local host=$1
    local port=$2
    local password=$3
    local name=$4
    
    log "Promoting $name to master..."
    redis_cmd "$host" "$port" "$password" REPLICAOF NO ONE > /dev/null
    log "success: $name promoted to master"
}

# Verify data consistency
verify_data() {
    local source_host=$1
    local source_port=$2
    local source_password=$3
    local target_host=$4
    local target_port=$5
    local target_password=$6
    local name=$7
    
    log "Verifying data consistency for $name..."
    
    local source_keys=$(get_key_count "$source_host" "$source_port" "$source_password")
    local target_keys=$(get_key_count "$target_host" "$target_port" "$target_password")
    
    log "Source keys: $source_keys, Target keys: $target_keys"
    
    if [ "$source_keys" -eq "$target_keys" ]; then
        log "success: Key count matches!"
    else
        log "warning: Key count mismatch (difference: $((source_keys - target_keys)))"
    fi
}

# ============================================================================
# MAIN MIGRATION PROCESS
# ============================================================================

main() {
    log "=========================================="
    log "Starting Zero-Downtime Migration Process"
    log "=========================================="
    
    # Phase 1: Pre-flight checks
    log ""
    log "PHASE 1: Pre-flight connectivity checks"
    log "----------------------------------------"
    
    check_connectivity "$FLY_REDIS_HOST" "$FLY_REDIS_PORT" "$FLY_REDIS_PASSWORD" "Fly Redis"
    check_connectivity "$DRAGONFLY_MASTER_HOST" "$DRAGONFLY_MASTER_PORT" "$DRAGONFLY_MASTER_PASSWORD" "Dragonfly Master"
    check_connectivity "$DRAGONFLY_REPLICA_HOST" "$DRAGONFLY_REPLICA_PORT" "$DRAGONFLY_MASTER_PASSWORD" "Dragonfly Replica"
    
    log "success: All instances are accessible"
    
    # Phase 2: Setup replication
    log ""
    log "PHASE 2: Setting up replication"
    log "--------------------------------"
    
    setup_replication "$DRAGONFLY_MASTER_HOST" "$DRAGONFLY_MASTER_PORT" "$DRAGONFLY_MASTER_PASSWORD" \
                     "$FLY_REDIS_HOST" "$FLY_REDIS_PORT" "$FLY_REDIS_PASSWORD" \
                     "Dragonfly 1 Master"
    
    # Phase 3: Wait for initial sync
    log ""
    log "PHASE 3: Waiting for initial synchronization"
    log "--------------------------------------------"
    
    wait_for_sync "$DRAGONFLY_MASTER_HOST" "$DRAGONFLY_MASTER_PORT" "$DRAGONFLY_MASTER_PASSWORD" "Dragonfly Master"

    # Phase 4: Monitor replication lag
    log ""
    log "PHASE 4: Monitoring replication lag"
    log "------------------------------------"
    
    monitor_lag "$DRAGONFLY_MASTER_HOST" "$DRAGONFLY_MASTER_PORT" "$DRAGONFLY_MASTER_PASSWORD" "Dragonfly Master"
    # Phase 5: Verify Dragonfly replicas are syncing
    log ""
    log "PHASE 5: Verifying internal Dragonfly replication"
    log "--------------------------------------------------"
    
    sleep 5  # Give replicas time to sync
    
    local df_replica_info=$(get_replication_info "$DRAGONFLY_REPLICA_HOST" "$DRAGONFLY_REPLICA_PORT" "$DRAGONFLY_MASTER_PASSWORD")
    
    log "Dragonfly Replica status:"
    echo "$df_replica_info" | grep -E "role:|master_link_status:" | sed 's/^/  /'
    
    # Phase 6: Final verification before cutover
    log ""
    log "PHASE 6: Final data verification"
    log "---------------------------------"
    
    verify_data "$FLY_REDIS_HOST" "$FLY_REDIS_PORT" "$FLY_REDIS_PASSWORD" \
                "$DRAGONFLY_MASTER_HOST" "$DRAGONFLY_MASTER_PORT" "$DRAGONFLY_MASTER_PASSWORD" \
                "Migration" 
    
    # Phase 7: Ready for cutover
    log ""
    log "=========================================="
    log "MIGRATION PREPARATION COMPLETE!"
    log "=========================================="
    log ""
    log "Your Dragonfly instances are now in sync with Fly Redis."
    log "Replication is live and ongoing."
    log ""
    log "NEXT STEPS:"
    log "1. Update your application configuration to use Dragonfly endpoints:"
    log "   - Cluster Master: $DRAGONFLY_MASTER_HOST:$DRAGONFLY_MASTER_PORT"
    log "   - Cluster Replica: $DRAGONFLY_REPLICA_HOST:$DRAGONFLY_REPLICA_PORT"
    log ""
    log "2. Deploy your application with new endpoints (no downtime - replication is live)"
    log ""
    log "3. After verifying application is working, run the cutover script:"
    log "   ./migrate.sh cutover"
    log ""
    log "Replication will continue until you run the cutover."
    log "=========================================="
}

# ============================================================================
# CUTOVER PROCESS
# ============================================================================

cutover() {
    log "=========================================="
    log "Starting Cutover Process"
    log "=========================================="
    
    read -p "Have you updated and deployed your application to use Dragonfly? (yes/no): " confirm
    if [ "$confirm" != "yes" ]; then
        log "Cutover cancelled. Update your application first."
        exit 0
    fi
    
    log ""
    log "Promoting Dragonfly instances to master..."
    log "-------------------------------------------"
    
    promote_to_master "$DRAGONFLY_MASTER_HOST" "$DRAGONFLY_MASTER_PORT" "$DRAGONFLY_MASTER_PASSWORD" "Dragonfly Master"
    
    log ""
    log "Final verification..."
    log "---------------------"
    
    verify_data "$FLY_REDIS_HOST" "$FLY_REDIS_PORT" "$FLY_REDIS_PASSWORD" \
                "$DRAGONFLY_MASTER_HOST" "$DRAGONFLY_MASTER_PORT" "$DRAGONFLY_MASTER_PASSWORD" \
                "Migration"
    
    log ""
    log "=========================================="
    log "CUTOVER COMPLETE!"
    log "=========================================="
    log ""
    log "Your Dragonfly clusters are now independent masters."
    log ""
    log "NEXT STEPS:"
    log "1. Monitor your application for any issues"
    log "2. After 24-48 hours of stable operation, you can:"
    log "   - Decommission Fly Redis instances"
    log "   - Remove Fly Redis from your infrastructure"
    log ""
    log "Your Fly Redis instances are still running but no longer being replicated."
    log "=========================================="
}

# ============================================================================
# SCRIPT ENTRY POINT
# ============================================================================

if [ "$1" = "cutover" ]; then
    cutover
else
    main
fi