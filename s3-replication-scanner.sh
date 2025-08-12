#!/bin/bash

# DOCUMENTATION:
#  ./s3-replication-scanner.sh
# Environment variable for replication status filter
#   - REPLICATION_STATUS_FILTER: Filter objects by status (default: "PENDING")
#   - Valid values: "PENDING", "FAILED", "COMPLETED", "" (empty for all)
#   - HOST: Metastore host endpoint (default: "localhost:9000")
#   - MAX_KEYS: Maximum keys per API request (default: 1000)
#   - BUCKET_LIST: Comma-separated bucket names to process (optional)


REPLICATION_STATUS_FILTER=${REPLICATION_STATUS_FILTER:-"PENDING"}

HOST=${HOST:-"localhost:9000"}
MAX_KEYS=${MAX_KEYS:-1000}
BUCKET_LIST=${BUCKET_LIST:-""}

# Counters for summary
BUCKETS_PROCESSED=0
BUCKETS_FAILED=0
OBJECTS_FOUND=0

echo "Filtering objects with replication status: '$REPLICATION_STATUS_FILTER'"
echo "Using host: $HOST"
echo "Max keys per request: $MAX_KEYS"
if [ ! -z "$BUCKET_LIST" ]; then
    echo "Using provided bucket list: $BUCKET_LIST"
fi
echo "=================================="

# Get list of buckets - either from parameter or fetch all
if [ ! -z "$BUCKET_LIST" ]; then
    # Convert comma-separated list to space-separated
    buckets=$(echo "$BUCKET_LIST" | tr ',' ' ')
    echo "Using provided buckets: $buckets"
else
    # Get list of all buckets
    echo "Fetching bucket list from metastore..."
    buckets=$(curl -f "$HOST/default/metastore" 2>/dev/null | jq -r '.[].key' 2>/dev/null)
    curl_exit_code=$?
    
    if [ $curl_exit_code -ne 0 ]; then
        echo "ERROR: Failed to connect to metastore at $HOST (curl exit code: $curl_exit_code)"
        echo "Partial results: Unable to fetch bucket list"
        exit 1
    fi
    
    if [ -z "$buckets" ]; then
        echo "WARNING: No buckets found in metastore"
        exit 0
    fi
    echo "Found buckets from metastore"
fi

# Function to check if bucket has replication enabled
has_replication_enabled() {
    local bucket=$1
    # Remove db/ prefix if present
    local clean_bucket="${bucket#db/}"
    
    local replication_config=$(curl -f "$HOST/default/attributes/$clean_bucket" 2>/dev/null | jq '.replicationConfiguration' 2>/dev/null)
    local curl_exit_code=$?
    
    if [ $curl_exit_code -ne 0 ]; then
        echo "  WARNING: Failed to fetch attributes for bucket $clean_bucket (curl exit code: $curl_exit_code)" >&2
        return 2  # error state
    fi
    
    if [ "$replication_config" != "null" ] && [ "$replication_config" != "" ]; then
        return 0  # has replication config
    else
        return 1  # no replication config
    fi
}

# Function to get objects with specific replication status (with pagination support)
get_objects_with_status() {
    local bucket=$1
    local status_filter=$2
    local marker=$3
    
    # Remove db/ prefix if present
    local clean_bucket="${bucket#db/}"
    
    # Build URL with pagination parameters
    local url="$HOST/default/bucket/$clean_bucket?maxKeys=$MAX_KEYS"
    if [ ! -z "$marker" ]; then
        url="$url&marker=$marker"
    fi
    
    echo "  Fetching from: $url" >&2
    
    # Get objects in the bucket with pagination
    local response=$(curl -f "$url" 2>/dev/null)
    local curl_exit_code=$?
    
    if [ $curl_exit_code -ne 0 ]; then
        echo "  ERROR: Failed to fetch objects from bucket $clean_bucket (curl exit code: $curl_exit_code)" >&2
        return 1
    fi
    
    local objects=$(echo "$response" | jq -r '.Contents[]?' 2>/dev/null)
    local is_truncated=$(echo "$response" | jq -r '.IsTruncated // false' 2>/dev/null)
    local last_key=$(echo "$objects" | jq -r '.key' | tail -n 1 2>/dev/null)
    
    if [ -z "$objects" ]; then
        echo "  No objects found in bucket $clean_bucket" >&2
        return 0
    fi
    
    # Filter objects by replication status
    local filtered=$(echo "$objects" | jq -r --arg status "$status_filter" '
        select(.value | fromjson | .replicationInfo.status == $status) | 
        (.key | split("\u0000")) as $key_parts |
        {
            bucket: "'$clean_bucket'", 
            key: ($key_parts[0] // .key), 
            versionId: ($key_parts[1] // ""), 
            replicationStatus: (.value | fromjson | .replicationInfo.status)
        }
    ' 2>/dev/null)
    
    # Output filtered results if any
    if [ ! -z "$filtered" ]; then
        echo "$filtered"
    fi
    
    # Continue pagination if truncated
    if [ "$is_truncated" = "true" ] && [ ! -z "$last_key" ]; then
        echo "  More results available, next marker: $last_key" >&2
        get_objects_with_status "$bucket" "$status_filter" "$last_key"
        return $?
    fi
    
    return 0
}

# Main logic
echo "Buckets with replication enabled:"
for bucket in $buckets; do
    if has_replication_enabled "$bucket"; then
        # Remove db/ prefix for display
        clean_bucket="${bucket#db/}"
        echo "- $clean_bucket"
        
        # Get objects with the specified replication status
        filtered_objects=$(get_objects_with_status "$bucket" "$REPLICATION_STATUS_FILTER")
        
        if [ ! -z "$filtered_objects" ]; then
            echo "  Objects with replication status '$REPLICATION_STATUS_FILTER':"
            echo "$filtered_objects" | jq -r '
                "    Bucket: " + .bucket + " | Key: " + .key + " | VersionId: " + .versionId + " | Status: " + .replicationStatus
            '
        else
            echo "    No objects with replication status '$REPLICATION_STATUS_FILTER'"
        fi
        echo ""
    fi
done