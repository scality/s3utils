#!/bin/bash
#
# list-buckets-with-replication.sh
#
# Lists all buckets with replication enabled by querying the S3 metadata API.
# Output is a JSON file with metadata and results.
#
# Usage: ./list-buckets-with-replication.sh
#
# Environment variables:
#   BUCKETD_HOST  - Bucketd hostname (default: localhost)
#   BUCKETD_PORT  - Bucketd port (default: 9000)
#   BATCH_SIZE    - Number of parallel requests (default: 10)
#   OUTPUT_FILE   - Output file path (default: buckets-with-replication.json)
#

set -e

# ===========================================================================
# Configuration
# ===========================================================================
BUCKETD_HOST="${BUCKETD_HOST:-localhost}"
BUCKETD_PORT="${BUCKETD_PORT:-9000}"
BUCKETD_URL="http://${BUCKETD_HOST}:${BUCKETD_PORT}"
BATCH_SIZE="${BATCH_SIZE:-10}" # low to not overload bucketd
OUTPUT_FILE="${OUTPUT_FILE:-buckets-with-replication.json}"

# Runtime variables
TMP_DIR=$(mktemp -d)
START_TIME=$(date +%s)
TIMESTAMP=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
FETCH_ERRORS=0

# Cleanup temporary directory on exit
trap "rm -rf $TMP_DIR" EXIT

# ===========================================================================
# Helper functions
# ===========================================================================

log() {
    echo "$@"
}

log_error() {
    echo "Error: $@" >&2
}

check_dependencies() {
    local missing=0
    for cmd in curl jq; do
        if ! command -v "$cmd" &> /dev/null; then
            log_error "$cmd is required but not installed"
            missing=1
        fi
    done
    return $missing
}

check_bucketd_connection() {
    log "Checking bucketd connectivity..."
    if ! curl -s --connect-timeout 5 "$BUCKETD_URL/_/healthcheck" > /dev/null 2>&1; then
        log_error "Cannot connect to bucketd at $BUCKETD_URL"
        return 1
    fi
    log "Connected to bucketd"
}

# Fetch all bucket names from the users..bucket database
# Handles pagination for large deployments
# Exits script on failure
fetch_bucket_list() {
    local marker=""
    local is_truncated="true"

    while [ "$is_truncated" = "true" ]; do
        local url="$BUCKETD_URL/default/bucket/users..bucket"
        [ -n "$marker" ] && url="${url}?marker=${marker}"

        local response
        response=$(curl -sf "$url") || {
            log_error "Failed to fetch bucket list from users..bucket"
            exit 1
        }

        # Extract bucket names from response
        # Keys are formatted as "{canonicalId}..|..{bucketName}"
        echo "$response" | jq -r '.Contents[].key // empty' | sed 's/.*\.\.|\.\.//'

        # Check if there are more pages
        is_truncated=$(echo "$response" | jq -r '.IsTruncated')
        if [ "$is_truncated" = "true" ]; then
            marker=$(echo "$response" | jq -r '.Contents[-1].key | @uri')
        fi
    done
}

# Fetch bucket attributes for a batch of buckets
fetch_bucket_attributes_batch() {
    local buckets=("$@")
    local curl_args=()

    for bucket in "${buckets[@]}"; do
        curl_args+=("-o" "$TMP_DIR/attr_${bucket}.json" "$BUCKETD_URL/default/attributes/$bucket")
    done

    # Try parallel fetch first, fall back to sequential if not supported
    curl -s --parallel --parallel-max "$BATCH_SIZE" "${curl_args[@]}" 2>/dev/null \
        || curl -s "${curl_args[@]}" 2>/dev/null \
        || true
}

# Check if a bucket has replication enabled
# Returns 0 if replication is enabled, 1 otherwise
has_replication_enabled() {
    local attr_file="$1"

    # Check if file exists and is not empty
    if [ ! -f "$attr_file" ] || [ ! -s "$attr_file" ]; then
        return 1
    fi

    # Check if replicationConfiguration exists
    local repl_config
    repl_config=$(jq -r '.replicationConfiguration // empty' "$attr_file" 2>/dev/null)
    if [ -z "$repl_config" ] || [ "$repl_config" = "null" ]; then
        return 1
    fi

    # Check if any rule is enabled
    # Handle different API formats: enabled:true, status:"Enabled", Status:"Enabled"
    local enabled_count
    enabled_count=$(jq -r '
        .replicationConfiguration.rules // .replicationConfiguration.Rules // []
        | [.[] | select(.enabled == true or .status == "Enabled" or .Status == "Enabled")]
        | length
    ' "$attr_file" 2>/dev/null)

    if [ "$enabled_count" -gt 0 ]; then
        return 0
    fi

    return 1
}

# Extract replication info from bucket attributes
extract_replication_info() {
    local attr_file="$1"
    local bucket="$2"

    local owner
    local owner_display_name
    local full_role
    local source_role

    owner=$(jq -r '.owner // "unknown"' "$attr_file")
    owner_display_name=$(jq -r '.ownerDisplayName // "unknown"' "$attr_file")

    # Role format is "sourceRole,destRole" - extract source role (before comma)
    full_role=$(jq -r '.replicationConfiguration.role // .replicationConfiguration.Role // ""' "$attr_file")
    source_role=$(echo "$full_role" | cut -d',' -f1)

    # Build JSON result object
    jq -n \
        --arg bucket "$bucket" \
        --arg owner "$owner" \
        --arg ownerDisplayName "$owner_display_name" \
        --arg sourceRole "$source_role" \
        '{
            bucket: $bucket,
            owner: $owner,
            ownerDisplayName: $ownerDisplayName,
            sourceRole: $sourceRole
        }'
}


# Build the final output JSON with metadata
build_output() {
    local bucket_count="$1"
    local repl_count="$2"
    local duration="$3"
    local fetch_errors="$4"

    jq -n \
        --arg timestamp "$TIMESTAMP" \
        --argjson durationSeconds "$duration" \
        --arg bucketdUrl "$BUCKETD_URL" \
        --argjson totalBucketsScanned "$bucket_count" \
        --argjson bucketsWithReplication "$repl_count" \
        --argjson bucketsWithoutReplication "$((bucket_count - repl_count - fetch_errors))" \
        --argjson fetchErrors "$fetch_errors" \
        --slurpfile results "$TMP_DIR/results.ndjson" \
        '{
            metadata: {
                timestamp: $timestamp,
                durationSeconds: $durationSeconds,
                bucketdUrl: $bucketdUrl,
                counts: {
                    totalBucketsScanned: $totalBucketsScanned,
                    bucketsWithReplication: $bucketsWithReplication,
                    bucketsWithoutReplication: $bucketsWithoutReplication,
                    fetchErrors: $fetchErrors
                }
            },
            results: $results
        }'
}

print_summary() {
    local bucket_count="$1"
    local repl_count="$2"
    local duration="$3"
    local fetch_errors="$4"

    log ""
    log "=== Summary ==="
    log "Total buckets scanned:      $bucket_count"
    log "  With replication:         $repl_count"
    log "  Without replication:      $((bucket_count - repl_count - fetch_errors))"
    log "  Fetch errors:             $fetch_errors"
    log "Duration:                   ${duration}s"
    log "Output saved to: $OUTPUT_FILE"
    log ""
    log "Done."
}

# ===========================================================================
# Main
# ===========================================================================

main() {
    log "=== List Buckets with Replication Enabled ==="
    log "Bucketd: $BUCKETD_URL"
    log "Batch size: $BATCH_SIZE"
    log "Output file: $OUTPUT_FILE"
    log ""

    # Check prerequisites
    check_dependencies || exit 1
    check_bucketd_connection || exit 1

    # Fetch list of all buckets
    log ""
    log "Step 1: Fetching bucket list..."

    # Stream bucket list to file (memory-efficient for large bucket counts)
    fetch_bucket_list > "$TMP_DIR/buckets.txt"

    local bucket_count
    bucket_count=$(wc -l < "$TMP_DIR/buckets.txt" | tr -d ' ')

    log "Found $bucket_count buckets (excluding internal buckets)"

    if [ "$bucket_count" -eq 0 ]; then
        log "No buckets found"
        echo '{"metadata":{},"results":[]}' > "$OUTPUT_FILE"
        exit 0
    fi

    # Process buckets in batches
    log ""
    log "Step 2: Fetching bucket attributes in batches of $BATCH_SIZE..."

    local processed=0
    local batch_num=0
    local batch_start=1

    # Initialize empty results file (NDJSON format: one JSON object per line)
    : > "$TMP_DIR/results.ndjson"

    while [ "$batch_start" -le "$bucket_count" ]; do
        batch_num=$((batch_num + 1))

        # Read current batch from file (memory-efficient)
        local batch=()
        while IFS= read -r bucket; do
            batch+=("$bucket")
        done < <(sed -n "${batch_start},$((batch_start + BATCH_SIZE - 1))p" "$TMP_DIR/buckets.txt")

        local batch_count=${#batch[@]}

        # Fetch attributes for this batch
        fetch_bucket_attributes_batch "${batch[@]}"

        # Process each bucket in the batch
        for bucket in "${batch[@]}"; do
            local attr_file="$TMP_DIR/attr_${bucket}.json"

            # Check if fetch failed (file missing or empty)
            if [ ! -s "$attr_file" ]; then
                FETCH_ERRORS=$((FETCH_ERRORS + 1))
                continue
            fi

            if has_replication_enabled "$attr_file"; then
                extract_replication_info "$attr_file" "$bucket" >> "$TMP_DIR/results.ndjson"
            fi

            # Cleanup attribute file
            rm -f "$attr_file"
        done

        processed=$((processed + batch_count))
        batch_start=$((batch_start + BATCH_SIZE))
        log "  Processed $processed/$bucket_count buckets (batch $batch_num)"
    done

    # Calculate final stats
    local repl_count
    repl_count=$(jq -s 'length' "$TMP_DIR/results.ndjson" 2>/dev/null || echo 0)

    local end_time
    end_time=$(date +%s)

    local duration=$((end_time - START_TIME))

    # Build and save final output
    build_output "$bucket_count" "$repl_count" "$duration" "$FETCH_ERRORS" > "$OUTPUT_FILE"

    # Print summary
    print_summary "$bucket_count" "$repl_count" "$duration" "$FETCH_ERRORS"
}

main "$@"
