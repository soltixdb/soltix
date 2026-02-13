#!/bin/bash

# Soltix Benchmark Test Runner
# Usage: ./scripts/run_benchmarks.sh [OPTIONS]

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Default values
BENCHTIME="1s"
COUNT=1
MEMPROFILE=""
CPUPROFILE=""
OUTPUT_DIR="benchmark_results"
VERBOSE=false
PACKAGES=""

# Help function
show_help() {
    cat << EOF
Usage: ${0##*/} [OPTIONS]

Run benchmark tests for Soltix timeseries database

OPTIONS:
    -h, --help              Show this help message
    -t, --time DURATION     Benchmark duration (default: 1s)
    -c, --count N           Run benchmarks N times (default: 1)
    -m, --memprofile FILE   Write memory profile to FILE
    -p, --cpuprofile FILE   Write CPU profile to FILE
    -o, --output DIR        Output directory for results (default: benchmark_results)
    -v, --verbose           Verbose output
    -a, --all               Run all benchmarks (default)
    --wal                   Run only WAL benchmarks
    --storage               Run only storage benchmarks
    --aggregation           Run only aggregation benchmarks
    --coordinator           Run only coordinator benchmarks
    --compression           Run only compression benchmarks

EXAMPLES:
    # Run all benchmarks with default settings
    ./scripts/run_benchmarks.sh

    # Run WAL benchmarks for 5 seconds, 3 times
    ./scripts/run_benchmarks.sh --wal -t 5s -c 3

    # Run all benchmarks with memory profiling
    ./scripts/run_benchmarks.sh -m mem.prof

    # Run all benchmarks with CPU and memory profiling
    ./scripts/run_benchmarks.sh -p cpu.prof -m mem.prof

    # Run specific component benchmarks with verbose output
    ./scripts/run_benchmarks.sh --storage --aggregation -v

EOF
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -h|--help)
            show_help
            exit 0
            ;;
        -t|--time)
            BENCHTIME="$2"
            shift 2
            ;;
        -c|--count)
            COUNT="$2"
            shift 2
            ;;
        -m|--memprofile)
            MEMPROFILE="$2"
            shift 2
            ;;
        -p|--cpuprofile)
            CPUPROFILE="$2"
            shift 2
            ;;
        -o|--output)
            OUTPUT_DIR="$2"
            shift 2
            ;;
        -v|--verbose)
            VERBOSE=true
            shift
            ;;
        -a|--all)
            PACKAGES="all"
            shift
            ;;
        --wal)
            PACKAGES="$PACKAGES wal"
            shift
            ;;
        --storage)
            PACKAGES="$PACKAGES storage"
            shift
            ;;
        --aggregation)
            PACKAGES="$PACKAGES aggregation"
            shift
            ;;
        --coordinator)
            PACKAGES="$PACKAGES coordinator"
            shift
            ;;
        --compression)
            PACKAGES="$PACKAGES compression"
            shift
            ;;
        *)
            echo -e "${RED}Unknown option: $1${NC}"
            show_help
            exit 1
            ;;
    esac
done

# Default to all packages if none specified
if [ -z "$PACKAGES" ] || [ "$PACKAGES" = "all" ]; then
    PACKAGES="wal storage aggregation coordinator compression"
fi

# Create output directory
mkdir -p "$OUTPUT_DIR"

# Print configuration
echo -e "${BLUE}=== Soltix Benchmark Tests ===${NC}"
echo -e "${GREEN}Configuration:${NC}"
echo "  Benchmark time: $BENCHTIME"
echo "  Count: $COUNT"
echo "  Output directory: $OUTPUT_DIR"
echo "  Packages: $PACKAGES"
[ -n "$MEMPROFILE" ] && echo "  Memory profile: $MEMPROFILE"
[ -n "$CPUPROFILE" ] && echo "  CPU profile: $CPUPROFILE"
echo ""

# Build benchmark command options
BENCH_OPTS="-bench=. -benchtime=$BENCHTIME -count=$COUNT -benchmem"
[ "$VERBOSE" = true ] && BENCH_OPTS="$BENCH_OPTS -v"

# Timestamp for filenames
TIMESTAMP=$(date +%Y%m%d_%H%M%S)

# Function to run benchmarks for a package
run_benchmark() {
    local package=$1
    local package_path="./internal/$package"
    
    echo -e "${YELLOW}Running $package benchmarks...${NC}"
    
    local output_file="$OUTPUT_DIR/${package}_${TIMESTAMP}.txt"
    local bench_cmd="go test $package_path $BENCH_OPTS"
    
    # Add profiling options
    if [ -n "$MEMPROFILE" ]; then
        bench_cmd="$bench_cmd -memprofile=$OUTPUT_DIR/${package}_mem_${TIMESTAMP}.prof"
    fi
    if [ -n "$CPUPROFILE" ]; then
        bench_cmd="$bench_cmd -cpuprofile=$OUTPUT_DIR/${package}_cpu_${TIMESTAMP}.prof"
    fi
    
    # Run benchmark and save output
    if [ "$VERBOSE" = true ]; then
        echo "Command: $bench_cmd"
    fi
    
    $bench_cmd 2>&1 | tee "$output_file"
    
    if [ ${PIPESTATUS[0]} -eq 0 ]; then
        echo -e "${GREEN}✓ $package benchmarks completed${NC}"
        echo -e "  Results saved to: $output_file"
    else
        echo -e "${RED}✗ $package benchmarks failed${NC}"
        return 1
    fi
    echo ""
}

# Run benchmarks for each package
FAILED_PACKAGES=""
for package in $PACKAGES; do
    if ! run_benchmark "$package"; then
        FAILED_PACKAGES="$FAILED_PACKAGES $package"
    fi
done

# Summary
echo -e "${BLUE}=== Benchmark Summary ===${NC}"
if [ -z "$FAILED_PACKAGES" ]; then
    echo -e "${GREEN}All benchmarks completed successfully!${NC}"
else
    echo -e "${RED}Some benchmarks failed:$FAILED_PACKAGES${NC}"
    exit 1
fi

# Generate comparison report if count > 1
if [ "$COUNT" -gt 1 ]; then
    echo ""
    echo -e "${BLUE}=== Benchmark Comparison ===${NC}"
    echo "You can compare results using benchstat:"
    echo "  go install golang.org/x/perf/cmd/benchstat@latest"
    for package in $PACKAGES; do
        echo "  benchstat $OUTPUT_DIR/${package}_${TIMESTAMP}.txt"
    done
fi

# Profile analysis hints
if [ -n "$MEMPROFILE" ] || [ -n "$CPUPROFILE" ]; then
    echo ""
    echo -e "${BLUE}=== Profile Analysis ===${NC}"
    echo "Analyze profiles using pprof:"
    if [ -n "$CPUPROFILE" ]; then
        echo "  go tool pprof -http=:8080 $OUTPUT_DIR/*_cpu_*.prof"
    fi
    if [ -n "$MEMPROFILE" ]; then
        echo "  go tool pprof -http=:8080 $OUTPUT_DIR/*_mem_*.prof"
    fi
fi

echo ""
echo -e "${GREEN}Done! Results are in $OUTPUT_DIR/${NC}"
