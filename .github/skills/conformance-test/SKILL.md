# GRIP Conformance Testing Workflow

## Overview
This skill describes how to use the conformance testing framework in the GRIP project to identify issues and verify engine functionality across different backends.

## Process
1. **Environment Setup**
   - Ensure Docker is installed and running
   - Verify Go environment is set up for building GRIP
   - Confirm Python dependencies are available

2. **Backend Selection**
   - Choose a backend to test against (arango, mongo, postgres, badger, pebble, sqlite)
   - The conformance.sh script supports multiple backends for comprehensive testing

3. **Test Execution**
   - Run `./conformance/conformance.sh <backend>` to execute all conformance tests
   - Tests are automatically filtered by the `ot_` prefix
   - Backend-specific exclusions may be applied (e.g., mongo excludes nested_index)

4. **Result Analysis**
   - Test results are displayed in console output
   - Detailed YAML results can be written to a file using `--output` flag
   - Server logs are captured for debugging failed tests

## Decision Points and Branching Logic
- If no backend is specified, show help information
- If an invalid backend is specified, list available backends and exit with error
- For each backend:
  - Start appropriate Docker container (if needed)
  - Build GRIP binary if not already built
  - Start GRIP server with correct configuration
  - Run conformance tests against the running server
  - Capture and report results

## Quality Criteria and Completion Checks
- All conformance tests should pass for a successful run
- Server logs are captured when tests fail for debugging purposes
- Test output shows number of passed vs total tests
- Exit code reflects test success (0) or failure (non-zero)
- Docker containers are properly cleaned up after testing

## Usage Examples
```
# Run conformance tests against ArangoDB backend
./conformance/conformance.sh arango

# Run conformance tests and output results to file
./conformance/conformance.sh mongo --output test_results.yaml

# List available backends
./conformance/conformance.sh --list
```

## Output
 - A YAML file with client side testing information can be found at `filename` using the `--output filename` flag passed to conformance.sh
 - A STDERR dump of the GRIP server logs can be found at `filename.server_log`

## Troubleshooting
- If Docker containers fail to start, check Docker installation and port availability
- If server fails to start, examine `/tmp/grip-server.log` for details
- If tests fail, review the console output and server logs for specific error information
- Ensure all dependencies are installed (Go, Python packages, Docker)