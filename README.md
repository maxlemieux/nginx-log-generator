# nginx-log-generator
Testing tool for generating sample nginx log entries to a file

Build: 

```
make
```

Usage: ./nginx_log_generator <log_file_path> [options]
Required:
  <log_file_path>   Path to the output log file
Options:
  --threads N      Number of threads (default: 8)
  --buffer N       Buffer size in MB per thread (default: 16MB)
  --entries N      Total log entries to generate (default: 5000000)
  --rate N         Rate limit in KB/s (0 = no limit, default: 0)
  --overwrite      Overwrite existing log file instead of appending
  --help           Display this help message

