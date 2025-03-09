#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <pthread.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <libgen.h>
#include <errno.h>
#include <sys/time.h>    // For gettimeofday

// Configuration
#define DEFAULT_NUM_THREADS 8
#define DEFAULT_BUFFER_SIZE (1024 * 1024 * 16)  // 16MB per thread buffer
#define DEFAULT_TOTAL_ENTRIES 5000000           // Approx 5GB total
#define LINE_ESTIMATE_SIZE 300                  // Estimated bytes per log entry
#define DEFAULT_RATE_LIMIT 0                    // No rate limit by default (KB/s)

// Thread data structure
typedef struct {
    int thread_id;
    char *buffer;
    size_t buffer_size;
    size_t entries_per_thread;
    const char *log_file;
    long bytes_written;
    int rate_limit;       // Rate limit in KB/s per thread
    pthread_mutex_t *file_mutex;  // Mutex for file access
} thread_data_t;

// Sample data arrays
const char *ips[] = {
    "192.168.1.101", "10.0.0.54", "172.16.32.12", "192.168.0.87", 
    "10.10.10.25", "8.8.8.8", "1.1.1.1"
};
const int num_ips = sizeof(ips) / sizeof(ips[0]);

const char *user_agents[] = {
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (iPad; CPU OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Mobile/15E148 Safari/604.1"
};
const int num_user_agents = sizeof(user_agents) / sizeof(user_agents[0]);

const char *endpoints[] = {
    "/api/users", "/api/products", "/api/orders", "/api/auth/login", 
    "/api/auth/logout", "/api/profile", "/api/settings", "/api/dashboard", 
    "/", "/about", "/contact"
};
const int num_endpoints = sizeof(endpoints) / sizeof(endpoints[0]);

const char *status_codes[] = {
    "200", "201", "204", "400", "401", "403", "404", "500"
};
const int num_status_codes = sizeof(status_codes) / sizeof(status_codes[0]);

const char *http_methods[] = {
    "GET", "POST", "PUT", "DELETE", "PATCH"
};
const int num_http_methods = sizeof(http_methods) / sizeof(http_methods[0]);

// Function to create directory if it doesn't exist
int mkdirs(const char *path) {
    char tmp[256];
    char *p = NULL;
    size_t len;
    
    snprintf(tmp, sizeof(tmp), "%s", path);
    len = strlen(tmp);
    
    if (tmp[len - 1] == '/')
        tmp[len - 1] = 0;
    
    for (p = tmp + 1; *p; p++) {
        if (*p == '/') {
            *p = 0;
            mkdir(tmp, 0755);
            *p = '/';
        }
    }
    
    return mkdir(tmp, 0755);
}

// Helper function to get current time in microseconds
long long get_microseconds() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (long long)tv.tv_sec * 1000000LL + (long long)tv.tv_usec;
}

// Thread function to generate logs
void* generate_logs(void *arg) {
    thread_data_t *data = (thread_data_t *)arg;
    unsigned int seed = time(NULL) + data->thread_id;  // Different seed per thread
    FILE *fp = NULL;
    size_t total_bytes = 0;
    size_t entries_generated = 0;
    time_t now;
    struct tm *tm_info;
    
    // Rate limiting variables
    long long start_time = 0;
    size_t bytes_since_last_check = 0;
    const size_t check_interval_bytes = 64 * 1024;  // Check rate limit every ~64KB
    
    // Pre-allocate a line buffer that's reused
    char line[LINE_ESTIMATE_SIZE * 2];  // Double the estimate to be safe
    
    // Prepare a local buffer for collecting entries before writing
    size_t flush_interval = 1000;  // Number of entries before flushing to main file
    size_t buffer_entries = 0;
    char *batch_buffer = malloc(LINE_ESTIMATE_SIZE * 2 * flush_interval);
    size_t batch_buffer_used = 0;
    
    if (!batch_buffer) {
        fprintf(stderr, "Thread %d: Failed to allocate batch buffer\n", data->thread_id);
        return NULL;
    }
    
    while (entries_generated < data->entries_per_thread) {
        // Generate random components
        const char *ip = ips[rand_r(&seed) % num_ips];
        const char *user_agent = user_agents[rand_r(&seed) % num_user_agents];
        const char *endpoint = endpoints[rand_r(&seed) % num_endpoints];
        const char *status = status_codes[rand_r(&seed) % num_status_codes];
        const char *method = http_methods[rand_r(&seed) % num_http_methods];
        
        // Generate random values
        float request_time = (float)(rand_r(&seed) % 250) / 100.0f;  // 0-2.5s
        int bytes_sent = 100 + (rand_r(&seed) % 9900);  // 100-10000 bytes
        
        // Get current timestamp for each entry
        time(&now);
        tm_info = gmtime(&now);
        char timestamp[32];
        strftime(timestamp, sizeof(timestamp), "%Y-%m-%dT%H:%M:%S.000Z", tm_info);
        
        // Format log entry
        int bytes = snprintf(line, sizeof(line),
            "{\"time\":\"%s\",\"remote_addr\":\"%s\",\"method\":\"%s\","
            "\"path\":\"%s\",\"status\":%s,\"request_time\":%.2f,"
            "\"bytes_sent\":%d,\"user_agent\":\"%s\"}\n",
            timestamp, ip, method, endpoint, status, request_time, bytes_sent, user_agent);
        
        // Add to batch buffer
        if (batch_buffer_used + bytes < LINE_ESTIMATE_SIZE * 2 * flush_interval) {
            memcpy(batch_buffer + batch_buffer_used, line, bytes);
            batch_buffer_used += bytes;
            buffer_entries++;
        }
        
        total_bytes += bytes;
        bytes_since_last_check += bytes;
        entries_generated++;
        
        // Flush to file when we reach the flush interval or on last entry
        if (buffer_entries >= flush_interval || 
            entries_generated >= data->entries_per_thread) {
            
            // Lock the mutex to ensure atomic write to the shared file
            pthread_mutex_lock(data->file_mutex);
            
            // Open the shared log file in append mode
            fp = fopen(data->log_file, "a");
            if (!fp) {
                fprintf(stderr, "Thread %d: Cannot open file %s: %s\n", 
                        data->thread_id, data->log_file, strerror(errno));
                pthread_mutex_unlock(data->file_mutex);
                continue;
            }
            
            // Write the batch buffer
            if (fwrite(batch_buffer, 1, batch_buffer_used, fp) != batch_buffer_used) {
                fprintf(stderr, "Thread %d: Write error: %s\n", 
                        data->thread_id, strerror(errno));
            }
            
            // Ensure data is written to disk
            fflush(fp);
            fsync(fileno(fp));
            
            // Close the file
            fclose(fp);
            fp = NULL;
            
            // Release the mutex
            pthread_mutex_unlock(data->file_mutex);
            
            // Reset the batch buffer
            batch_buffer_used = 0;
            buffer_entries = 0;
        }
        
        // Apply rate limiting if enabled
        if (data->rate_limit > 0) {
            if (start_time == 0) {
                start_time = get_microseconds();
            }
            
            // Check rate periodically rather than every write for better performance
            if (bytes_since_last_check >= check_interval_bytes) {
                long long current_time = get_microseconds();
                long long elapsed_us = current_time - start_time;
                
                // Convert rate limit from KB/s to bytes/us
                double max_bytes = (data->rate_limit * 1024.0) * (elapsed_us / 1000000.0);
                
                if (total_bytes > max_bytes) {
                    // We're writing too fast - sleep to maintain desired rate
                    double overage_bytes = total_bytes - max_bytes;
                    long sleep_us = (long)(overage_bytes * 1000000.0 / (data->rate_limit * 1024.0));
                    
                    // Cap the sleep time to prevent extreme values
                    if (sleep_us > 1000000) sleep_us = 1000000;
                    
                    usleep(sleep_us);
                }
                
                bytes_since_last_check = 0;
            }
        }
        
        // Provide status update occasionally
        if (entries_generated % 500000 == 0) {
            long long current_time = get_microseconds();
            double elapsed_sec = (current_time - start_time) / 1000000.0;
            double current_rate = (elapsed_sec > 0) ? 
                (total_bytes / 1024.0) / elapsed_sec : 0;
                
            printf("Thread %d: Generated %zu entries (%.2f MB, %.2f KB/s)\n", 
                   data->thread_id, entries_generated, 
                   total_bytes / (1024.0 * 1024.0),
                   current_rate);
        }
    }
    
    // Clean up
    free(batch_buffer);
    
    data->bytes_written = total_bytes;
    printf("Thread %d: Completed %zu entries (%.2f MB)\n", 
           data->thread_id, entries_generated, total_bytes / (1024.0 * 1024.0));
    
    return NULL;
}

// Main function
int main(int argc, char *argv[]) {
    // Check if log file path was provided
    if (argc < 2) {
        fprintf(stderr, "Error: Log file path is required as the first argument\n");
        fprintf(stderr, "Usage: %s <log_file_path> [options]\n", argv[0]);
        fprintf(stderr, "Run with --help for more information\n");
        return 1;
    }
    
    // Get the log file path from the first argument
    const char *log_file = argv[1];
    
    // If help was requested, show help and exit
    if (strcmp(log_file, "--help") == 0) {
        printf("Usage: %s <log_file_path> [options]\n", argv[0]);
        printf("Required:\n");
        printf("  <log_file_path>   Path to the output log file\n");
        printf("Options:\n");
        printf("  --threads N      Number of threads (default: %d)\n", DEFAULT_NUM_THREADS);
        printf("  --buffer N       Buffer size in MB per thread (default: %dMB)\n", 
               (int)(DEFAULT_BUFFER_SIZE / (1024 * 1024)));
        printf("  --entries N      Total log entries to generate (default: %u)\n", (unsigned int)DEFAULT_TOTAL_ENTRIES);
        printf("  --rate N         Rate limit in KB/s (0 = no limit, default: %d)\n", DEFAULT_RATE_LIMIT);
        printf("  --overwrite      Overwrite existing log file instead of appending\n");
        printf("  --help           Display this help message\n");
        return 0;
    }
    
    int num_threads = DEFAULT_NUM_THREADS;
    size_t buffer_size = DEFAULT_BUFFER_SIZE;
    size_t total_entries = DEFAULT_TOTAL_ENTRIES;
    int append_mode = 1;  // Default to append mode
    int rate_limit = DEFAULT_RATE_LIMIT; // Default to no rate limit
    
    // Parse command line arguments (starting from the second argument)
    for (int i = 2; i < argc; i++) {
        if (strcmp(argv[i], "--threads") == 0 && i + 1 < argc) {
            num_threads = atoi(argv[++i]);
            if (num_threads <= 0) num_threads = DEFAULT_NUM_THREADS;
        } else if (strcmp(argv[i], "--buffer") == 0 && i + 1 < argc) {
            buffer_size = atoi(argv[++i]) * 1024 * 1024;  // Convert MB to bytes
            if (buffer_size <= 0) buffer_size = DEFAULT_BUFFER_SIZE;
        } else if (strcmp(argv[i], "--entries") == 0 && i + 1 < argc) {
            total_entries = atoll(argv[++i]);
            if (total_entries <= 0) total_entries = DEFAULT_TOTAL_ENTRIES;
        } else if (strcmp(argv[i], "--rate") == 0 && i + 1 < argc) {
            rate_limit = atoi(argv[++i]);
            if (rate_limit < 0) rate_limit = DEFAULT_RATE_LIMIT;
        } else if (strcmp(argv[i], "--overwrite") == 0) {
            append_mode = 0;  // Set to overwrite mode
        } else if (strcmp(argv[i], "--help") == 0) {
            printf("Usage: %s <log_file_path> [options]\n", argv[0]);
            printf("Required:\n");
            printf("  <log_file_path>   Path to the output log file\n");
            printf("Options:\n");
            printf("  --threads N      Number of threads (default: %d)\n", DEFAULT_NUM_THREADS);
            printf("  --buffer N       Buffer size in MB per thread (default: %dMB)\n", 
                   (int)(DEFAULT_BUFFER_SIZE / (1024 * 1024)));
            printf("  --entries N      Total log entries to generate (default: %u)\n", (unsigned int)DEFAULT_TOTAL_ENTRIES);
            printf("  --rate N         Rate limit in KB/s (0 = no limit, default: %d)\n", DEFAULT_RATE_LIMIT);
            printf("  --overwrite      Overwrite existing log file instead of appending\n");
            printf("  --help           Display this help message\n");
            return 0;
        }
    }
    
    printf("Log Generator Configuration:\n");
    printf("  Log file: %s\n", log_file);
    printf("  Threads: %d\n", num_threads);
    printf("  Buffer size: %zu bytes per thread\n", buffer_size);
    printf("  Total entries: %zu\n", total_entries);
    printf("  Mode: %s\n", append_mode ? "append" : "overwrite");
    printf("  Rate limit: %d KB/s %s\n", rate_limit, 
           rate_limit == 0 ? "(no limit)" : "");
    
    // Calculate per-thread rate limit
    int thread_rate_limit = 0;
    if (rate_limit > 0) {
        thread_rate_limit = rate_limit / num_threads;
        if (thread_rate_limit < 1) thread_rate_limit = 1;  // Minimum 1 KB/s per thread
        printf("  Rate limit per thread: %d KB/s\n", thread_rate_limit);
    }
    
    // Create directory for log file if it doesn't exist
    char dir_path[256];
    char *log_file_copy = strdup(log_file);
    char *dir = dirname(log_file_copy);
    snprintf(dir_path, sizeof(dir_path), "%s", dir);
    
    if (mkdirs(dir_path) != 0 && errno != EEXIST) {
        fprintf(stderr, "Failed to create directory %s: %s\n", dir_path, strerror(errno));
        free(log_file_copy);
        return 1;
    }
    free(log_file_copy);
    
    // Initialize the output file if in overwrite mode
    if (!append_mode) {
        FILE *init_fp = fopen(log_file, "w");
        if (!init_fp) {
            fprintf(stderr, "Cannot create/overwrite log file %s: %s\n", log_file, strerror(errno));
            return 1;
        }
        fclose(init_fp);
    }
    
    // Create a mutex for synchronizing file access
    pthread_mutex_t file_mutex;
    if (pthread_mutex_init(&file_mutex, NULL) != 0) {
        fprintf(stderr, "Failed to initialize mutex\n");
        return 1;
    }
    
    // Initialize random seed
    srand(time(NULL));
    
    // Create threads
    pthread_t threads[num_threads];
    thread_data_t thread_data[num_threads];
    size_t entries_per_thread = total_entries / num_threads;
    size_t remaining_entries = total_entries % num_threads;
    
    printf("Starting log generation with %d threads...\n", num_threads);
    
    // Record start time
    clock_t start = clock();
    
    // Start threads
    for (int i = 0; i < num_threads; i++) {
        thread_data[i].thread_id = i;
        thread_data[i].buffer = malloc(buffer_size);
        if (!thread_data[i].buffer) {
            fprintf(stderr, "Failed to allocate buffer memory for thread %d\n", i);
            return 1;
        }
        thread_data[i].buffer_size = buffer_size;
        thread_data[i].log_file = log_file;
        thread_data[i].file_mutex = &file_mutex;  // Pass mutex to thread
        
        // Distribute remaining entries to first thread
        thread_data[i].entries_per_thread = entries_per_thread;
        if (i == 0) {
            thread_data[i].entries_per_thread += remaining_entries;
        }
        
        // Set the rate limit for this thread
        if (rate_limit > 0) {
            thread_data[i].rate_limit = rate_limit / num_threads;
            if (thread_data[i].rate_limit < 1) thread_data[i].rate_limit = 1;
        } else {
            thread_data[i].rate_limit = 0; // No limit
        }
        
        if (pthread_create(&threads[i], NULL, generate_logs, &thread_data[i]) != 0) {
            fprintf(stderr, "Failed to create thread %d\n", i);
            return 1;
        }
    }
    
    // Wait for all threads to complete
    for (int i = 0; i < num_threads; i++) {
        pthread_join(threads[i], NULL);
        // Free thread buffer
        free(thread_data[i].buffer);
    }
    
    // Destroy mutex
    pthread_mutex_destroy(&file_mutex);
    
    // Calculate elapsed time
    clock_t end = clock();
    double elapsed = (double)(end - start) / CLOCKS_PER_SEC;
    
    // Calculate total bytes written
    long total_bytes = 0;
    for (int i = 0; i < num_threads; i++) {
        total_bytes += thread_data[i].bytes_written;
    }
    
    // Print statistics
    printf("Done! Generated approximately %zu log entries in %.2f seconds.\n", 
           total_entries, elapsed);
    printf("Total file size: %.2f GB\n", total_bytes / (1024.0 * 1024.0 * 1024.0));
    printf("Average speed: %.2f MB/s (%.2f KB/s)\n", 
           total_bytes / (1024.0 * 1024.0) / elapsed,
           total_bytes / 1024.0 / elapsed);
           
    if (rate_limit > 0) {
        printf("Target rate limit: %d KB/s\n", rate_limit);
    }
    printf("File location: %s\n", log_file);
    
    // Validate that the file exists and is accessible
    FILE *verify_fp = fopen(log_file, "r");
    if (!verify_fp) {
        fprintf(stderr, "Warning: Cannot open generated log file for verification: %s\n", 
                strerror(errno));
    } else {
        fclose(verify_fp);
        printf("Log file verified and accessible.\n");
    }
    
    return 0;
}
