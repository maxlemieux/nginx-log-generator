CC = gcc
CFLAGS = -Wall -O3 -march=native -mtune=native -pthread
LDFLAGS = -pthread
TARGET = nginx_log_generator

all: $(TARGET)

$(TARGET): nginx_log_generator.c
	$(CC) $(CFLAGS) -o $@ $< $(LDFLAGS)

clean:
	rm -f $(TARGET)

.PHONY: all clean
