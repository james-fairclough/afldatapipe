# Example: gunicorn.conf.py
bind = "0.0.0.0:8080"  # Bind to all network interfaces on port 8080
workers = 2            # Set an appropriate number of workers
threads = 4            # Use threads for concurrency if needed
preload_app = True      # Preload the application
timeout = 120           # Set timeout to handle long-running requests
