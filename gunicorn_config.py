# gunicorn_config.py

# Timeout in seconds
timeout = 300  # Set to 5 minutes

# Bind to the appropriate host and port
bind = "0.0.0.0:8080"

# Number of workers and threads (adjust based on your app's needs)
workers = 2
threads = 4
