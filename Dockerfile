FROM apache/airflow:3.1.5

# RUN pip install requests beautifulsoup4 pandas

# Copy requirements.txt first to leverage Docker cache
COPY requirements.txt /requirements.txt

# Install the dependencies
RUN pip install --no-cache-dir -r /requirements.txt