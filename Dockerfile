# Use Ubuntu 18.04 as the base image
FROM ubuntu:18.04

# Install necessary dependencies
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    build-essential \
    zlib1g-dev \
    && rm -rf /var/lib/apt/lists/*

# Install PyInstaller
RUN pip3 install --no-cache-dir pyinstaller

# Create a working directory
WORKDIR /app

# Copy the Python script into the container
COPY bacnet-scan.py /app/bacnet-scan.py

# Install dependencies
COPY requirements.txt /app/requirements.txt
RUN pip3 install -r requirements.txt

# Run PyInstaller to create the executable
RUN pyinstaller --onefile bacnet-scan.py

# Create a directory to store the output executable
RUN mkdir /output

# Copy the executable to the output directory
RUN cp /app/dist/bacnet-scan /output/bacnet-scan

# Set the output directory as the volume
VOLUME /output

# List files in the /output folder
CMD ["bash", "-c", "ls -la /output"]

# Define the command to run when the container starts (not needed in this case, only building.)
#CMD ["/output/bacnet-scan"]
CMD ["bash", "-c", "sleep 10; echo 'Executable built. Check the mounted volume.'"]
