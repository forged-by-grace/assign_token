# Start the official python base image
FROM python:3.11.3

# Create a new directory in the container image
WORKDIR /app

# Copy the requirements.txt file to the same directory in the container image
COPY ./requirements.txt /app/requirements.txt

# Install the required dependencies
RUN pip install --no-cache-dir --upgrade -r requirements.txt

# Copy the code files to the container
COPY . /app

# Start the container
CMD [ "python", "main.py"]