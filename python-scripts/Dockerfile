FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Fix line endings and set permissions
RUN chmod +x run.sh && \
    sed -i 's/\r$//' run.sh

# Run the shell script instead of directly running Python
CMD ["./run.sh"] 