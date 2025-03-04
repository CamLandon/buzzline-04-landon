"""
project_consumer_avg_sentiment.py

Consumes JSON messages from a Kafka topic (or file) and visualizes the average sentiment rating over time using a real-time line chart.
"""

#####################################
# Import Modules
#####################################

import os
import json
import matplotlib.pyplot as plt
import matplotlib.animation as animation
from collections import deque
from kafka import KafkaConsumer  # Uncomment if using Kafka

# Choose source: 'file' or 'kafka'
SOURCE = 'file'  # Change to 'kafka' if using Kafka

# File path if using file-based streaming
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_FILE = os.path.join(PROJECT_ROOT, "data", "project_live.json")

# Kafka settings
KAFKA_TOPIC = "buzzline-topic"
KAFKA_SERVER = "localhost:9092"

# Rolling window size
WINDOW_SIZE = 20

# Store last 20 sentiment values
timestamps = deque(maxlen=WINDOW_SIZE)
sentiments = deque(maxlen=WINDOW_SIZE)

# Set up plot
fig, ax = plt.subplots()
ax.set_ylim(0, 1)  # Sentiment ranges from 0 to 1
ax.set_xlim(0, WINDOW_SIZE)
ax.set_title("Cameron Landon: Average Sentiment Rating Over Time")
ax.set_xlabel("Message Index")
ax.set_ylabel("Average Sentiment")

sentiment_line, = ax.plot([], [], label="Avg Sentiment", color="blue")
ax.legend()

# Read data (Kafka or File)
def get_messages():
    if SOURCE == "kafka":
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_SERVER,
            value_deserializer=lambda x: json.loads(x.decode("utf-8"))
        )
        for message in consumer:
            yield message.value
    else:  # Read from file
        with open(DATA_FILE, "r") as f:
            for line in f:
                yield json.loads(line.strip())

# Update function for animation
def update(frame):
    try:
        message = next(data_stream)  # Get next message
        sentiment = message.get("sentiment", 0)
        timestamps.append(len(timestamps))  # Just using an index for x-axis
        sentiments.append(sentiment)
        
        sentiments_list = list(sentiments)  # Convert deque to list
        avg_sentiments = [sum(sentiments_list[:i+1]) / (i+1) for i in range(len(sentiments_list))]

        
        # Update plot
        sentiment_line.set_data(list(timestamps), avg_sentiments)
        ax.set_xlim(max(0, len(timestamps) - WINDOW_SIZE), len(timestamps))
    
    except StopIteration:
        pass  # No more data

# Start data stream
data_stream = get_messages()

# Animate
ani = animation.FuncAnimation(fig, update, interval=1000, cache_frame_data=False)
plt.show()