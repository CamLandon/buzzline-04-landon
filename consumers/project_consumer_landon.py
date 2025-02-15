"""
project_consumer_sentiment.py

Consumes JSON messages from a Kafka topic (or file) and visually compares
sentiment over time between the food and humor categories in real-time using a dual-line chart.
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
DATA_FILE = "../data/project_live.json"

# Kafka settings
KAFKA_TOPIC = "buzzline-topic"
KAFKA_SERVER = "localhost:9092"

# Rolling window size
WINDOW_SIZE = 20

# Store last 20 sentiment values for food & humor
food_sentiment = deque(maxlen=WINDOW_SIZE)
humor_sentiment = deque(maxlen=WINDOW_SIZE)
timestamps = deque(maxlen=WINDOW_SIZE)

# Set up plot
fig, ax = plt.subplots()
ax.set_ylim(0, 1)  # Sentiment ranges from 0 to 1
ax.set_xlim(0, WINDOW_SIZE)
ax.set_title("Cameron Landon: Sentiment Over Time (Food vs Humor)")
ax.set_xlabel("Message Index")
ax.set_ylabel("Sentiment Score")

food_line, = ax.plot([], [], label="Food Sentiment", color="blue")
humor_line, = ax.plot([], [], label="Humor Sentiment", color="red")
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
        category = message.get("category", "")
        sentiment = message.get("sentiment", 0)
        timestamp = message.get("timestamp", "")

        if category == "food":
            food_sentiment.append(sentiment)
        elif category == "humor":
            humor_sentiment.append(sentiment)

        timestamps.append(len(timestamps))  # Just using an index for x-axis

        # Update plots
        food_line.set_data(list(timestamps), list(food_sentiment))
        humor_line.set_data(list(timestamps), list(humor_sentiment))

        ax.set_xlim(max(0, len(timestamps) - WINDOW_SIZE), len(timestamps))  # Scrolls x-axis dynamically

    except StopIteration:
        pass  # No more data

# Start data stream
data_stream = get_messages()

# Animate
ani = animation.FuncAnimation(fig, update, interval=1000)
plt.show()
