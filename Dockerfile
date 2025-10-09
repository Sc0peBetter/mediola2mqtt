ARG BUILD_FROM
# hadolint ignore=DL3006
FROM $BUILD_FROM

ENV LANG C.UTF-8

# Install required packages in a single layer
# hadolint ignore=DL3018
RUN apk add --no-cache \
    python3 \
    py3-pip \
    py3-paho-mqtt \
    py3-requests \
    py3-yaml

# Copy application files
COPY mediola2mqtt.py /
COPY run.sh /

# Make the entrypoint script executable
RUN chmod a+x /run.sh

# Set the entrypoint command
CMD [ "/run.sh" ]
