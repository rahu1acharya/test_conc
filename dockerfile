# for more information, please refer to https://aka.ms/vscode-docker-python
FROM amazon/aws-glue-streaming-libs:glue_streaming_libs_4.0.0_image_01

# Keeps python from generating .pyc files in the container
ENV PYTHONDONTWRITEBYTECODE=1

# Turns off buffering for easier container logging
ENV PYTHONUNBUFFERED=1

# Set working directory
WORKDIR /home/glue_user/workspace/

# #Install pip requirements
# COPY requirements.txt .

# Copy source file to directory
COPY /src/glue .

# Set entrypoint correctly to the commands to be run properly
ENTRYPOINT ["/bin/bash"]