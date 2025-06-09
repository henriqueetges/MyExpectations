FROM openjdk:11-slim

RUN apt-get update && apt-get install -y \
    curl \
    python3 \
    python3-pip \
    
    && rm -rf /var/lib/apt/lists/*

ENV SPARK_VERSION=3.5.6
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH

RUN curl -fsSL https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz | \
    tar -xz -C /opt && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} $SPARK_HOME

WORKDIR /app
COPY . .

COPY requirements.txt .
RUN pip3 install --upgrade pip && \
    pip3 install -r requirements.txt

EXPOSE 4040

CMD ["jupyter", "lab", "--ip=0.0.0.0", "--no-browser", "--allow-root"]
