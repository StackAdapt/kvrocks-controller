FROM apache/kvrocks:2.12.1
USER root

RUN apt-get update && apt-get install -y procps && rm -rf /var/lib/apt/lists/*

# Copy your custom kvrocks.conf file into the image
COPY kvrocks.conf /var/lib/kvrocks/kvrocks.conf

RUN mkdir /tmp/kvrocks7770 /tmp/kvrocks7771 /tmp/kvrocks7772 /tmp/kvrocks7773

RUN echo "kvrocks -c /var/lib/kvrocks/kvrocks.conf --port 7770 --dir /tmp/kvrocks7770 --daemonize yes --cluster-enabled yes --bind 0.0.0.0" >> start.sh
RUN echo "kvrocks -c /var/lib/kvrocks/kvrocks.conf --port 7771 --dir /tmp/kvrocks7771 --daemonize yes --cluster-enabled yes --bind 0.0.0.0" >> start.sh
RUN echo "kvrocks -c /var/lib/kvrocks/kvrocks.conf --port 7772 --dir /tmp/kvrocks7772 --daemonize yes --cluster-enabled yes --bind 0.0.0.0" >> start.sh
RUN echo "kvrocks -c /var/lib/kvrocks/kvrocks.conf --port 7773 --dir /tmp/kvrocks7773 --daemonize yes --cluster-enabled yes --bind 0.0.0.0" >> start.sh
RUN echo "tail -f /dev/null" >> start.sh
RUN chmod +x start.sh

EXPOSE 7770:7770
EXPOSE 7771:7771
EXPOSE 7772:7772
EXPOSE 7773:7773

ENTRYPOINT ["sh","start.sh"]
