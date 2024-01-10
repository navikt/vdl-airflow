FROM ghcr.io/elementary-data/elementary:v0.13.2

RUN exec /bin/bash -c "apt update && apt install -y jq curl"

COPY config.yml config.yml
COPY profiles.yml /root/.dbt/profiles.yml
COPY run.sh run.sh
RUN exec /bin/bash -c "chmod +x run.sh"

ENTRYPOINT []
