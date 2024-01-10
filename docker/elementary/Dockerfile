FROM ghcr.io/elementary-data/elementary:v0.13.2

RUN exec /bin/bash -c "apt update && apt install -y jq curl"

COPY config.yml /root/.edr/config.yml
COPY profiles.yml /root/.dbt/profiles.yml
COPY run.sh run.sh
RUN exec /bin/bash -c "chmod +x run.sh"

# Workaround, hope to remove this someday
# ref: https://elementary-community.slack.com/archives/C02CTC89LAX/p1700738842128249
RUN dbt --log-format json deps --project-dir /app/elementary/.venv/lib/python3.11/site-packages/elementary/monitor/dbt_project

ENTRYPOINT []
