FROM ghcr.io/elementary-data/elementary@sha256:95e1027e09e8e5737ad2eeaa0f1f309bb9278ab414092d514318fc218ea9e2be

RUN exec /bin/bash -c "apt update && apt install -y jq curl"

COPY config.yml /root/.edr/config.yml
COPY profiles.yml /root/.dbt/profiles.yml
COPY run.sh run.sh
RUN exec /bin/bash -c "chmod +x run.sh"

# Workaround, hope to remove this someday
# ref: https://elementary-community.slack.com/archives/C02CTC89LAX/p1700738842128249
RUN dbt --log-format json deps --project-dir /usr/local/lib/python3.10/site-packages/elementary/monitor/dbt_project

ENTRYPOINT []
