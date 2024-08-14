FROM python:3.11-slim-bookworm@sha256:fc39d2e68b554c3f0a5cb8a776280c0b3d73b4c04b83dbade835e2a171ca27ef AS base

# innstall python packages
WORKDIR /
RUN python3 -m venv /venv --without-pip
COPY requirements.txt .
RUN pip --python /venv/bin/python install -r requirements.txt
# Set Python path
ENV PATH="/venv/bin:${PATH}"