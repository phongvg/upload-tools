FROM node:20-slim

WORKDIR /app

RUN apt-get update \
  && apt-get install -y --no-install-recommends python3 python3-pip python3-venv ffmpeg \
  && rm -rf /var/lib/apt/lists/*

COPY package.json ./
RUN npm install --omit=dev

COPY qc/requirements.txt ./qc/requirements.txt
RUN python3 -m venv /opt/venv \
  && /opt/venv/bin/pip install --no-cache-dir -r ./qc/requirements.txt

COPY qc ./qc
COPY public ./public
COPY src ./src

ENV NODE_ENV=production
ENV PORT=8080
ENV PYTHONUNBUFFERED=1
ENV PATH="/opt/venv/bin:${PATH}"

CMD ["npm", "start"]
