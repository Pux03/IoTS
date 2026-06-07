FROM node:20

RUN apt-get update && apt-get install -y curl gnupg

RUN curl -fsSL https://dl.k6.io/key.gpg | gpg --dearmor -o /usr/share/keyrings/k6-archive-keyring.gpg

RUN echo "deb [signed-by=/usr/share/keyrings/k6-archive-keyring.gpg] https://dl.k6.io/deb stable main" \
    > /etc/apt/sources.list.d/k6.list

RUN apt-get update && apt-get install -y k6

WORKDIR /app

COPY package*.json ./

RUN npm install

COPY . .

EXPOSE 3000

CMD ["node", "server.js"]
