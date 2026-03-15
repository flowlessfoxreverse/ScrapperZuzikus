FROM node:20-alpine

WORKDIR /app

# Install curl for healthcheck
RUN apk add --no-cache curl

COPY package*.json ./
RUN npm ci --omit=dev

COPY src/ ./src/

EXPOSE 3001

CMD ["node", "src/checker.js"]
