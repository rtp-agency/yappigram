FROM node:20-alpine AS builder

ARG NEXT_PUBLIC_API_URL=""
ARG NEXT_PUBLIC_BASE_PATH=""
ARG CRM_BACKEND_URL="http://crm-backend:8000"
ENV NEXT_PUBLIC_API_URL=$NEXT_PUBLIC_API_URL
ENV NEXT_PUBLIC_BASE_PATH=$NEXT_PUBLIC_BASE_PATH
ENV CRM_BACKEND_URL=$CRM_BACKEND_URL

WORKDIR /app
COPY package.json ./
RUN npm install
COPY . .
RUN rm -rf .next && npm run build

FROM node:20-alpine AS runner
WORKDIR /app

RUN adduser --disabled-password --gecos '' appuser

COPY --from=builder --chown=appuser:appuser /app/.next/standalone ./
COPY --from=builder --chown=appuser:appuser /app/.next/static ./.next/static
COPY --from=builder --chown=appuser:appuser /app/public ./public
COPY --from=builder --chown=appuser:appuser /app/custom-server.js ./custom-server.js
RUN npm install http-proxy

USER appuser

EXPOSE 3000
CMD ["node", "custom-server.js"]
