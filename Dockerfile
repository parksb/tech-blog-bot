FROM denoland/deno:2.2.8

WORKDIR /app

COPY deno.json deno.json
COPY deno.lock deno.lock
RUN deno install

COPY . .

CMD ["deno", "serve", "-A", "bot.ts"]
