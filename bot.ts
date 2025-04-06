import "jsr:@std/dotenv/load";
import { DB } from "https://deno.land/x/sqlite/mod.ts";
import { parseFeed } from "https://deno.land/x/rss/mod.ts";
import { DenoKvMessageQueue, DenoKvStore } from "@fedify/fedify/x/denokv";
import { createBot, link, mention, Session, text } from "@fedify/botkit";
import FEEDS from "./feeds.ts";

const SERVER_NAME = Deno.env.get("SERVER_NAME");
if (!SERVER_NAME) {
  console.error("SERVER_NAME env var required.");
  Deno.exit(1);
}

const SQLITE_PATH = "./data/data.db";
const KV_PATH = "./data/kv";

interface Feed {
  url: string;
  title: string;
  lastId: string | null;
  language: string;
}

interface Post {
  title: string;
  link: string;
  date: ZonedDateTime | null;
  feedUrl: string;
  entryId: string;
  language: string;
}

const postQ: Post[] = [];

function prepareData() {
  try {
    Deno.mkdirSync("./data", { recursive: true });
  } catch { /* ignore */ }
}

function openDb(): DB {
  const db = new DB(SQLITE_PATH);
  db.execute(`
    CREATE TABLE IF NOT EXISTS feeds (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      title TEXT NOT NULL,
      url TEXT NOT NULL UNIQUE,
      last_entry_id TEXT,
      language TEXT DEFAULT 'en'
    );
  `);
  return db;
}

function seedFeeds(db: DB) {
  const feeds = FEEDS.map((f) => [f.title, f.url, f.language]);
  for (const feed of feeds) {
    db.query(
      "INSERT OR IGNORE INTO feeds (title, url, language) VALUES (?, ?, ?)",
      feed,
    );
  }
}

function getFeeds(db: DB): Feed[] {
  return db.query<[string, string, string | null, string]>(
    "SELECT title, url, last_entry_id, language FROM feeds",
  ).map(([title, url, lastId, language]) => ({
    url,
    title,
    lastId,
    language,
  }));
}

function getFeed(db: DB, url: string): Feed {
  const row = db.query<[string, string, string | null, string]>(
    "SELECT title, url, last_entry_id, language FROM feeds WHERE url = ?",
    [url],
  )[0];
  if (!row) throw new Error(`No such feed: ${url}`);
  const [title, _, lastId, language] = row;
  return { url, title, lastId, language };
}

function setLastId(db: DB, url: string, id: string) {
  db.query("UPDATE feeds SET last_entry_id = ? WHERE url = ?", [id, url]);
}

function isDupId(db: DB, url: string, id: string): boolean {
  const row = db.query<[string | null]>(
    "SELECT last_entry_id FROM feeds WHERE url = ?",
    [url],
  )[0];
  if (!row) return true;
  return row[0] === id;
}

function format(datetime: ZonedDateTime): string {
  const year = datetime.year;
  const month = String(datetime.month).padStart(2, "0");
  const day = String(datetime.day).padStart(2, "0");
  return `${year}-${month}-${day}`;
}

async function fetchNew(db: DB) {
  const seen = new Set(postQ.map((p) => p.entryId));

  const feeds = getFeeds(db);
  const cunks = (() => {
    const size = 10;
    const chunks: Feed[][] = [];
    for (let i = 0; i < feeds.length; i += size) {
      chunks.push(feeds.slice(i, i + size));
    }
    return chunks;
  })();

  for (const chunk of cunks) {
    await Promise.all(chunk.map(async ({ url, lastId, language }) => {
      try {
        const res = await fetch(url);
        const xml = await res.text();
        const feed = await parseFeed(xml);

        const items: Post[] = [];
        for (const e of feed.entries) {
          if (!e.links?.length || !e.links[0].href) continue;

          const id = e.id ?? e.links[0].href;
          if (!id || id === lastId) break;

          items.push({
            title: e.title.value ?? "Untitled",
            link: e.links[0].href,
            feedUrl: url,
            entryId: id,
            date: e.published
              ? Temporal.Instant.from(e.published.toISOString())
                .toZonedDateTimeISO("Asia/Seoul")
              : null,
            language,
          });
        }

        if (lastId == null) {
          setLastId(db, url, items[0].entryId);
        } else {
          items.reverse().forEach((item) => {
            if (!seen.has(item.entryId)) {
              postQ.push(item);
              seen.add(item.entryId);
              console.log(`Enqueued: [${item.feedUrl}] ${item.title}`);
            }
          });
        }
      } catch (err) {
        console.error(`Failed feed (${url}):`, err);
      }
    }));
  }
}

async function publishNext(db: DB, s: Session<any>) {
  if (postQ.length === 0) return;

  const p = postQ.shift();
  if (!p) return;

  try {
    if (isDupId(db, p.feedUrl, p.entryId)) return;

    const f = getFeed(db, p.feedUrl);
    await s.publish(
      text`[${f.title}] ${link(p.title, p.link)}${
        p.date !== null ? ` (${format(p.date)})` : ""
      }`,
      {
        language: p.language,
      },
    );

    setLastId(db, p.feedUrl, p.entryId);
    console.log(`Published: ${p.title} (${p.feedUrl})`);
  } catch (err) {
    console.error(`Failed post (${p.title}):`, err);
  }
}

prepareData();
const db = openDb();
const kv = await Deno.openKv(KV_PATH);

const bot = createBot<void>({
  username: "bot",
  name: "Tech Blog Bot",
  summary: text`Fediverse bot for delivering tech blog articles`,
  icon: new URL(
    "https://raw.githubusercontent.com/parksb/tech-blog-bot/refs/heads/main/icon.png",
  ),
  properties: {
    "Source code": link("GitHub", "https://github.com/parksb/tech-blog-bot"),
    "Created by": mention("@parksb@social.silicon.moe"),
  },
  kv: new DenoKvStore(kv),
  queue: new DenoKvMessageQueue(kv),
  behindProxy: true,
});

seedFeeds(db);
await fetchNew(db);
await publishNext(db, bot.getSession(`https://${SERVER_NAME}`));

Deno.cron(
  "load feeds",
  "0 * * * *",
  () => fetchNew(db),
);

Deno.cron(
  "publish article",
  "*/1 * * * *",
  () => publishNext(db, bot.getSession(`https://${SERVER_NAME}`)),
);

export default bot;
