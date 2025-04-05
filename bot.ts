import "jsr:@std/dotenv/load";
import { DB } from "https://deno.land/x/sqlite/mod.ts";
import { parseFeed } from "https://deno.land/x/rss/mod.ts";
import { DenoKvStore } from "@fedify/fedify/x/denokv";
import {
  createBot,
  InProcessMessageQueue,
  link,
  Session,
  text,
} from "@fedify/botkit";

const SERVER_NAME = Deno.env.get("SERVER_NAME");
if (SERVER_NAME == null) {
  console.error("SERVER_NAME environment variable required.");
  Deno.exit(1);
}

const SQLITE_PATH = "./data/rss.db";
const KV_PATH = "./data/kv";

interface Feed {
  url: string;
  title: string;
  lastEntryId: string | null;
  language: string;
}

interface Post {
  title: string;
  link: string;
  feedUrl: string;
  entryId: string;
  language: string;
}

const queue: Post[] = [];

function ensureDataDir() {
  try {
    Deno.mkdirSync("./data", { recursive: true });
  } catch { /* ignore */ }
}

function initDB(): DB {
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

function readAllFeeds(db: DB): Feed[] {
  return db.query<[string, string, string | null, string]>(
    "SELECT url, title, last_entry_id, language FROM feeds",
  ).map(([url, title, lastEntryId, language]) => ({
    url,
    title,
    lastEntryId,
    language,
  }));
}

function updateLastEntryId(db: DB, url: string, id: string) {
  db.query("UPDATE feeds SET last_entry_id = ? WHERE url = ?", [id, url]);
}

function isDuplicateLastEntryId(db: DB, url: string, id: string): boolean {
  const result = db.query<[string | null]>(
    "SELECT last_entry_id FROM feeds WHERE url = ?",
    [url],
  );

  if (result.length === 0) return true;

  const [lastEntryId] = result[0];
  return lastEntryId === id;
}

function readFeed(db: DB, url: string): Feed {
  const result = db.query<[string, string, string | null, string]>(
    "SELECT title, url, last_entry_id, language FROM feeds WHERE url = ?",
    [url],
  );

  if (result.length === 0) {
    throw new Error(`Feed not found: ${url}`);
  }

  const [title, _, lastEntryId, language] = result[0];
  return { url, title, lastEntryId, language };
}

async function enqueue(db: DB) {
  for (const { url, lastEntryId, language } of readAllFeeds(db)) {
    try {
      const res = await fetch(url);
      const xml = await res.text();
      const feed = await parseFeed(xml);
      const entries: Post[] = [];

      for (const entry of feed.entries) {
        const entryId = entry.id ?? entry.links[0]?.href;
        if (!entryId || entryId === lastEntryId) break;

        entries.push({
          title: entry.title.value ?? "Untitled",
          link: entry.links[0]?.href ?? "",
          feedUrl: url,
          entryId,
          language,
        });
      }

      entries.reverse().forEach((entry) => {
        if (!queue.some((q) => q.entryId === entry.entryId)) {
          queue.push(entry);
          console.log(`Enqueued: ${entry.title} (${entry.feedUrl})`);
        }
      });
    } catch (err) {
      console.error(`Failed to process feed (${url}):`, err);
    }
  }
}

async function dequeue(db: DB, session: Session<any>) {
  if (queue.length === 0) return;

  const post = queue.shift();
  if (!post) return;

  try {
    if (isDuplicateLastEntryId(db, post.feedUrl, post.entryId)) return;

    const feed = readFeed(db, post.feedUrl);
    await session.publish(
      text`${link(post.title, post.link)} (${feed.title})`,
      {
        language: post.language,
      },
    );

    updateLastEntryId(db, post.feedUrl, post.entryId);

    console.log(`Published: ${post.title} (${post.feedUrl})`);
  } catch (err) {
    console.error(`Failed to publish (${post.title}):`, err);
  }
}

ensureDataDir();
const db = initDB();

const kv = await Deno.openKv(KV_PATH);
const bot = createBot<void>({
  username: "techblogbot",
  name: "Tech Blog Bot",
  summary: text`A bot that posts tech blogs`,
  kv: new DenoKvStore(kv),
  queue: new InProcessMessageQueue(),
  behindProxy: true,
});

await enqueue(db);
const session = bot.getSession(`https://${SERVER_NAME}`);
await dequeue(db, session);

Deno.cron("scheduled enqueue", "0 * * * *", async () => {
  await enqueue(db);
});

Deno.cron("scheduled dequeue", "*/1 * * * *", async () => {
  const session = bot.getSession(`https://${SERVER_NAME}`);
  await dequeue(db, session);
});

export default bot;
