import "jsr:@std/dotenv/load";
import { DB } from "https://deno.land/x/sqlite/mod.ts";
import { parseFeed } from "https://deno.land/x/rss/mod.ts";
import { DenoKvMessageQueue, DenoKvStore } from "@fedify/fedify/x/denokv";
import { createBot, link, mention, Session, text } from "@fedify/botkit";

const SERVER_NAME = Deno.env.get("SERVER_NAME");
if (!SERVER_NAME) {
  console.error("SERVER_NAME env var required.");
  Deno.exit(1);
}

const SQLITE_PATH = "./data/data.db";
const KV_PATH = "./data/kv";

type RawFeed = {
  title: string;
  url: string;
  showDescription?: boolean;
  language: string;
}

interface Feed {
  url: string;
  title: string;
  lastId: string | null;
  showDescription: boolean;
  language: string;
}

interface Post {
  id: string;
  title: string;
  description: string | null;
  link: string;
  date: Temporal.Instant | null;
  feedUrl: string;
  language: string;
}

const queue: Post[] = [];

function prepareData() {
  try {
    Deno.mkdirSync("./data", { recursive: true });
  } catch { /* ignore */ }
}

function openDb(): DB {
  const db = new DB(SQLITE_PATH);

  const columns = {
    id: "id INTEGER PRIMARY KEY AUTOINCREMENT",
    title: "title TEXT NOT NULL",
    show_description: "show_description BOOLEAN NOT NULL DEFAULT 0",
    url: "url TEXT NOT NULL UNIQUE",
    last_entry_id: "last_entry_id TEXT",
    language: "language TEXT NOT NULL DEFAULT 'en'",
  };

  db.execute(`
    CREATE TABLE IF NOT EXISTS feeds (
      ${columns.id},
      ${columns.title},
      ${columns.show_description},
      ${columns.url},
      ${columns.last_entry_id},
      ${columns.language}
    );
  `);

  const existingColumns = db.query("PRAGMA table_info(feeds)").map((row) =>
    row[1]
  );

  if (!existingColumns.includes("show_description")) {
    db.execute(
      `ALTER TABLE feeds ADD COLUMN ${columns.show_description}`,
    );
  }

  return db;
}

async function fetchFeeds(): Promise<RawFeed[]> {
  const URL =
    "https://raw.githubusercontent.com/parksb/tech-blog-bot/refs/heads/main/feeds.json";

  const res = await fetch(URL);
  if (!res.ok) {
    throw new Error(`Failed to fetch feeds: ${res.status} ${res.statusText}`);
  }

  return res.json();
}

async function seedFeeds(db: DB) {
  const rawFeeds = await fetchFeeds();

  const feeds = rawFeeds.map(
    (x) => [x.title, x.url, x.showDescription ?? false, x.language],
  );

  for (const feed of feeds) {
    db.query(
      "INSERT OR IGNORE INTO feeds (title, url, show_description, language) VALUES (?, ?, ?, ?)",
      feed,
    );

    db.query(
      "UPDATE feeds SET title = ?, show_description = ?, language = ? WHERE url = ?",
      [feed[0], feed[2], feed[3], feed[1]],
    );
  }

  const urls = feeds.map((f) => f[1]);
  if (urls.length > 0) {
    const placeholders = urls.map(() => "?").join(", ");
    db.query(
      `DELETE FROM feeds WHERE url NOT IN (${placeholders})`,
      urls,
    );
  }
}

function getFeeds(db: DB): Feed[] {
  return db.query<[string, string, boolean, string | null, string]>(
    "SELECT title, url, show_description, last_entry_id, language FROM feeds",
  ).map(([title, url, showDescription, lastId, language]) => ({
    title,
    url,
    showDescription,
    lastId,
    language,
  }));
}

function getFeed(db: DB, url: string): Feed {
  const row = db.query<[string, boolean, string | null, string]>(
    "SELECT title, show_description, last_entry_id, language FROM feeds WHERE url = ?",
    [url],
  )[0];
  if (!row) throw new Error(`No such feed: ${url}`);
  const [title, showDescription, lastId, language] = row;
  return { title, url, showDescription, lastId, language };
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

function format(datetime: Temporal.ZonedDateTime): string {
  const year = datetime.year;
  const month = String(datetime.month).padStart(2, "0");
  const day = String(datetime.day).padStart(2, "0");
  return `${year}-${month}-${day}`;
}

function isSameDay(a: Temporal.Instant, b: Temporal.Instant): boolean {
  const aDate = a.toZonedDateTimeISO("UTC");
  const bDate = b.toZonedDateTimeISO("UTC");
  return aDate.year === bDate.year &&
    aDate.month === bDate.month &&
    aDate.day === bDate.day;
}

async function fetchNew(db: DB) {
  const seen = new Set(queue.map((p) => p.id));
  const threeDaysAgo = Temporal.Now.instant().subtract({ hours: 72 });

  const feeds = new Map(
    getFeeds(db).map((feed) => [
      feed.url,
      feed,
    ]),
  );

  const cunks = (() => {
    const size = 10;
    const chunks: Feed[][] = [];
    for (let i = 0; i < feeds.size; i += size) {
      chunks.push(Array.from(feeds.values()).slice(i, i + size));
    }
    return chunks;
  })();

  const removeHttp = (url: string | null) => url?.replace(/^https?:\/\//, "");

  for (const chunk of cunks) {
    await Promise.all(chunk.map(async ({ url, lastId, language }) => {
      try {
        const res = await fetch(url);
        const xml = await res.text();
        const feed = await parseFeed(xml);

        const { showDescription } = feeds.get(url) ?? {};

        const items: Post[] = [];
        for (const entry of feed.entries) {
          if (!entry.links?.length || !entry.links[0].href) continue;

          const id = entry.id ?? entry.links[0].href;
          if (!id || removeHttp(id) === removeHttp(lastId)) break;

          const date = entry.published ?? entry.updated ?? null;

          let description = showDescription
            ? entry.content?.value ?? null
            : null;

          if (description && description.length > 250) {
            description = null;
          }

          items.push({
            id,
            title: entry.title?.value ?? "Untitled",
            description,
            link: entry.links[0].href,
            feedUrl: url,
            date: date ? Temporal.Instant.from(date.toISOString()) : null,
            language,
          });

          if (lastId === null) break;
        }

        if (lastId === null) {
          setLastId(db, url, items[0].id);
        } else {
          items.reverse()
            .filter((x) => !seen.has(x.id))
            .filter((x) =>
              x.date ? Temporal.Instant.compare(x.date, threeDaysAgo) > 0 : true
            )
            .forEach((x) => {
              queue.push(x);
              seen.add(x.id);
              console.log(`Enqueued: [${x.feedUrl}] ${x.title} (${x.id})`);
            });
        }
      } catch (err) {
        console.error(`Failed to fetch: [${url}] `, err);
      }
    }));
  }
}

async function publishNext(db: DB, session: Session<void>) {
  if (queue.length === 0) return;

  const x = queue.shift();
  if (!x) return;

  const date = (x: Post) => {
    if (x.date && !isSameDay(x.date, Temporal.Now.instant())) {
      return ` (${format(x.date.toZonedDateTimeISO("Asia/Seoul"))})`;
    }
    return "";
  };

  const description = (x: Post) => {
    return x.description ? `\n${x.description}` : "";
  };

  try {
    if (isDupId(db, x.feedUrl, x.id)) return;

    const f = getFeed(db, x.feedUrl);

    await session.publish(
      text`[${f.title}] ${link(x.title, x.link)}${description(x)}${date(x)}`,
      { language: x.language },
    );

    setLastId(db, x.feedUrl, x.id);
    console.log(`Published: [${f.title}] ${x.title} (${x.id})`);
  } catch (err) {
    console.error(
      `Failed to publish: [${x.feedUrl}] ${x.title} (${x.id}) `,
      err,
    );
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
  "*/3 * * * *",
  () => publishNext(db, bot.getSession(`https://${SERVER_NAME}`)),
);

export default bot;
