"""Updated claude blogger_service.py - cleaned queries, likes aggregation, stats endpoint"""
# Testing Github Actions Workflow run (Test 3)
import os
import json
import logging
import hashlib
import threading
from datetime import datetime
from typing import Optional, Dict, Any, List
import boto3
import requests
from urllib.parse import parse_qs, quote
import oracledb
from fastapi import FastAPI, HTTPException, Request, Response, status
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, EmailStr
from fastapi.staticfiles import StaticFiles
import uvicorn

# ==================== CONFIG ====================
AWS_REGION = os.getenv("AWS_REGION", "eu-north-1")
ADMIN_EMAIL = os.getenv("ADMIN_EMAIL", "admin@chakorahub.com")
BLOG_PUBLIC_URL = os.getenv("BLOG_PUBLIC_URL", "https://www.chakorahub.com/blogger")
ORACLE_HOST = os.getenv("ORACLE_HOST", "56.228.73.210")
ORACLE_PORT = int(os.getenv("ORACLE_PORT", "1521"))
ORACLE_SERVICE_NAME = os.getenv("ORACLE_SERVICE_NAME", "FREEPDB1")
ORACLE_USER = os.getenv("ORACLE_USER", "SUPPORT")
ORACLE_PASSWORD = os.getenv("ORACLE_PASSWORD", "Welcome123")
ORACLE_SCHEMA = (os.getenv("ORACLE_SCHEMA", "CHAKORA") or "CHAKORA").strip().upper()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("blogger_service")

try:
    ses_client = boto3.client("ses", region_name=AWS_REGION)
except Exception as e:
    print("SES client init error:", e)
    ses_client = None

app = FastAPI(title="ChakoraHub Blogger Service")
os.makedirs("static", exist_ok=True)
app.mount("/static", StaticFiles(directory="static"), name="static")
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://chakorahub.com",
        "https://www.chakorahub.com",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ==================== REDIS SERVICE (HTTP proxy — no direct redis import) ====================
# All cache I/O is delegated to redis_service (port 6380).
# blogger_service holds zero redis library imports.
#
# Blog cache key schema (enforced in redis_service):
#   blog:list              DB 5   20 min  — post list base (likes merged at read-time)
#   blog:{id}              DB 5   10 min  — single post full response
#   blog:stats             DB 5   30 min  — aggregate stats
#   blog:subscriber_count  DB 5   30 min  — subscriber count
#   blog:dashboard         DB 5   10 min  — combined dashboard payload
#   blog:likes:{id}        DB 5   no TTL  — persistent INCR counter
# ══════════════════════════════════════════════════════════════════════════════

REDIS_SERVICE_URL = os.getenv("REDIS_SERVICE_URL", "http://127.0.0.1:6380")


def _rs(method: str, path: str, *, body=None, params: str = ""):
    """
    Fire-and-forget HTTP call to redis_service.
    Returns the parsed JSON dict on success, or None on any error.
    Timeout is intentionally short (2 s) — cache failures must never
    block an Oracle response from reaching the user.
    """
    base = REDIS_SERVICE_URL.rstrip("/")
    url = f"{base}{path}"
    if params:
        url = f"{url}?{params}"
    try:
        response = requests.request(
            method=method.upper(),
            url=url,
            json=body if body is not None else None,
            timeout=2,
        )
        response.raise_for_status()
        return response.json() if response.text else {}
    except Exception as exc:
        logger.warning("redis_service %s %s failed on %s: %s", method, path, base, exc)
        return None


# ── Blog API-response cache helpers ──────────────────────────────────────────

def blog_cache_get(key: str):
    """Return cached data for key, or None on MISS / error."""
    resp = _rs("GET", "/blogger/cache/get", params=f"key={quote(key, safe='')}")
    if resp and resp.get("success") and resp.get("found"):
        logger.info("✅ blog cache HIT: %s", key)
        return resp.get("data")
    return None


def blog_cache_set(key: str, data, ttl: int = None):
    """Store data in the blog API-response cache."""
    payload = {"key": key, "data": data}
    if ttl:
        payload["ttl"] = ttl
    resp = _rs("POST", "/blogger/cache/set", body=payload)
    if resp and resp.get("success"):
        logger.info("✅ blog cache SET: %s ttl=%s", key, resp.get("ttl"))


def blog_cache_delete(key: str):
    """
    Evict one blog cache key or a glob pattern.
    Pass key='blog:*' to flush all response-cache entries at once.
    Like counters are protected inside redis_service and are never
    deleted by wildcard flushes.
    """
    resp = _rs("DELETE", "/blogger/cache/delete", params=f"key={quote(key, safe='')}")
    if resp and resp.get("success"):
        logger.info("🗑️ blog cache DELETE: %s", key)


# ── Like counter helpers ──────────────────────────────────────────────────────

def blog_likes_get(post_id: int) -> int:
    """Return the current like count for a single post (0 if counter absent)."""
    resp = _rs("GET", "/blogger/likes/get", params=f"post_id={post_id}")
    if resp and resp.get("success"):
        return int(resp.get("like_count", 0))
    return 0


def blog_likes_mget(post_ids: List[int]) -> Dict[int, int]:
    """
    Bulk-fetch like counts for multiple posts via a single redis MGET.
    Returns {post_id: count} with 0 for any missing counter.
    """
    if not post_ids:
        return {}
    resp = _rs("POST", "/blogger/likes/mget", body=post_ids)
    if resp and resp.get("success"):
        raw = resp.get("likes", {})
        return {pid: int(raw.get(str(pid), 0)) for pid in post_ids}
    return {pid: 0 for pid in post_ids}


def blog_likes_incr(post_id: int) -> int:
    """
    Atomically increment the like counter for a post.
    Returns the new count (falls back to 0 on error — non-fatal).
    """
    resp = _rs("POST", "/blogger/likes/incr", params=f"post_id={post_id}")
    if resp and resp.get("success"):
        return int(resp.get("like_count", 0))
    return 0


def blog_likes_seed(items: List[Dict]) -> None:
    """
    Startup helper: bulk SETNX like counters from Oracle data.
    items = [{"post_id": 1, "like_count": 42}, ...]
    SETNX in redis_service ensures live counters are never overwritten.
    """
    if not items:
        return
    resp = _rs("POST", "/blogger/likes/seed", body={"items": items})
    if resp and resp.get("success"):
        logger.info(
            "[startup] ✓ like counters seeded: total=%s seeded=%s skipped=%s",
            resp.get("total"), resp.get("seeded"), resp.get("skipped"),
        )


def get_request_identity(request: Request) -> Dict[str, Optional[str]]:
    login_type = (request.headers.get("x-login-type") or "").strip().lower()
    user_id = (request.headers.get("x-user-id") or "").strip()
    employee_id = (request.headers.get("x-employee-id") or "").strip()

    if user_id:
        return {"user_id": user_id, "guest_identifier": None}

    if employee_id:
        return {"user_id": employee_id, "guest_identifier": None}

    forwarded_for = request.headers.get("x-forwarded-for", "")
    client_host = forwarded_for.split(",")[0].strip() if forwarded_for else (request.client.host if request.client else "guest")
    guest_identifier = hashlib.sha256(f"{login_type}:{client_host}".encode("utf-8")).hexdigest()
    return {"user_id": None, "guest_identifier": guest_identifier}


def get_active_subscriber_emails() -> List[str]:
    conn = get_db_connection()
    if not conn:
        return []

    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            SELECT DISTINCT LOWER(EMAIL)
            FROM BLOG_SUBSCRIBERS
            WHERE IS_ACTIVE = 1
              AND EMAIL IS NOT NULL
              AND TRIM(EMAIL) != ''
            """
        )
        return [row[0] for row in cursor.fetchall() if row and row[0]]
    except Exception as e:
        print("Subscriber email fetch error:", e)
        return []
    finally:
        cursor.close()
        conn.close()


def send_new_post_notifications(title: str, summary: str, post_url: str) -> Dict[str, Any]:
    if ses_client is None:
        return {"sent": 0, "failed": 0, "reason": "SES unavailable"}

    subscribers = get_active_subscriber_emails()
    if not subscribers:
        return {"sent": 0, "failed": 0, "reason": "No active subscribers"}

    subject = f"New ChakoraHub Blog Post: {title}"
    safe_summary = (summary or "A new post has been published on ChakoraHub Blog.").strip()
    html_body = f"""
    <html>
      <body style=\"font-family: Arial, sans-serif; color: #111827;\">
        <h2 style=\"margin-bottom: 12px;\">New Blog Post Published</h2>
        <p style=\"font-size: 16px;\"><strong>{title}</strong></p>
        <p style=\"line-height: 1.6;\">{safe_summary}</p>
        <p>
          <a href=\"{post_url}\" style=\"display: inline-block; padding: 10px 16px; background: #2563eb; color: #ffffff; text-decoration: none; border-radius: 6px;\">Read the Post</a>
        </p>
        <p style=\"margin-top: 24px; color: #6b7280; font-size: 13px;\">You are receiving this because you subscribed to ChakoraHub blog updates.</p>
      </body>
    </html>
    """
    text_body = (
        f"New Blog Post Published\n\n"
        f"{title}\n\n"
        f"{safe_summary}\n\n"
        f"Read here: {post_url}\n"
    )

    sent = 0
    failed = 0

    for email in subscribers:
        try:
            ses_client.send_email(
                Source=ADMIN_EMAIL,
                Destination={"ToAddresses": [email]},
                Message={
                    "Subject": {"Data": subject},
                    "Body": {
                        "Html": {"Data": html_body},
                        "Text": {"Data": text_body},
                    },
                },
            )
            sent += 1
        except Exception as e:
            failed += 1
            logger.exception("SES send failed for %s", email)

    return {"sent": sent, "failed": failed}


def get_subscriber_count_data(use_cache: bool = True):
    cache_key = "blog:subscriber_count"
    if use_cache:
        cached = blog_cache_get(cache_key)
        if cached:
            return cached

    conn = get_db_connection()
    if not conn:
        return {"success": False, "count": 0}

    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM BLOG_SUBSCRIBERS WHERE IS_ACTIVE = 1")
    count = cursor.fetchone()[0]
    cursor.close()
    conn.close()

    result = {
        "success": True,
        "count": count,
    }
    blog_cache_set(cache_key, result)
    return result


def get_blog_stats_data(use_cache: bool = True):
    cache_key = "blog:stats"
    if use_cache:
        cached = blog_cache_get(cache_key)
        if cached:
            return cached

    conn = get_db_connection()
    if not conn:
        return {"success": False, "posts": 0, "reads": 0, "likes": 0}

    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT
            COUNT(*) AS TOTAL_POSTS,
            COALESCE(SUM(VIEW_COUNT), 0) AS TOTAL_READS,
            (
                SELECT COUNT(*)
                FROM BLOG_LIKES bl
                JOIN BLOG_POSTS bp ON bp.ID = bl.POST_ID
                WHERE bp.IS_PUBLISHED = 1
            ) AS TOTAL_LIKES
        FROM BLOG_POSTS
        WHERE IS_PUBLISHED = 1
        """
    )
    total_posts, total_reads, total_likes = cursor.fetchone()
    cursor.close()
    conn.close()

    result = {
        "success": True,
        "posts": total_posts,
        "reads": total_reads,
        "likes": total_likes,
    }
    blog_cache_set(cache_key, result)
    return result


# ==================== DATABASE ====================

oracledb.defaults.fetch_lobs = False


def _read_lob(val):
    if val is None:
        return ""
    if hasattr(val, "read"):
        return val.read()
    return str(val)
def get_db_connection():
    try:
        dsn = oracledb.makedsn(
            host=ORACLE_HOST,
            port=ORACLE_PORT,
            service_name=ORACLE_SERVICE_NAME,
        )
        conn = oracledb.connect(
            user=ORACLE_USER,
            password=ORACLE_PASSWORD,
            dsn=dsn,
        )
        cursor = conn.cursor()
        cursor.execute(f"ALTER SESSION SET CURRENT_SCHEMA = {ORACLE_SCHEMA}")
        cursor.close()
        return conn

    except Exception as e:
        print("DB connection error:", e)
        return None


# ==================== MODELS ====================

class SubscribeRequest(BaseModel):
    email: EmailStr
    phone: str


class BlogPostCreate(BaseModel):
    title: str
    summary: str
    content: str
    author: Optional[str] = None
    tags: Optional[str] = ""
    is_locked: Optional[bool] = False
    publish_date: Optional[str] = None


# ==================== HEALTH ====================

@app.get("/health")
def health():

    db_status = "disconnected"

    conn = get_db_connection()

    if conn:
        db_status = "connected"
        conn.close()

    return {
        "service": "blogger_service",
        "database": db_status,
        "timestamp": datetime.now().isoformat(),
    }


# ==================== POSTS LIST ====================

@app.get("/blogger/posts")
def get_posts():
    print("fetching posts list...")
    cache_key = "blog:list"

    cached = blog_cache_get(cache_key)
    
    if cached:
        base_posts = cached.get("posts", [])
    else:
        conn = get_db_connection()
        if not conn:
            return {"success": False, "posts": []}

        cursor = conn.cursor()

        # Cache only base post data from Oracle; merge likes dynamically via redis_service.
        query = """
                SELECT
                    ID,
                    TITLE,
                    SUMMARY,
                    AUTHOR,
                    PUBLISH_DATE,
                    IS_LOCKED,
                    VIEW_COUNT,
                    TAGS
                FROM BLOG_POSTS
                WHERE IS_PUBLISHED = 1
                ORDER BY PUBLISH_DATE DESC
        """

        cursor.execute(query)
        rows = cursor.fetchall()

        base_posts = []
        for r in rows:
            base_posts.append({
                "id": r[0],
                "title": r[1],
                "summary": _read_lob(r[2]),
                "author": r[3],
                "date": str(r[4]),
                "locked": bool(r[5]),
                "view_count": r[6] or 0,
                "tags": r[7].split(",") if r[7] else []
            })

        cursor.close()
        conn.close()

        blog_cache_set(cache_key, {"posts": base_posts})

    post_ids = [p["id"] for p in base_posts]
    like_map = blog_likes_mget(post_ids)

    posts = []
    for post in base_posts:
        post_copy = dict(post)
        post_copy["like_count"] = like_map.get(post_copy["id"], 0)
        posts.append(post_copy)

    return {"success": True, "posts": posts}


# ==================== SINGLE POST ====================

@app.get("/blogger/post/{post_id}")
def get_post(post_id: int, request: Request):

    cache_key = f"blog:{post_id}"

    cached = blog_cache_get(cache_key)

    if cached:
        return cached

    conn = get_db_connection()

    if not conn:
        raise HTTPException(status_code=503, detail="Database unavailable")

    cursor = conn.cursor()

    # Simple query — no JOIN with BLOG_LIKES (like count comes from redis_service)
    query = """
    SELECT
        ID,
        TITLE,
        SUMMARY,
        CONTENT,
        AUTHOR,
        PUBLISH_DATE,
        IS_LOCKED,
        TAGS,
        VIEW_COUNT
    FROM BLOG_POSTS
    WHERE ID = :1
    AND IS_PUBLISHED = 1
    """

    cursor.execute(query, (post_id,))

    post = cursor.fetchone()

    if not post:
        raise HTTPException(status_code=404, detail="Post not found")

    cursor.execute(
        "UPDATE BLOG_POSTS SET VIEW_COUNT = VIEW_COUNT + 1 WHERE ID = :1",
        (post_id,),
    )

    conn.commit()

    cursor.close()
    conn.close()

    blog_cache_delete("blog:stats")
    blog_cache_delete("blog:dashboard")

    response = {
        "success": True,
        "post": {
            "id": post[0],
            "title": post[1],
            "summary": _read_lob(post[2]),
            "content": _read_lob(post[3]),
            "author": post[4],
            "date": str(post[5]),
            "tags": post[7].split(",") if post[7] else [],
            "locked": bool(post[6]),
            "view_count": (post[8] or 0) + 1,
            "like_count": blog_likes_get(post[0]),
        },
    }

    blog_cache_set(cache_key, response)

    return response

# BLOG LIKES

@app.post("/blogger/like/{post_id}")
def like_post(post_id: int, request: Request):

    conn = get_db_connection()
    if not conn:
        return {"success": False}

    cursor = conn.cursor()

    identity = get_request_identity(request)

    if identity["user_id"]:
        cursor.execute(
            "SELECT 1 FROM BLOG_LIKES WHERE POST_ID = :1 AND USER_ID = :2 FETCH FIRST 1 ROWS ONLY",
            (post_id, identity["user_id"]),
        )
    else:
        cursor.execute(
            "SELECT 1 FROM BLOG_LIKES WHERE POST_ID = :1 AND GUEST_IDENTIFIER = :2 FETCH FIRST 1 ROWS ONLY",
            (post_id, identity["guest_identifier"]),
        )

    if cursor.fetchone():
        cursor.close()
        conn.close()
        return {"success": True, "like_count": blog_likes_get(post_id), "duplicate": True}

    if identity["user_id"]:
        cursor.execute(
            "INSERT INTO BLOG_LIKES (POST_ID, USER_ID) VALUES (:1, :2)",
            (post_id, identity["user_id"]),
        )
    else:
        cursor.execute(
            "INSERT INTO BLOG_LIKES (POST_ID, GUEST_IDENTIFIER) VALUES (:1, :2)",
            (post_id, identity["guest_identifier"]),
        )

    conn.commit()
    cursor.close()
    conn.close()

    # Atomically increment the like counter via redis_service — no Oracle round-trip
    like_count = blog_likes_incr(post_id)

    blog_cache_delete("blog:stats")
    blog_cache_delete("blog:dashboard")
    blog_cache_delete(f"blog:{post_id}")

    return {"success": True, "like_count": like_count}

# ==================== BLOG STATS ====================

@app.get("/blogger/stats")
async def blog_stats():
    return get_blog_stats_data()


# ==================== SUBSCRIBE ====================

@app.post("/blogger/subscribe")
def subscribe(payload: SubscribeRequest):

    conn = get_db_connection()

    if not conn:
        raise HTTPException(status_code=503, detail="Database unavailable")

    cursor = conn.cursor()

    try:
        cursor.execute(
            """
            INSERT INTO BLOG_SUBSCRIBERS (EMAIL, PHONE, SUBSCRIBED_AT, IS_ACTIVE)
            VALUES (:1, :2, CURRENT_TIMESTAMP, TRUE)
            """,
            (payload.email.lower(), payload.phone),
        )

        conn.commit()

    except Exception:

        raise HTTPException(status_code=400, detail="Already subscribed")

    finally:

        cursor.close()
        conn.close()

    blog_cache_delete("blog:subscriber_count")
    blog_cache_delete("blog:dashboard")

    return {"success": True}


@app.get("/blogger/subscriber_count")
def sub_count():
    return get_subscriber_count_data()


@app.get("/blogger/dashboard")
def blogger_dashboard():
    cache_key = "blog:dashboard"
    cached = blog_cache_get(cache_key)
    if cached:
        return cached

    posts_data = get_posts()
    stats_data = get_blog_stats_data()
    subscriber_data = get_subscriber_count_data()

    result = {
        "success": True,
        "posts": posts_data.get("posts", []),
        "stats": {
            "posts": stats_data.get("posts", 0),
            "reads": stats_data.get("reads", 0),
            "likes": stats_data.get("likes", 0),
        },
        "subscribers": subscriber_data.get("count", 0),
    }
    blog_cache_set(cache_key, result)
    return result


# ==================== STARTUP CACHE WARM ====================

@app.on_event("startup")
async def warm_cache_on_startup():
    """
    Pre-load all blog caches from Oracle at service startup.
    This ensures the very first visitor gets data in ~1ms from Redis
    instead of waiting 3-4 seconds for Oracle.
    """
    def _warm():
        try:
            print("[startup] Warming blogger caches...")
            get_subscriber_count_data(use_cache=False)
            print("[startup] ✓ subscriber_count cached")
        except Exception as e:
            print(f"[startup] subscriber_count warm error: {e}")

        try:
            get_blog_stats_data(use_cache=False)
            print("[startup] ✓ blog_stats cached")
        except Exception as e:
            print(f"[startup] blog_stats warm error: {e}")

        try:
            # Warm posts + seed per-post like counters in Redis from Oracle
            conn = get_db_connection()
            if conn:
                cursor = conn.cursor()
                cursor.execute(
                    """
                    SELECT bp.ID, COUNT(bl.ID) AS LIKE_COUNT
                    FROM BLOG_POSTS bp
                    LEFT JOIN BLOG_LIKES bl ON bl.POST_ID = bp.ID
                    WHERE bp.IS_PUBLISHED = 1
                    GROUP BY bp.ID
                    """
                )
                rows = cursor.fetchall()
                cursor.close()
                conn.close()

                seed_items = [
                    {"post_id": row[0], "like_count": int(row[1] or 0)}
                    for row in rows
                ]

                if seed_items:
                    # FIX: Do NOT delete existing like counters before seeding.
                    # blog_likes_seed uses SETNX — it skips keys that already exist,
                    # which protects any likes that arrived between startup and seed.
                    # Deleting first would nuke live counters on every restart.
                    blog_likes_seed(seed_items)
                    print(f"[startup] ✓ likes seeded for {len(seed_items)} posts")

            get_posts()  # also warms blog:list
            # After the existing get_posts() call
            try:
                posts_data = get_posts()
                stats_data = get_blog_stats_data()
                subscriber_data = get_subscriber_count_data()
                result = {
                    "success": True,
                    "posts": posts_data.get("posts", []),
                    "stats": {
                        "posts": stats_data.get("posts", 0),
                        "reads": stats_data.get("reads", 0),
                        "likes": stats_data.get("likes", 0),
                    },
                "subscribers": subscriber_data.get("count", 0),
                }
                blog_cache_set("blog:dashboard", result)
                print("[startup] ✓ blog:dashboard cached")
            except Exception as e:
                print(f"[startup] dashboard warm error: {e}")
            print("[startup] ✓ posts_base cached")
        except Exception as e:
            print(f"[startup] posts warm error: {e}")

        print("[startup] Cache warm complete.")

    # Run in background thread so startup doesn't block Uvicorn accepting requests
    threading.Thread(target=_warm, daemon=True).start()


# ==================== ADMIN CREATE POST ====================

@app.options("/blogger/admin/new_post")
async def options_new_post():
    return Response(status_code=200)


@app.post("/blogger/admin/new_post")
async def create_post(request: Request):
    logger.info("create_post request received")

    conn = get_db_connection()

    if not conn:
        logger.error("create_post failed: database connection unavailable")
        return {"success": False}

    cursor = conn.cursor()

    try:
        data = await request.json()
    except Exception:
        data = {}

    if not isinstance(data, dict) or not data:
        try:
            raw_body = (await request.body()).decode("utf-8", errors="ignore")
            parsed = parse_qs(raw_body, keep_blank_values=True)
            data = {key: values[-1] if values else "" for key, values in parsed.items()}
        except Exception:
            data = {}

    logger.info(
        "create_post parsed payload keys=%s title=%s author=%s is_published=%s is_locked=%s",
        sorted(list(data.keys())) if isinstance(data, dict) else [],
        (data or {}).get("title"),
        (data or {}).get("author"),
        (data or {}).get("is_published"),
        (data or {}).get("is_locked"),
    )

    if data.get("is_locked") in {"true", "false"}:
        data["is_locked"] = data.get("is_locked") == "true"

    if data.get("is_published") in {"true", "false"}:
        data["is_published"] = data.get("is_published") == "true"

    title = data.get("title")
    summary = data.get("summary")
    content = data.get("content")
    author = data.get("author") or "Admin"
    tags = data.get("tags", "")
    is_locked = data.get("is_locked", False)
    is_published = bool(data.get("is_published", True))
    publish_date = data.get("publish_date")

    if not title or not content:
        logger.warning("create_post rejected: missing title/content")
        return {"success": False, "message": "Missing title/content"}

    inserted_post_id = None
    publish_dt = None
    if publish_date:
        try:
            publish_dt = datetime.fromisoformat(str(publish_date).replace("Z", "+00:00"))
        except Exception:
            publish_dt = None

    if not publish_dt:
        publish_dt = datetime.now()
    try:
        query = """
        INSERT INTO BLOG_POSTS
        (TITLE, SUMMARY, CONTENT, AUTHOR, TAGS, IS_LOCKED, IS_PUBLISHED, PUBLISH_DATE, LIKE_COUNT, VIEW_COUNT)
        VALUES (:1,:2,:3,:4,:5,:6,:7,:8,0,0)
        """

        logger.info(
            "create_post inserting title=%s author=%s tags=%s is_locked=%s is_published=%s publish_date=%s",
            title,
            author,
            tags,
            is_locked,
            is_published,
            publish_date,
        )

        cursor.execute(
            query,
            (title, summary, content, author, tags, 1 if is_locked else 0, 1 if is_published else 0, publish_dt),
        )

        try:
            cursor.execute("SELECT MAX(ID) FROM BLOG_POSTS")
            inserted_post_id = cursor.fetchone()[0]
        except Exception:
            inserted_post_id = None

        conn.commit()
        logger.info("create_post insert committed inserted_post_id=%s", inserted_post_id)
    except Exception:
        logger.exception("create_post insert failed")
        raise
    finally:
        cursor.close()
        conn.close()

    # Flush all blog response-cache entries so the new post appears immediately.
    # Like counters (blog:likes:*) are protected inside redis_service and are untouched.
    blog_cache_delete("blog:*")

    notification_result = {"sent": 0, "failed": 0, "reason": "Draft post"}
    if is_published:
        post_url = BLOG_PUBLIC_URL
        if inserted_post_id:
            post_url = f"{BLOG_PUBLIC_URL}#post-{inserted_post_id}"
        notification_result = send_new_post_notifications(title, summary or "", post_url)
        logger.info("create_post notification result=%s", notification_result)
    else:
        logger.info("create_post saved as draft; notifications skipped")

    return {
        "success": True,
        "notification": notification_result,
    }


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=7500)
