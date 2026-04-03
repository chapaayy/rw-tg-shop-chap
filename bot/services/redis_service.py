import json
import inspect
import logging
import time
import uuid
from typing import Any, Optional

from redis.asyncio import Redis

from config.settings import Settings


class RedisService:
    _UNLOCK_SCRIPT = """
if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("del", KEYS[1])
else
    return 0
end
"""

    def __init__(self, settings: Settings):
        self.settings = settings
        self._client: Optional[Redis] = None
        self._available = False
        self._last_error_at = 0.0
        self._error_log_cooldown_seconds = 30.0

    @property
    def client(self) -> Optional[Redis]:
        return self._client

    def is_available(self) -> bool:
        return bool(self._available and self._client is not None)

    def build_key(self, *parts: Any) -> str:
        normalized_parts = [str(part).strip(":") for part in parts if str(part).strip(":")]
        return ":".join([self.settings.REDIS_KEY_PREFIX, *normalized_parts])

    async def connect(self) -> bool:
        if not self.settings.REDIS_ENABLED:
            logging.info("Redis disabled via REDIS_ENABLED=False. Falling back to in-memory behavior.")
            return False

        try:
            self._client = Redis.from_url(
                self.settings.redis_dsn,
                encoding="utf-8",
                decode_responses=True,
                socket_connect_timeout=self.settings.REDIS_CONNECT_TIMEOUT_SECONDS,
                socket_timeout=self.settings.REDIS_SOCKET_TIMEOUT_SECONDS,
                health_check_interval=30,
            )
            await self._client.ping()
            self._available = True
            logging.info("Redis connected successfully.")
            return True
        except Exception as exc:
            self._log_error("Failed to connect to Redis; falling back to safe mode.", exc)
            self._available = False
            await self._safe_close_client()
            self._client = None
            return False

    async def close(self) -> None:
        if self._client is None:
            return
        try:
            await self._safe_close_client()
        except Exception as exc:
            self._log_error("Failed to close Redis client cleanly.", exc)
        finally:
            self._client = None
            self._available = False

    async def get_json(self, key: str) -> Optional[Any]:
        redis_key = self._qualify_key(key)
        if not self.is_available():
            return None
        try:
            raw = await self._client.get(redis_key)
            if raw is None:
                return None
            return json.loads(raw)
        except Exception as exc:
            self._log_error(f"Redis get_json failed for key '{redis_key}'.", exc)
            return None

    async def set_json(self, key: str, value: Any, ttl_seconds: int) -> bool:
        redis_key = self._qualify_key(key)
        if not self.is_available():
            return False
        try:
            payload = json.dumps(value, ensure_ascii=False)
            await self._client.set(redis_key, payload, ex=max(1, int(ttl_seconds)))
            return True
        except Exception as exc:
            self._log_error(f"Redis set_json failed for key '{redis_key}'.", exc)
            return False

    async def delete(self, *keys: str) -> int:
        if not keys or not self.is_available():
            return 0
        redis_keys = [self._qualify_key(key) for key in keys]
        try:
            deleted = await self._client.delete(*redis_keys)
            return int(deleted or 0)
        except Exception as exc:
            self._log_error(f"Redis delete failed for keys '{redis_keys}'.", exc)
            return 0

    async def set_if_not_exists(self, key: str, value: str, ttl_seconds: int) -> bool:
        redis_key = self._qualify_key(key)
        if not self.is_available():
            return False
        try:
            result = await self._client.set(
                redis_key,
                value,
                nx=True,
                ex=max(1, int(ttl_seconds)),
            )
            return bool(result)
        except Exception as exc:
            self._log_error(f"Redis set_if_not_exists failed for key '{redis_key}'.", exc)
            return False

    async def exists(self, key: str) -> Optional[bool]:
        redis_key = self._qualify_key(key)
        if not self.is_available():
            return None
        try:
            exists = await self._client.exists(redis_key)
            return bool(exists)
        except Exception as exc:
            self._log_error(f"Redis exists check failed for key '{redis_key}'.", exc)
            return None

    async def check_rate_limit(
        self,
        key: str,
        *,
        max_requests: int,
        window_seconds: int,
    ) -> bool:
        """Return True if request is allowed, False if rate limit exceeded."""
        redis_key = self._qualify_key(key)
        if not self.is_available():
            return True
        try:
            current = await self._client.incr(redis_key)
            if current == 1:
                await self._client.expire(redis_key, max(1, int(window_seconds)))
            return int(current) <= int(max_requests)
        except Exception as exc:
            self._log_error(f"Redis check_rate_limit failed for key '{redis_key}'.", exc)
            return True

    async def acquire_lock(self, key: str, ttl_seconds: int) -> Optional[str]:
        redis_key = self._qualify_key(key)
        if not self.is_available():
            return None
        token = str(uuid.uuid4())
        try:
            locked = await self._client.set(
                redis_key,
                token,
                nx=True,
                ex=max(1, int(ttl_seconds)),
            )
            return token if locked else None
        except Exception as exc:
            self._log_error(f"Redis acquire_lock failed for key '{redis_key}'.", exc)
            return None

    async def release_lock(self, key: str, token: str) -> bool:
        redis_key = self._qualify_key(key)
        if not self.is_available() or not token:
            return False
        try:
            result = await self._client.eval(self._UNLOCK_SCRIPT, 1, redis_key, token)
            return bool(result)
        except Exception as exc:
            self._log_error(f"Redis release_lock failed for key '{redis_key}'.", exc)
            return False

    def _qualify_key(self, key: str) -> str:
        if not key:
            return self.build_key("empty")
        prefix = f"{self.settings.REDIS_KEY_PREFIX}:"
        if key.startswith(prefix):
            return key
        return self.build_key(key)

    def _log_error(self, message: str, exc: Exception) -> None:
        now = time.monotonic()
        if now - self._last_error_at >= self._error_log_cooldown_seconds:
            logging.warning("%s Error: %s", message, exc)
            self._last_error_at = now

    async def _safe_close_client(self) -> None:
        if self._client is None:
            return

        close_method = getattr(self._client, "aclose", None)
        if not callable(close_method):
            close_method = getattr(self._client, "close", None)
        if not callable(close_method):
            return

        try:
            maybe_awaitable = close_method()
            if inspect.isawaitable(maybe_awaitable):
                await maybe_awaitable
        except Exception:
            logging.debug("Suppressed exception while closing Redis client.", exc_info=True)
