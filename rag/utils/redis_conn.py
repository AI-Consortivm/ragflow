#
#  Copyright 2025 The InfiniFlow Authors. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

import logging
import json
import uuid

import valkey as redis
from rag import settings
from rag.utils import singleton
from valkey.lock import Lock
from valkey.retry import Retry
from valkey.backoff import ExponentialBackoff
from valkey.exceptions import ConnectionError, TimeoutError, ConnectionResetError, BusyLoadingError
import trio

class RedisMsg:
    def __init__(self, consumer, queue_name, group_name, msg_id, message):
        self.__consumer = consumer
        self.__queue_name = queue_name
        self.__group_name = group_name
        self.__msg_id = msg_id
        self.__message = json.loads(message["message"])

    def ack(self):
        try:
            self.__consumer.xack(self.__queue_name, self.__group_name, self.__msg_id)
            return True
        except Exception as e:
            logging.warning("[EXCEPTION]ack" + str(self.__queue_name) + "||" + str(e))
        return False

    def get_message(self):
        return self.__message

    def get_msg_id(self):
        return self.__msg_id


@singleton
class RedisDB:
    lua_delete_if_equal = None
    LUA_DELETE_IF_EQUAL_SCRIPT = """
        local current_value = redis.call('get', KEYS[1])
        if current_value and current_value == ARGV[1] then
            redis.call('del', KEYS[1])
            return 1
        end
        return 0
    """

    def __init__(self):
        self.REDIS = None
        self.config = settings.REDIS
        self.__open__()

    def register_scripts(self) -> None:
        cls = self.__class__
        client = self.REDIS
        cls.lua_delete_if_equal = client.register_script(cls.LUA_DELETE_IF_EQUAL_SCRIPT)

    def __open__(self):
        try:
            # Configure retry with exponential backoff for connection resilience
            retry_config = Retry(ExponentialBackoff(), 3)
            retry_on_error_list = [ConnectionError, TimeoutError, ConnectionResetError, BusyLoadingError]
            
            # Extract connection parameters with Azure Redis best practices
            host_parts = self.config["host"].split(":")
            host = host_parts[0]
            port = int(host_parts[1]) if len(host_parts) > 1 else int(self.config.get("port", 6379))
            
            connection_params = {
                "host": host,
                "port": port,
                "db": int(self.config.get("db", 1)),
                "password": self.config.get("password"),
                "decode_responses": True,
                
                # SSL/TLS support for Azure Redis
                "ssl": bool(self.config.get("ssl", False)),
                "ssl_cert_reqs": None if not self.config.get("ssl", False) else "required",
                
                # Connection hardening for Azure Redis
                "retry": retry_config,
                "retry_on_error": retry_on_error_list,
                "retry_on_timeout": True,
                "health_check_interval": int(self.config.get("health_check_interval", 30)),
                "socket_timeout": float(self.config.get("socket_timeout", 30)),
                "socket_connect_timeout": float(self.config.get("socket_connect_timeout", 10)),
                "socket_keepalive": True,
                "socket_keepalive_options": {
                    1: 1,  # TCP_KEEPIDLE
                    2: 3,  # TCP_KEEPINTVL  
                    3: 5   # TCP_KEEPCNT
                }
            }
            
            self.REDIS = redis.StrictRedis(**connection_params)
            self.register_scripts()
            logging.info(f"Redis connected with health check interval: {connection_params['health_check_interval']}s")
        except Exception as e:
            logging.warning(f"Redis can't be connected: {e}")
        return self.REDIS

    def health(self):
        self.REDIS.ping()
        a, b = "xx", "yy"
        self.REDIS.set(a, b, 3)

        if self.REDIS.get(a) == b:
            return True

    def is_alive(self):
        return self.REDIS is not None
    
    def check_connection_health(self):
        """
        Check Redis connection health and reconnect if needed.
        Returns True if connection is healthy, False otherwise.
        """
        if not self.REDIS:
            logging.warning("Redis connection is None, attempting to reconnect")
            self.__open__()
            return self.REDIS is not None
            
        try:
            # Use PING to check connection health
            self.REDIS.ping()
            return True
        except (ConnectionError, TimeoutError, ConnectionResetError) as e:
            logging.warning(f"Redis health check failed with transient error: {e}. Reconnecting.")
            self.__open__()
            return self.REDIS is not None
        except Exception as e:
            logging.error(f"Redis health check failed with unexpected error: {e}. Reconnecting.")
            self.__open__()
            return self.REDIS is not None

    def exist(self, k):
        if not self.REDIS:
            return
        try:
            return self.REDIS.exists(k)
        except (ConnectionError, TimeoutError, ConnectionResetError) as e:
            logging.warning(f"RedisDB.exist {k} got transient error: {e}. Attempting reconnection.")
            self.__open__()
        except Exception as e:
            logging.warning(f"RedisDB.exist {k} got unexpected exception: {e}")
            self.__open__()

    def get(self, k):
        if not self.REDIS:
            return
        try:
            return self.REDIS.get(k)
        except (ConnectionError, TimeoutError, ConnectionResetError) as e:
            logging.warning(f"RedisDB.get {k} got transient error: {e}. Attempting reconnection.")
            self.__open__()
        except Exception as e:
            logging.warning(f"RedisDB.get {k} got unexpected exception: {e}")
            self.__open__()

    def set_obj(self, k, obj, exp=3600):
        try:
            self.REDIS.set(k, json.dumps(obj, ensure_ascii=False), exp)
            return True
        except (ConnectionError, TimeoutError, ConnectionResetError) as e:
            logging.warning(f"RedisDB.set_obj {k} got transient error: {e}. Attempting reconnection.")
            self.__open__()
        except Exception as e:
            logging.warning(f"RedisDB.set_obj {k} got unexpected exception: {e}")
            self.__open__()
        return False

    def set(self, k, v, exp=3600):
        try:
            self.REDIS.set(k, v, exp)
            return True
        except (ConnectionError, TimeoutError, ConnectionResetError) as e:
            logging.warning(f"RedisDB.set {k} got transient error: {e}. Attempting reconnection.")
            self.__open__()
        except Exception as e:
            logging.warning(f"RedisDB.set {k} got unexpected exception: {e}")
            self.__open__()
        return False

    def sadd(self, key: str, member: str):
        try:
            self.REDIS.sadd(key, member)
            return True
        except Exception as e:
            logging.warning("RedisDB.sadd " + str(key) + " got exception: " + str(e))
            self.__open__()
        return False

    def srem(self, key: str, member: str):
        try:
            self.REDIS.srem(key, member)
            return True
        except Exception as e:
            logging.warning("RedisDB.srem " + str(key) + " got exception: " + str(e))
            self.__open__()
        return False

    def smembers(self, key: str):
        try:
            res = self.REDIS.smembers(key)
            return res
        except Exception as e:
            logging.warning(
                "RedisDB.smembers " + str(key) + " got exception: " + str(e)
            )
            self.__open__()
        return None

    def zadd(self, key: str, member: str, score: float):
        try:
            self.REDIS.zadd(key, {member: score})
            return True
        except Exception as e:
            logging.warning("RedisDB.zadd " + str(key) + " got exception: " + str(e))
            self.__open__()
        return False

    def zcount(self, key: str, min: float, max: float):
        try:
            res = self.REDIS.zcount(key, min, max)
            return res
        except Exception as e:
            logging.warning("RedisDB.zcount " + str(key) + " got exception: " + str(e))
            self.__open__()
        return 0

    def zpopmin(self, key: str, count: int):
        try:
            res = self.REDIS.zpopmin(key, count)
            return res
        except Exception as e:
            logging.warning("RedisDB.zpopmin " + str(key) + " got exception: " + str(e))
            self.__open__()
        return None

    def zrangebyscore(self, key: str, min: float, max: float):
        try:
            res = self.REDIS.zrangebyscore(key, min, max)
            return res
        except Exception as e:
            logging.warning(
                "RedisDB.zrangebyscore " + str(key) + " got exception: " + str(e)
            )
            self.__open__()
        return None

    def transaction(self, key, value, exp=3600):
        try:
            pipeline = self.REDIS.pipeline(transaction=True)
            pipeline.set(key, value, exp, nx=True)
            pipeline.execute()
            return True
        except Exception as e:
            logging.warning(
                "RedisDB.transaction " + str(key) + " got exception: " + str(e)
            )
            self.__open__()
        return False

    def queue_product(self, queue, message) -> bool:
        for _ in range(3):
            try:
                payload = {"message": json.dumps(message)}
                self.REDIS.xadd(queue, payload)
                return True
            except Exception as e:
                logging.exception(
                    "RedisDB.queue_product " + str(queue) + " got exception: " + str(e)
                )
        return False

    def queue_consumer(self, queue_name, group_name, consumer_name, msg_id=b">") -> RedisMsg:
        """https://redis.io/docs/latest/commands/xreadgroup/"""
        try:
            group_info = self.REDIS.xinfo_groups(queue_name)
            if not any(gi["name"] == group_name for gi in group_info):
                self.REDIS.xgroup_create(queue_name, group_name, id="0", mkstream=True)
            args = {
                "groupname": group_name,
                "consumername": consumer_name,
                "count": 1,
                "block": 5,
                "streams": {queue_name: msg_id},
            }
            messages = self.REDIS.xreadgroup(**args)
            if not messages:
                return None
            stream, element_list = messages[0]
            if not element_list:
                return None
            msg_id, payload = element_list[0]
            res = RedisMsg(self.REDIS, queue_name, group_name, msg_id, payload)
            return res
        except Exception as e:
            if str(e) == 'no such key':
                pass
            else:
                logging.exception(
                    "RedisDB.queue_consumer "
                    + str(queue_name)
                    + " got exception: "
                    + str(e)
                )
        return None

    def get_unacked_iterator(self, queue_names: list[str], group_name, consumer_name):
        try:
            for queue_name in queue_names:
                try:
                    group_info = self.REDIS.xinfo_groups(queue_name)
                except Exception as e:
                    if str(e) == 'no such key':
                        logging.warning(f"RedisDB.get_unacked_iterator queue {queue_name} doesn't exist")
                        continue
                if not any(gi["name"] == group_name for gi in group_info):
                    logging.warning(f"RedisDB.get_unacked_iterator queue {queue_name} group {group_name} doesn't exist")
                    continue
                current_min = 0
                while True:
                    payload = self.queue_consumer(queue_name, group_name, consumer_name, current_min)
                    if not payload:
                        break
                    current_min = payload.get_msg_id()
                    logging.info(f"RedisDB.get_unacked_iterator {queue_name} {consumer_name} {current_min}")
                    yield payload
        except Exception:
            logging.exception(
                "RedisDB.get_unacked_iterator got exception: "
            )
            self.__open__()

    def get_pending_msg(self, queue, group_name):
        try:
            messages = self.REDIS.xpending_range(queue, group_name, '-', '+', 10)
            return messages
        except Exception as e:
            if 'No such key' not in (str(e) or ''):
                logging.warning(
                    "RedisDB.get_pending_msg " + str(queue) + " got exception: " + str(e)
                )
        return []

    def requeue_msg(self, queue: str, group_name: str, msg_id: str):
        try:
            messages = self.REDIS.xrange(queue, msg_id, msg_id)
            if messages:
                self.REDIS.xadd(queue, messages[0][1])
                self.REDIS.xack(queue, group_name, msg_id)
        except Exception as e:
            logging.warning(
                "RedisDB.get_pending_msg " + str(queue) + " got exception: " + str(e)
            )

    def queue_info(self, queue, group_name) -> dict | None:
        try:
            groups = self.REDIS.xinfo_groups(queue)
            for group in groups:
                if group["name"] == group_name:
                    return group
        except Exception as e:
            logging.warning(
                "RedisDB.queue_info " + str(queue) + " got exception: " + str(e)
            )
        return None

    def delete_if_equal(self, key: str, expected_value: str) -> bool:
        """
        Do follwing atomically:
        Delete a key if its value is equals to the given one, do nothing otherwise.
        """
        return bool(self.lua_delete_if_equal(keys=[key], args=[expected_value], client=self.REDIS))

    def delete(self, key) -> bool:
        try:
            self.REDIS.delete(key)
            return True
        except (ConnectionError, TimeoutError, ConnectionResetError) as e:
            logging.warning(f"RedisDB.delete {key} got transient error: {e}. Attempting reconnection.")
            self.__open__()
        except Exception as e:
            logging.warning(f"RedisDB.delete {key} got unexpected exception: {e}")
            self.__open__()
        return False
    
    
REDIS_CONN = RedisDB()


class RedisDistributedLock:
    def __init__(self, lock_key, lock_value=None, timeout=10, blocking_timeout=1):
        self.lock_key = lock_key
        if lock_value:
            self.lock_value = lock_value
        else:
            self.lock_value = str(uuid.uuid4())
        self.timeout = timeout
        self.lock = Lock(REDIS_CONN.REDIS, lock_key, timeout=timeout, blocking_timeout=blocking_timeout)

    def acquire(self):
        """
        Acquire lock with improved error handling for connection issues.
        """
        try:
            REDIS_CONN.delete_if_equal(self.lock_key, self.lock_value)
            result = self.lock.acquire(token=self.lock_value)
            if result:
                logging.debug(f"Successfully acquired Redis lock: {self.lock_key}")
            return result
        except (ConnectionError, TimeoutError, ConnectionResetError) as e:
            logging.warning(f"Transient Redis error during lock acquire for {self.lock_key}: {e}")
            # Let the retry mechanism in the client handle this
            raise
        except Exception as e:
            logging.error(f"Unexpected error during lock acquire for {self.lock_key}: {e}")
            raise

    async def spin_acquire(self):
        REDIS_CONN.delete_if_equal(self.lock_key, self.lock_value)
        while True:
            if self.lock.acquire(token=self.lock_value):
                break
            await trio.sleep(10)

    def release(self):
        """
        Tolerant lock release that doesn't fail the pipeline on transient Redis errors.
        Relies on TTL as fallback for cleanup if release fails.
        """
        try:
            REDIS_CONN.delete_if_equal(self.lock_key, self.lock_value)
            logging.debug(f"Successfully released Redis lock: {self.lock_key}")
        except (ConnectionError, TimeoutError, ConnectionResetError) as e:
            logging.warning(f"Transient Redis error during lock release for {self.lock_key}: {e}. "
                          f"Lock will expire via TTL in {self.timeout}s")
            # Force connection pool reset to ensure fresh connection for next operation
            try:
                if REDIS_CONN.REDIS and hasattr(REDIS_CONN.REDIS, 'connection_pool'):
                    REDIS_CONN.REDIS.connection_pool.disconnect()
                    logging.info("Redis connection pool reset after transient error")
            except Exception as reset_e:
                logging.warning(f"Failed to reset Redis connection pool: {reset_e}")
        except Exception as e:
            logging.error(f"Unexpected error during lock release for {self.lock_key}: {e}. "
                         f"Lock will expire via TTL in {self.timeout}s")
            # Even on unexpected errors, don't propagate to avoid breaking the pipeline
