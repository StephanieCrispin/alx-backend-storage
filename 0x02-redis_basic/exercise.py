# usr/bin/python3
"""Contains a redis class"""
import redis
import uuid
from functools import wraps
from typing import Union, Optional, Callable


def count_calls(method: Callable) -> Callable:
    """Retuns a function that counts the number
    of times a particular method has been called
    using redis to increment it
    """
    @wraps(method)
    def wrapper(self, *args, **kwargs):
        """
        The wrapper function where the counting is done and
        and returned to the and the method is returned to the
        original function
        """
        count = method.__qualname__
        self._redis.incr(count)
        return method(self, *args, *kwargs)
    return wrapper


def call_history(method: Callable) -> Callable:
    """Returns a function that stores th history of  input and
    output for a particular function
    """
    inputs = method.__qualname__ + ":inputs"
    outputs = method.__qualname__ + ":outputs"

    @wraps(method)
    def wrapper(self, *args, **kwargs):
        """The wrapper function that stores the historical inputs
        and outputs of a function ALA Callable
        """
        self.redis.rpush(inputs, str(args))
        data = method(self, *args, **kwargs)
        self.redis.rpush(outputs, str(data))
        return data
    return wrapper


def replay(method: Callable) -> None:
    """
    Replays the history of a function
    Args:
        method: The function to be decorated
    Returns:
        None
    """
    name = method.__qualname__
    cache = redis.Redis()
    calls = cache.get(name).decode("utf-8")
    print("{} was called {} times:".format(name, calls))
    inputs = cache.lrange(name + ":inputs", 0, -1)
    outputs = cache.lrange(name + ":outputs", 0, -1)
    for i, o in zip(inputs, outputs):
        print("{}(*{}) -> {}".format(name, i.decode('utf-8'),
                                     o.decode('utf-8')))


class Cache:
    """Defines methods to handle redis cache"""

    def __init__(self) -> None:
        """
        Initialize redis client
        Attributes:
            self._redis(redis.Redis): redis client
        """
        self._redis = redis.Redis()
        self._redis.flushdb()

    def get(self, key: str, fn: Optional[Callable] = None) -> \
            Union[str, int, float, bytes]:
        """Basically this method gets a value by a key and if the data is in
        bytes and a function is provided to convert the data to utf-8 then it
        uses the function the function is optional
        """
        data = self.client.get(key)
        if data is None:
            return None
        if fn is not None:
            return fn(data)
        return data

    @call_history
    @count_calls
    def store(self, data: Union[str, int, float, bytes]) -> str:
        """
        Store data in redis cache
        Args:
            data(dict): data to store
        Returns:
            str: key
        """
        key = str(uuid.uuid4())
        self._redis.set(key, data)
        return key

    def get_str(self, key: str) -> str:
        """
        Get data as string from redis cache
        Args:
            key (str): key
        Returns:
            str: data
        """
        data = self.get(key, lambda x: x.decode('utf-8'))
        return data

    def get_int(self, key: str) -> int:
        """
        Get data as integer from redis cache
        Args:
            key (str): key
        Returns:
            int: data
        """
        data = self.get(key)
        return int(data)
