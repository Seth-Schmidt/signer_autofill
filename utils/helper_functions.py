from functools import wraps


def aiorwlock_aqcuire_release(fn):
    """
    A decorator that wraps a function and handles cleanup of any child processes
    spawned by the function in case of an exception.

    Args:
        fn (function): The function to be wrapped.

    Returns:
        function: The wrapped function.
    """
    @wraps(fn)
    async def wrapper(self, *args, **kwargs):
        self._logger.info(
            'Using signer {} for submission task. Acquiring lock', self._source_address,
        )
        await self._rwlock.writer_lock.acquire()
        self._logger.info(
            'Using signer {} for submission task. Acquired lock', self._source_address,
        )
        # self._logger.debug('Wrapping fn: {}', fn.__name__)
        try:
            # including the retry calls
            await fn(self, *args, **kwargs)

        except Exception as e:
            self._logger.opt(exception=True).error(
                'Error in using signer {} for submission task: {}', self._source_address, e,
            )
            # nothing to do here
            pass
        finally:
            try:
                self._rwlock.writer_lock.release()
            except Exception as e:
                self._logger.trace(
                    'Error releasing rwlock: {}. But moving on regardless... | Context: '
                    'Using signer {} for submission task: {}.', e, self._source_address, kwargs,
                )
    return wrapper