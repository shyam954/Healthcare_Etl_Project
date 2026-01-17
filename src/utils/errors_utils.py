def exception_handler(logger):
    def decorator(fn):
        def wrapper(*args, **kwargs):
            try:
                return fn(*args, **kwargs)
            except Exception as e:
                logger.error(f"Exception in {fn.__name__}: {str(e)}")
                raise
        return wrapper
    return decorator