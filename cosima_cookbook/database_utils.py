# enforce unique ORM objects: https://github.com/sqlalchemy/sqlalchemy/wiki/UniqueObject


def _unique(session, cls, hashfunc, queryfunc, constructor, arg, kw):
    cache = getattr(session, "_unique_cache", None)
    if cache is None:
        session._unique_cache = cache = {}

    key = (cls, hashfunc(*arg, **kw))
    if key in cache:
        return cache[key]
    else:
        with session.no_autoflush:
            q = session.query(cls)
            q = queryfunc(q, *arg, **kw)
            obj = q.first()
            if not obj:
                obj = constructor(*arg, **kw)
                session.add(obj)
        cache[key] = obj
        return obj


class UniqueMixin(object):
    @classmethod
    def unique_hash(cls, *arg, **kw):
        return NotImplementedError()

    @classmethod
    def unique_filter(cls, query, *arg, **kw):
        return NotImplementedError()

    @classmethod
    def as_unique(cls, session, *arg, **kw):
        return _unique(session, cls, cls.unique_hash, cls.unique_filter, cls, arg, kw)
