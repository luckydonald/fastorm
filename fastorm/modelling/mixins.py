from sqlalchemy import Column, func, DateTime


class TimestampMixin(object):
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())
# end class