from sqlalchemy import Column, func, DateTime, FetchedValue


class TimestampMixin(object):
    created_at = Column(DateTime, default=func.now(), server_default=FetchedValue())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now(), server_default=FetchedValue(), server_onupdate=FetchedValue())
# end class