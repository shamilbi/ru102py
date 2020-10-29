from redisolar.dao.base import MeterReadingDaoBase
from redisolar.dao.redis.base import RedisDaoBase
from redisolar.dao.redis.capacity_report import CapacityReportDaoRedis
from redisolar.dao.redis.feed import FeedDaoRedis
#from redisolar.dao.redis.metric import MetricDaoRedis as Mdr
from redisolar.dao.redis.metric_timeseries import MetricDaoRedisTimeseries as Mdr
from redisolar.models import MeterReading

# Uncomment for Challenge #3
from redisolar.dao.redis.site_stats import SiteStatsDaoRedis



class MeterReadingDaoRedis(MeterReadingDaoBase, RedisDaoBase):
    """MeterReadingDaoRedis persists MeterReading models to Redis."""
    def add(self, meter_reading: MeterReading, **kwargs) -> None:
        Mdr(self.redis, self.key_schema).insert(meter_reading, **kwargs)
        CapacityReportDaoRedis(self.redis, self.key_schema).update(meter_reading, **kwargs)
        FeedDaoRedis(self.redis, self.key_schema).insert(meter_reading, **kwargs)

        # Uncomment for Challenge #3
        SiteStatsDaoRedis(self.redis, self.key_schema).update(meter_reading, **kwargs)
