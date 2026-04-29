import time


class ExecutionMetrics:
    def __init__(self):
        self.start_time = time.time()
        self.env_load_time = 0
        self.s3_time = 0
        self.trino_time = 0

    def set_env_load_time(self, value):
        self.env_load_time = value

    def add_s3_time(self, value):
        self.s3_time += value

    def add_trino_time(self, value):
        self.trino_time += value

    def summary(self):
        total = time.time() - self.start_time

        return {
            "total_time": round(total, 3),
            "env_load_time": round(self.env_load_time, 3),
            "s3_time": round(self.s3_time, 3),
            "trino_time": round(self.trino_time, 3),
        }

    def log_summary(self, logger):
        data = self.summary()

        logger.info("===== EXECUTION METRICS =====")
        for k, v in data.items():
            logger.info(f"{k}: {v}s")