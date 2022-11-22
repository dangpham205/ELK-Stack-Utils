import logging
# from logstash_async.handler import AsynchronousLogstashHandler
from datetime import datetime
import logstash

# host - port là của log stash instance đang đc chạy

class Logging(object):
    def __init__(self, logger_name='hd_logger--read_documents',
                 log_stash_host='localhost',
                 log_stash_upd_port=5959
    ):
        self.logger_name = logger_name
        self.log_stash_host = log_stash_host
        self.log_stash_upd_port = log_stash_upd_port
        
    def get(self):
        # ghi vào file log dưới local (chưa có sẽ tạo)
        logging.basicConfig(
            filename="logfile",
            filemode="a",
            format="%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s",
            datefmt="%H:%M:%S",
            level=logging.INFO,
        )

        self.stderrLogger = logging.StreamHandler()
        logging.getLogger().addHandler(self.stderrLogger)
        self.logger = logging.getLogger(self.logger_name)
        self.logger.addHandler(logstash.LogstashHandler(self.log_stash_host,
                                                        self.log_stash_upd_port,
                                                        version=1))
        return self.logger
    
    
if __name__ == "__main__":
    instance = Logging()
    logger = instance.get()

    count = 0
    from time import sleep


    logger.error({
        "doc_id": 1,
        "user_id": 269,
        "status": "Đã đọc",
        "timestamp": datetime.now()
    })
    # sleep(3)