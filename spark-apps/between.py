from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import time  # Để lấy timestamp

# ====== CẤU HÌNH ======
HOST = "host.docker.internal"
PORT = 9999
BATCH_INTERVAL = 1  # giây
CHECKPOINT_DIR = "file:///tmp/spark_avg_interval_checkpoint"
# ======================

def update_func(new_values, state):
    ...

def print_avg_interval(time, rdd):
    ...

def create_context():
    """
    Tạo mới StreamingContext.
    """
    sc = SparkContext(appName="AverageIntervalBetweenLines")
    sc.setLogLevel("ERROR")
    ssc = StreamingContext(sc, BATCH_INTERVAL)
    ssc.checkpoint(CHECKPOINT_DIR)

    # Nhận dữ liệu từ socket
    lines = ssc.socketTextStream(HOST, PORT)

    # Thêm timestamp cho mỗi dòng
    timestamped_lines = lines.map(lambda line: (line.strip(), time.time()))

    # Tạo cặp với key cố định để xử lý stateful
    pairs = timestamped_lines.map(lambda ts_line: ("avg_interval", ts_line))

    # Stateful update để theo dõi thời gian
    state = pairs.updateStateByKey(update_func)

    # In kết quả
    state.foreachRDD(lambda time, rdd: print_avg_interval(time, rdd))
    return ssc

if __name__ == "__main__":
    ssc = StreamingContext.getOrCreate(CHECKPOINT_DIR, create_context)
    ssc.sparkContext.setLogLevel("ERROR")
    ssc.start()
    ssc.awaitTermination()