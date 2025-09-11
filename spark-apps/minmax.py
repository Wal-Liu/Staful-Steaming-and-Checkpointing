from pyspark import SparkContext
from pyspark.streaming import StreamingContext

# ====== CẤU HÌNH ======
HOST = "host.docker.internal"
PORT = 9999
BATCH_INTERVAL = 1  # giây
CHECKPOINT_DIR = "file:///tmp/spark_linecount_checkpoint"
# ======================

def update_func(new_values, state):
    ...

def print_min_max_lines(time, rdd):
    ...

def create_context():
    """
    Tạo mới StreamingContext.
    Nếu checkpoint có dữ liệu cũ, lần chạy đầu không quan tâm phục hồi (sẽ overwrite).
    """
    sc = SparkContext(appName="MinMaxLineWordCount")
    sc.setLogLevel("ERROR")
    ssc = StreamingContext(sc, BATCH_INTERVAL)
    ssc.checkpoint(CHECKPOINT_DIR)

    # Nhận dữ liệu từ socket
    lines = ssc.socketTextStream(HOST, PORT)

    # Tạo cặp (key, (line, word_count)) với key cố định để xử lý stateful
    pairs = lines.map(lambda line: ("min_max", (line.strip(), len(line.strip().split()))))

    # Stateful update để theo dõi dòng có số từ min/max
    state = pairs.updateStateByKey(update_func)

    # In kết quả
    state.foreachRDD(lambda time, rdd: print_min_max_lines(time, rdd))
    return ssc

if __name__ == "__main__":
    ssc = StreamingContext.getOrCreate(CHECKPOINT_DIR, create_context)
    ssc.sparkContext.setLogLevel("ERROR")
    ssc.start()
    ssc.awaitTermination()