from pyspark import SparkContext
from pyspark.streaming import StreamingContext

# ====== CẤU HÌNH ======
HOST = "producer"
PORT = 9999
BATCH_INTERVAL = 1  # giây
CHECKPOINT_DIR = "file:///tmp/spark_wordcount_checkpoint"
# ======================

def update_func(new_values, running_count):
    """
    Hàm cập nhật state cho mỗi từ.
    - new_values: danh sách các giá trị trong batch hiện tại (ví dụ: [1, 1, 1])
    - running_count: trạng thái trước đó (số lần xuất hiện tích lũy) hoặc None
    """
    return sum(new_values) + (running_count or 0)

def print_top5(rdd):
    """In top 5 từ có count cao nhất"""
    if not rdd.isEmpty():
        # Lấy top 5 (sắp giảm dần theo count)
        top5 = rdd.takeOrdered(5, key=lambda x: -x[1])
        print("\n===== TOP 5 TỪ XUẤT HIỆN NHIỀU NHẤT =====")
        for rank, (word, count) in enumerate(top5, start=1):
            print(f"{rank}. {word:<15} {count}")
        print("==========================================\n")


def create_context():
    """
    Tạo mới StreamingContext.
    Nếu checkpoint có dữ liệu cũ, lần chạy đầu không quan tâm phục hồi (sẽ overwrite).
    """
    sc = SparkContext(appName="WordCountStatefulCheckpoint")
    sc.setLogLevel("ERROR")
    ssc = StreamingContext(sc, BATCH_INTERVAL)
    ssc.checkpoint(CHECKPOINT_DIR)

    # Nhận dữ liệu từ socket
    lines = ssc.socketTextStream(HOST, PORT)
    words = lines.flatMap(lambda line: line.strip().split())
    pairs = words.map(lambda w: (w, 1))

    # Stateful count
    counts = pairs.updateStateByKey(update_func)

    # In kết quả
    counts.foreachRDD(print_top5)
    return ssc

if __name__ == "__main__":
    ssc = StreamingContext.getOrCreate(CHECKPOINT_DIR, create_context)
    ssc.sparkContext.setLogLevel("ERROR")

    ssc.start()
    ssc.awaitTermination()
