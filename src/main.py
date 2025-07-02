from pyspark.sql import SparkSession

# إنشاء SparkSession
spark = SparkSession.builder \
    .appName("CommonFriends") \
    .getOrCreate()

# الحصول على SparkContext
sc = spark.sparkContext

# مسار ملف البيانات (تأكد من وجود الملف في ../data/friends.txt)
file_path = "../data/friends.txt"
lines = sc.textFile(file_path)

# تحويل كل سطر إلى (user_id, name, [friends list])
def parse_line(line):
    parts = line.strip().split()
    if len(parts) == 3:
        user_id = parts[0]
        name = parts[1]
        friends = parts[2].split(",")
        return (user_id, name, friends)
    else:
        return None

parsed = lines.map(parse_line).filter(lambda x: x is not None)

# إنشاء قاموس من user_id إلى الاسم
user_names = parsed.map(lambda x: (x[0], x[1])).collectAsMap()

# إنشاء أزواج الأصدقاء: ((min_id, max_id), set(friends))
pairs = parsed.flatMap(lambda x: [
    ((min(x[0], friend), max(x[0], friend)), set(x[2]))
    for friend in x[2]
])

# حساب الأصدقاء المشتركين لكل زوج
common_friends = pairs.reduceByKey(lambda a, b: a.intersection(b))

# الزوج الذي نبحث عنه: Mohamed (1) et Sidi (2)
target_pair = ("1", "2")

# استخراج النتيجة لهذا الزوج فقط
result = common_friends.filter(lambda x: x[0] == target_pair).collect()

# عرض النتيجة بشكل واضح
if result:
    common_ids = list(result[0][1])
    common_names = [user_names.get(uid, f"ID {uid}") for uid in common_ids]
    print(f"{target_pair[0]}<{user_names.get(target_pair[0])}> {target_pair[1]}<{user_names.get(target_pair[1])}> => Amis communs: {common_names}")
else:
    print("Aucun ami commun trouvé entre Mohamed (1) et Sidi (2).")

