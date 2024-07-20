from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

spark = SparkSession.builder \
    .appName("Product-Category Relationships") \
    .getOrCreate()

# Пример данных для продуктов и их категорий
products_data = [
    (1, "Product A"),
    (2, "Product B"),
    (3, "Product C"),
    (4, "Product D")
]

categories_data = [
    (1, "Category 1"),
    (1, "Category 2"),
    (2, "Category 3")
]

products_df = spark.createDataFrame(products_data, ["product_id", "product_name"])
categories_df = spark.createDataFrame(categories_data, ["product_id", "category_name"])

try:
    merged_df = products_df.join(categories_df, on="product_id", how="left")

    """
    Не очень понял из формулировки задания нужно ли нам возвращать в одном датафрейме и продукты с категориями и продукты без, 
    но в случае чего это быстро фиксится
    """

    # Получение пар "Имя продукта - Имя категории"
    product_category_pairs = merged_df.filter(merged_df.category_name.isNotNull())\
                                      .select("product_name", "category_name")
    
    merged_df.show()

    print("Пары 'Имя продукта - Имя категории':")
    product_category_pairs.show()

    # Получение имен продуктов без категорий
    products_without_categories = products_df.join(categories_df, on="product_id", how="left_anti").select("product_name")

    print("Продукты без категорий:")
    products_without_categories.show()

except Exception as e:
    print(f"Произошла ошибка: {e}")

finally:
    spark.stop()