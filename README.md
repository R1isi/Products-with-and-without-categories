# Products-with-and-without-categories

## Задание

Требуется написать метод на PySpark, который в одном датафрейме вернет все пары "Имя продукта - Имя категории" и имена всех продуктов, у которых нет категорий.

---

## Решение

Создадим два датафрейма: один с именами продукта, второй с категориями:

	spark = SparkSession.builder \
    .appName("Product-Category Relationships") \
    .getOrCreate()
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

Создадим связь между продуктом и категорией по id продукта:

	merged_df = products_df.join(categories_df, on="product_id", how="left")

Получение пар "Имя продукта - Имя категории":

	product_category_pairs = merged_df.filter(merged_df.category_name.isNotNull())\
                                      .select("product_name", "category_name")

Получение имен продуктов без категорий

	products_without_categories = products_df.join(categories_df, on="product_id", how="left_anti").select("product_name")

---

## Пример работы кода

При запуске код выводит следующее:

	+----------+------------+-------------+
	|product_id|product_name|category_name|
	+----------+------------+-------------+
	|         1|   Product A|   Category 2|
	|         1|   Product A|   Category 1|
	|         2|   Product B|   Category 3|
	|         3|   Product C|         NULL|
	|         4|   Product D|         NULL|
	+----------+------------+-------------+

	Пары 'Имя продукта - Имя категории':
	+------------+-------------+
	|product_name|category_name|
	+------------+-------------+
	|   Product A|   Category 1|
	|   Product A|   Category 2|
	|   Product B|   Category 3|
	+------------+-------------+

	Продукты без категорий:
	+------------+
	|product_name|
	+------------+
	|   Product C|
	|   Product D|
	+------------+

---

## Разработчик

Мой профиль: [R1isi](https://github.com/R1isi)
