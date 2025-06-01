from pyspark.sql import SparkSession

def get_product_category_pairs(df_products, df_categories, df_product_categories):

    df_result = (df_products
        .join(df_product_categories, on="id_product", how="left")
        .join(df_categories, on="id_category", how="left")
    ).select("product_name", "category_name")

    return df_result

def main():

    spark = SparkSession.builder.getOrCreate()

    df_products = spark.createDataFrame([
        (1, "Чай"),
        (2, "Банан"),
        (3, "Груша"),
        (4, "Сыр"),
        (5, "Шоколад")
    ], ["id_product", "product_name"])

    df_categories = spark.createDataFrame([
        (1, "Фрукты"),
        (2, "Молочные"),
        (3, "Перекус"),
        (4, "Мясное")
    ], ["id_category", "category_name"])

    df_product_categories = spark.createDataFrame([
        (2, 1),  # Банан -> Фрукты
        (2, 3),  # Банан -> Перекус
        (3, 1),  # Груша -> Фрукты
        (4, 2),  # Сыр -> Молочные
        (5, 3)  # Шоколад -> Перекус
    ], ["id_product", "id_category"])

    df_pairs = get_product_category_pairs(df_products, df_categories, df_product_categories)

    print("Все пары: 'Имя продукта'-'категория'")
    df_pairs.show(truncate=False)

    spark.stop()

if __name__ == '__main__':
    main()
