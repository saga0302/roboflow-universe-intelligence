-- Mart: dataset size and complexity analytics
WITH staged AS (
    SELECT * FROM {{ ref('stg_datasets') }}
)

SELECT
    dataset_type,
    dataset_size_bucket,
    classification_complexity,
    COUNT(*)                        AS total_datasets,
    AVG(image_count)                AS avg_images,
    MAX(image_count)                AS max_images,
    SUM(image_count)                AS total_images,
    AVG(class_count)                AS avg_classes,
    SUM(annotation_count)           AS total_annotations
FROM staged
GROUP BY 1, 2, 3
ORDER BY total_datasets DESC