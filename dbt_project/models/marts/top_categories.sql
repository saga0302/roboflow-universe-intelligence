-- Mart: top datasets by image count
WITH staged AS (
    SELECT * FROM {{ ref('stg_datasets') }}
)

SELECT
    dataset_name,
    workspace,
    dataset_type,
    image_count,
    class_count,
    dataset_size_bucket,
    classification_complexity
FROM staged
ORDER BY image_count DESC