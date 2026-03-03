-- Staging model: cleaning and standardizing raw Roboflow data
WITH source AS (
    SELECT * FROM raw.universe_datasets
),

cleaned AS (
    SELECT
        project_id,
        LOWER(TRIM(name))           AS dataset_name,
        workspace,
        LOWER(TRIM(type))           AS dataset_type,
        image_count,
        class_count,
        annotation_count,
        extracted_at::TIMESTAMP     AS extracted_at,

        -- Derived fields
        CASE
            WHEN image_count < 500   THEN 'small'
            WHEN image_count < 5000  THEN 'medium'
            ELSE 'large'
        END AS dataset_size_bucket,

        CASE
            WHEN class_count = 1  THEN 'binary'
            WHEN class_count <= 5 THEN 'few-class'
            ELSE 'multi-class'
        END AS classification_complexity

    FROM source
    WHERE project_id IS NOT NULL
      AND name IS NOT NULL
)

SELECT * FROM cleaned