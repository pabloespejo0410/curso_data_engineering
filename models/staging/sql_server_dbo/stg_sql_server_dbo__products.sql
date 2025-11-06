WITH source_data AS (

    SELECT

        CAST(PRODUCT_ID AS VARCHAR) AS product_id,
        
        -- Atributos del Producto
        CAST(NAME AS VARCHAR) AS product_name,
        CAST(PRICE AS FLOAT) AS unit_price_in_dollars,
        CAST(INVENTORY AS NUMERIC(38, 0)) AS inventory_quantity, 
        
        -- Metadatos de Fivetran
        CONVERT_TIMEZONE('UTC', _FIVETRAN_SYNCED) AS loaded_at

    FROM 
        {{ source('SQL_SERVER_DBO', 'PRODUCTS') }} 

    WHERE PRODUCT_ID IS NOT NULL 

)

SELECT * FROM source_data