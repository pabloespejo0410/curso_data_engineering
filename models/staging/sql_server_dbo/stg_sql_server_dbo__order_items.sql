{{
    config(
        materialized='incremental',
        -- Clave única combinada: Orden + Producto
        unique_key='order_item_sk', 
        incremental_strategy='merge' 
    )
}}

SELECT
    -- Clave subrogada que identifica cada línea de producto única
    MD5(CAST(ORDER_ID AS VARCHAR) || CAST(PRODUCT_ID AS VARCHAR)) AS order_item_sk, 
    
    MD5(CAST(ORDER_ID AS VARCHAR)) AS order_sk,      
    CAST(ORDER_ID AS VARCHAR) AS order_id,           
    MD5(CAST(PRODUCT_ID AS VARCHAR)) AS product_sk,
    
    CAST(QUANTITY AS INTEGER) AS quantity,
    
    CONVERT_TIMEZONE('UTC', _FIVETRAN_SYNCED) AS loaded_at -- Clave Incremental
FROM 
    {{ source('SQL_SERVER_DBO', 'ORDERS_ITEMS') }} 

WHERE 
    ORDER_ID IS NOT NULL 

{% if is_incremental() %}
    -- Solo procesa items nuevos o modificados desde la última carga
    AND CONVERT_TIMEZONE('UTC', _FIVETRAN_SYNCED) > (
        SELECT MAX(loaded_at) 
        FROM {{ this }}
    )
{% endif %}