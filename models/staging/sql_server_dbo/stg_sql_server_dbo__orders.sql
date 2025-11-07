{{
    config(
        materialized='incremental',
        unique_key='order_sk',
        incremental_strategy='merge' 
    )
}}

WITH orders AS (
    SELECT
        -- CLAVE PRIMARIA SUBROGADA
        MD5(CAST(ORDER_ID AS VARCHAR)) AS order_sk,
        CAST(ORDER_ID AS VARCHAR) AS order_id,
        
        -- CLAVES FORÁNEAS SUBROGADAS
        MD5(CAST(ADDRESS_ID AS VARCHAR)) AS address_sk,
        MD5(CAST(PROMO_ID AS VARCHAR)) AS promo_sk,
        MD5(CAST(USER_ID AS VARCHAR)) AS user_sk,
        
        -- MÉTRICAS
        CAST(SHIPPING_COST AS FLOAT) AS shipping_cost,
        CAST(ORDER_COST AS FLOAT) AS order_cost,
        CAST(ORDER_TOTAL AS FLOAT) AS order_total,
        
        -- ATRIBUTOS
        CAST(SHIPPING_SERVICE AS VARCHAR) AS shipping_service,
        CAST(TRACKING_ID AS VARCHAR) AS tracking_id,
        CAST(STATUS AS VARCHAR) AS order_status,
        
        -- FECHAS Y TIEMPOS
        CONVERT_TIMEZONE('UTC', CREATED_AT) AS created_at,
        CONVERT_TIMEZONE('UTC', ESTIMATED_DELIVERY_AT) AS estimated_delivery_at,
        CONVERT_TIMEZONE('UTC', DELIVERED_AT) AS delivered_at,
        
        -- MÉTRICAS: Duración de la Entrega (en horas)
        DATEDIFF(MINUTE, CREATED_AT, DELIVERED_AT) AS delivery_minutes_duration,
        
        -- METADATOS (CRÍTICO para el incremental)
        CONVERT_TIMEZONE('UTC', _FIVETRAN_SYNCED) AS loaded_at -- Usaremos este campo para el filtro
    FROM 
        {{ source('SQL_SERVER_DBO', 'ORDERS') }} 

    WHERE ORDER_ID IS NOT NULL 

    -- Solo se aplica después de la primera ejecución
    {% if is_incremental() %}

        -- Filtra la tabla origen para incluir solo filas
        -- donde la fecha de sincronización es posterior al máximo
        -- valor de `loaded_at` que ya existe en la tabla STG.
        AND CONVERT_TIMEZONE('UTC', _FIVETRAN_SYNCED) > (
            SELECT MAX(loaded_at) 
            FROM {{ this }}
        )
        
    {% endif %}
)

SELECT * FROM orders