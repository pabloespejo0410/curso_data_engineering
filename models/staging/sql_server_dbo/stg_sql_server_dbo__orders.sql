WITH source_data AS (

    SELECT
        -- Clave Subrogada (SK) del Hecho
        MD5(CAST(ORDER_ID AS VARCHAR)) AS order_sk,
        
        -- Clave de Negocio (ID)
        CAST(ORDER_ID AS VARCHAR) AS order_id,
        
        -- CLAVES FORÁNEAS (FKs) EN STAGING
        -- Las mantenemos como IDs de Negocio de origen, permitiendo NULLs
        CAST(ADDRESS_ID AS VARCHAR) AS address_fk,
        CAST(PROMO_ID AS VARCHAR) AS promo_fk,
        CAST(USER_ID AS VARCHAR) AS user_fk,
        
        -- Métricas
        CAST(SHIPPING_COST AS FLOAT) AS shipping_cost,
        CAST(ORDER_COST AS FLOAT) AS order_cost,
        CAST(ORDER_TOTAL AS FLOAT) AS order_total,
        
        -- Atributos
        CAST(SHIPPING_SERVICE AS VARCHAR) AS shipping_service,
        CAST(TRACKING_ID AS VARCHAR) AS tracking_id,
        CAST(STATUS AS VARCHAR) AS order_status,
        
        -- Fechas y Tiempos
        CONVERT_TIMEZONE('UTC', CREATED_AT) AS created_at,
        CONVERT_TIMEZONE('UTC', ESTIMATED_DELIVERY_AT) AS estimated_delivery_at,
        CONVERT_TIMEZONE('UTC', DELIVERED_AT) AS delivered_at,
        
        -- CÁLCULO: Duración de la Entrega (en horas)
        DATEDIFF(MINUTE, CREATED_AT, DELIVERED_AT) AS delivery_minutes_duration,
        
        -- Metadatos
        CONVERT_TIMEZONE('UTC', _FIVETRAN_SYNCED) AS loaded_at

    FROM 
        {{ source('SQL_SERVER_DBO', 'ORDERS') }} 

    WHERE ORDER_ID IS NOT NULL 
)

SELECT * FROM source_data


