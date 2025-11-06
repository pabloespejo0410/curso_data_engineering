-- models/staging/sql_server_dbo/stg_sql_server_dbo__events.sql

WITH source_data AS (

    SELECT
        -- CLAVE SUBROGADA 

        MD5(COALESCE(EVENT_ID, '')) AS event_sk,
        
        -- CLAVES NATURALES PARA FK

        CAST(EVENT_ID AS VARCHAR) AS event_id,
        CAST(USER_ID AS VARCHAR) AS user_id,
        CAST(PRODUCT_ID AS VARCHAR) AS product_id, 
        CAST(SESSION_ID AS VARCHAR) AS session_id,
        CAST(ORDER_ID AS VARCHAR) AS order_id,
        
        -- ATRIBUTOS DE EVENTO

        CAST(PAGE_URL AS VARCHAR) AS page_url,
        CAST(EVENT_TYPE AS VARCHAR) AS event_type,
        
        -- FECHAS

        CONVERT_TIMEZONE('UTC', CREATED_AT) AS created_at_utc,
        
        -- METADATOS
        
        CONVERT_TIMEZONE('UTC', _FIVETRAN_SYNCED) AS loaded_at_utc

    FROM 
        {{ source('SQL_SERVER_DBO', 'EVENTS') }}
)

SELECT * FROM source_data


