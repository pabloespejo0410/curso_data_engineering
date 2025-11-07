-- models/staging/sql_server_dbo/stg_sql_server_dbo__events.sql

WITH source_data AS (

    SELECT

        -- CLAVE PRIMARIA SUBROGADA

        MD5(CAST(EVENT_ID AS VARCHAR)) AS event_sk,
        CAST(EVENT_ID AS VARCHAR) AS event_id,

        -- CLAVES FORÁNEAS SUBROGADAS

        MD5(CAST(USER_ID AS VARCHAR)) AS user_sk,
        MD5(CAST(PRODUCT_ID AS VARCHAR)) AS product_sk, 
        CAST(SESSION_ID AS VARCHAR) AS session_id,
        MD5(CAST(ORDER_ID AS VARCHAR)) AS order_sk,
        
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


