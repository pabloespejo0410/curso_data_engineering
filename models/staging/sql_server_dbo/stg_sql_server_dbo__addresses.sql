-- models/staging/sql_server_dbo/stg_sql_server_dbo__addresses.sql

WITH source_data AS (

    SELECT
        -- CLAVE PRIMARIA SUBROGADA

        MD5(CAST(ADDRESS_ID AS VARCHAR)) AS address_sk,

        -- CLAVE FORÁNEA SUBROGADA

        CAST(ADDRESS_ID AS VARCHAR) AS address_id, 

        -- ATRIBUTOS DIRECCIÓN

        CAST(STATE AS VARCHAR) AS state,
        CAST(ZIPCODE AS VARCHAR) AS zipcode,
        CAST(COUNTRY AS VARCHAR) AS country,
        
        -- METADATOS FIVETRAN
        
        CONVERT_TIMEZONE('UTC', _FIVETRAN_SYNCED) AS loaded_at,

    FROM 
        {{ source('SQL_SERVER_DBO', 'ADDRESSES') }}

)

SELECT * FROM source_data


