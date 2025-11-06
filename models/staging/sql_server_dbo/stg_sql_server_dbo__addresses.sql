WITH source_data AS (

    SELECT
        -- Clave subrogada

        MD5(CAST(ADDRESS_ID AS VARCHAR)) AS address_sk,

        -- Clave de negocio

        CAST(ADDRESS_ID AS VARCHAR) AS address_id, 

        -- Atributos de la Dirección

        CAST(STATE AS VARCHAR) AS state,
        CAST(ZIPCODE AS VARCHAR) AS zipcode,
        CAST(COUNTRY AS VARCHAR) AS country,
        
        -- Metadatos de Fivetran
        
        CONVERT_TIMEZONE('UTC', _FIVETRAN_SYNCED) AS loaded_at,
        _FIVETRAN_DELETED AS is_deleted

    FROM 
        {{ source('SQL_SERVER_DBO', 'ADDRESSES') }}

    WHERE ADDRESS_ID IS NOT NULL 

)

SELECT * FROM source_data


